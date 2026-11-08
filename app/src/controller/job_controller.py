import asyncio
import json
import os
import re
import shutil
import uuid
from pathlib import Path

from fastapi import HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from log.system_log import system_logger
from services.database_service import (DatabaseUnavailable, EnqueueUncertain, JobConflict,
                                       MyDatabase, QueueFull)
from utils.resources import check_disk

# Tên cố định của file gốc trên disk, không dùng tên từ client để tránh path traversal.
ORIGINAL_NAME = 'original.bin'
# Chữ ký đầu file của các định dạng được hỗ trợ; kiểm tra sớm để từ chối ngay ở API (415)
# thay vì chiếm chỗ trong queue rồi mới failed.
_SIGNATURES = (b'%PDF-', b'\xff\xd8\xff', b'\x89PNG\r\n\x1a\n')
_RESULT_PATTERN = re.compile(r'attempt_(\d{3})/result\.json')

PUBLIC_FIELDS = ('id', 'status', 'target', 'owner', 'task', 'original_name',
                 'created', 'started', 'finished', 'attempt', 'error')


def _looks_supported(head: bytes) -> bool:
    if b'%PDF-' in head[:1024]:
        return True
    if head.startswith(_SIGNATURES[1:]):
        return True
    return head[:4] == b'RIFF' and head[8:12] == b'WEBP'


class JobController:
    """
    Controller xử lý các thao tác liên quan đến job: upload + enqueue, tra cứu trạng thái, tải artifact.
    """

    def __init__(self, config, worker_id: str, database: MyDatabase):
        self.storage_root = Path(config['storage']['root'])
        self.job_root = self.storage_root / 'jobs'
        self.job_root.mkdir(parents=True, exist_ok=True)
        self.config = config
        self.database = database
        self.worker_id = worker_id

    async def upload_job_file(self, request: Request, file: UploadFile, task: str):
        """
        Lưu file upload vào storage/jobs/<job_id>/original.bin rồi enqueue job.
        Mã lỗi: 400 rỗng, 413 quá lớn, 415 sai định dạng, 503 queue đầy/DB lỗi, 507 hết đĩa.
        """
        try:
            await asyncio.to_thread(check_disk, self.config)
        except OSError as exc:
            await file.close()
            system_logger.error("Từ chối upload vì ổ dữ liệu sắp đầy: %s", exc)
            raise HTTPException(507, 'Server tạm hết dung lượng lưu trữ, vui lòng thử lại sau') from exc

        job_id = uuid.uuid4().hex
        job_folder = self.job_root / job_id
        accepted = False
        file_path = job_folder / ORIGINAL_NAME
        try:
            job_folder.mkdir(parents=True, exist_ok=True)
            total = 0
            head = b''
            limit = self.config['server']['max_file_mb'] * 2**20
            with file_path.open('wb') as stream:
                while chunk := await file.read(1024 * 1024):
                    if not head:
                        head = chunk[:1024]
                    total += len(chunk)
                    if total > limit:
                        raise HTTPException(413, 'File gửi lên vượt quá dung lượng tối đa cho phép')
                    await asyncio.to_thread(stream.write, chunk)
                stream.flush()
                await asyncio.to_thread(os.fsync, stream.fileno())

            if total == 0:
                raise HTTPException(400, 'File gửi lên rỗng, vui lòng kiểm tra lại')
            if not _looks_supported(head):
                raise HTTPException(415, 'Chỉ nhận JPEG, PNG, WebP hoặc PDF')

            target = self.worker_id
            await asyncio.to_thread(
                self.database.enqueue,
                job_id,
                task,
                file_path.relative_to(self.storage_root).as_posix(),
                (file.filename or 'document')[:255],
                target,
            )

            accepted = True
            system_logger.info("Job %s (%s, %d bytes) đã enqueue cho worker %s",
                               job_id, task, total, target or 'any')
            return {'job_id': job_id, 'status': 'queued', 'target_worker': target,
                    'status_url': f'/jobs/{job_id}'}

        except EnqueueUncertain:
            # SQL có thể đã commit nhưng ACK bị mất: KHÔNG xóa original.bin.
            accepted = True
            system_logger.warning("Job %s: chưa chắc chắn đã enqueue; client cần poll job_id", job_id)
            return JSONResponse(
                {'job_id': job_id, 'status': 'acceptance_unknown',
                 'detail': 'Database outcome unknown; poll this job ID before resubmitting',
                 'status_url': f'/jobs/{job_id}'}, status_code=503,
                headers={'Retry-After': '5'})
        except JobConflict as exc:
            accepted = True  # Không xóa dữ liệu có thể thuộc job đã tồn tại.
            raise HTTPException(409, 'Job ID conflict') from exc
        except QueueFull as exc:
            raise HTTPException(503, 'Hàng đợi đã đầy, vui lòng thử lại sau',
                                headers={'Retry-After': '10'}) from exc
        except DatabaseUnavailable as exc:
            # Chắc chắn chưa ghi DB → xoá file, client gửi lại an toàn.
            raise HTTPException(503, 'Cơ sở dữ liệu tạm thời không khả dụng, vui lòng thử lại sau',
                                headers={'Retry-After': '5'}) from exc
        finally:
            await file.close()
            if not accepted:
                await asyncio.to_thread(shutil.rmtree, job_folder, True)

    @staticmethod
    def _artifacts(job) -> list[str]:
        """Cột artifacts trong SQL Server là chuỗi JSON, cần parse thành list."""
        raw = job.get('artifacts') or '[]'
        try:
            value = json.loads(raw) if isinstance(raw, str) else list(raw)
        except (TypeError, ValueError):
            return []
        return [item for item in value if isinstance(item, str)]

    def _load(self, job_id: str):
        job = self.database.get(job_id)
        if job is None:
            raise HTTPException(404, f'Không tìm thấy job với ID {job_id}')
        return job

    def get_job(self, job_id: str):
        """Trả thông tin public của job."""
        job = self._load(job_id)
        public = {key: job.get(key) for key in PUBLIC_FIELDS}
        public['artifacts'] = [ORIGINAL_NAME] + self._artifacts(job)
        public['download_url_template'] = f'/jobs/{job_id}/files?name=ARTIFACT_NAME'
        if job.get('status') == 'succeeded':
            public['result_url'] = f'/jobs/{job_id}/result'
        return public

    def _resolve(self, job_id: str, name: str) -> Path:
        base = (self.job_root / job_id).resolve()
        path = (base / name).resolve()
        if not path.is_relative_to(base) or not path.is_file():
            system_logger.warning("Artifact %s của job %s không tồn tại trên disk", name, job_id)
            raise HTTPException(404, 'Không tìm thấy file artifact này')
        return path

    def download_artifact(self, job_id: str, name: str):
        """Tải về artifact đã publish của job (hoặc original.bin)."""
        job = self._load(job_id)
        if name not in [ORIGINAL_NAME] + self._artifacts(job):
            raise HTTPException(404, 'Không tìm thấy artifact với tên này')
        path = self._resolve(job_id, name)
        return FileResponse(str(path), filename=path.name)

    def get_result(self, job_id: str):
        """Trả thẳng nội dung result.json của lần chạy thành công (tiện cho client, khỏi tải file)."""
        job = self._load(job_id)
        status = job.get('status')
        if status in ('queued', 'running'):
            return JSONResponse({'job_id': job_id, 'status': status, 'detail': 'Job chưa xử lý xong'},
                                status_code=202, headers={'Retry-After': '2'})
        if status != 'succeeded':
            raise HTTPException(409, f'Job kết thúc với trạng thái {status}: {job.get("error")}')

        results = sorted((name for name in self._artifacts(job) if _RESULT_PATTERN.fullmatch(name)),
                         reverse=True)
        if not results:
            raise HTTPException(404, 'Job không có result.json')
        path = self._resolve(job_id, results[0])
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc:
            system_logger.error("Không đọc được %s: %s", path, exc)
            raise HTTPException(500, 'Không đọc được kết quả của job') from exc
