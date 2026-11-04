from fastapi import HTTPException, Request, UploadFile
import json
import shutil
import uuid
from pathlib import Path
from fastapi.responses import FileResponse, JSONResponse
import os
import asyncio
from services.database_service import MyDatabase, JobConflict, QueueFull, EnqueueUncertain
from log.system_log import system_logger

# Tên cố định của file gốc trên disk, không dùng tên từ client để tránh path traversal.
ORIGINAL_NAME = 'original.bin'


class JobController:
    """
    Controller xử lý các thao tác liên quan đến job, bao gồm lấy thông tin folder, file, xóa job, liệt kê file và trả về response cho file.
    """
    def __init__(self, config, worker_id: str, database: MyDatabase):
        """
        Khởi tạo controller với đường dẫn gốc lưu trữ các job.
        """
        # Thư mục lưu trữ các job
        self.storage_root = Path(config['storage']['root'])

        # Thư mục gốc chứa tất cả các job, đảm bảo tồn tại trước khi sử dụng
        self.job_root = self.storage_root / 'jobs'
        self.job_root.mkdir(parents=True, exist_ok=True)

        # Cấu hình chung
        self.config = config
        self.database = database
        self.worker_id = worker_id

    async def upload_job_file(self, request: Request, file: UploadFile, task: str):
        """
        Lưu file upload vào storage/jobs/<job_id>/original.bin rồi enqueue job.

        Args:
            request (Request): Request hiện tại, dùng để ghi log.
            file (UploadFile): File upload từ client.
            task (str): Loại task của job (detect/ocr/analyze).

        Raises:
            HTTPException: 400/409/413/503 tuỳ lỗi.
        """
        # job_id luôn do server sinh ra
        job_id = uuid.uuid4().hex

        # Tạo thư mục cho job nếu chưa tồn tại
        job_folder = self.job_root / job_id
        job_folder.mkdir(parents=True, exist_ok=True)

        # Biến xác nhận job đã được enqueue thành công, nếu chưa enqueue thì cleanup folder rác.
        accepted = False
        file_path = job_folder / ORIGINAL_NAME
        try:
            # Tổng dung lượng file upload, nếu vượt quá max_file_mb thì từ chối request với 413.
            total = 0
            # Mở file để ghi, sử dụng 'wb' để ghi nhị phân
            with file_path.open('wb') as stream:
                # Đọc từng chunk từ file upload, tối đa 1 MiB mỗi chunk, để tránh nạp toàn bộ file vào RAM.
                while chunk := await file.read(1024*1024):
                    total += len(chunk)
                    # Kiểm tra nếu tổng dung lượng file upload vượt quá max_file_mb, nếu vượt quá thì từ chối request với 413.
                    if total > self.config['server']['max_file_mb'] * 2**20:
                        raise HTTPException(413, 'File gửi lên vượt quá dung lượng tối đa cho phép')

                    # Ghi chunk vào disk
                    await asyncio.to_thread(stream.write, chunk)

                stream.flush()  # Đẩy buffer Python xuống OS.
                # Thực hiện fsync để yêu cầu OS ghi dữ liệu xuống disk
                await asyncio.to_thread(os.fsync, stream.fileno())

            if total == 0:
                raise HTTPException(400, 'File gửi lên rỗng, vui lòng kiểm tra lại')

            # Lấy id worker từ spec nếu có, vì nếu có id có nghĩa là chỉ định job này cho worker đó, còn nếu không có id thì job sẽ được queue và worker nào rảnh sẽ claim job.
            target = self.worker_id

            # Enqueue job vào queue, sử dụng asyncio.to_thread để không block event loop khi enqueue
            await asyncio.to_thread(
                self.database.enqueue,                       # Gọi hàm enqueue của database để thêm job vào queue.
                job_id,                                 # job_id duy nhất của job.
                task,                                   # task: detect/ocr/analyze.
                file_path.relative_to(self.storage_root).as_posix(),    # Đường dẫn tương đối so với storage root, ví dụ jobs/<id>/original.bin.
                (file.filename or 'document')[:255],    # Tên gốc của file upload, cắt tối đa 255 ký tự để lưu vào SQLite.
                target                                  # Nếu có target worker id thì enqueue job cho worker đó, nếu không có thì job sẽ được queue và worker nào rảnh sẽ claim job.
            )

            # Ghi nhận queue đã nhận công việc thành công.
            accepted = True
            # ghi log
            system_logger.info("Yêu cầu %s %s %s đã được enqueue thành công cho worker %s", request.method, request.url.path, job_id, target or 'any')

            return {'job_id': job_id, 'status': 'queued', 'target_worker': target, 'status_url': f'/jobs/{job_id}'}

        except EnqueueUncertain:
            # SQL có thể đã commit nhưng ACK bị mất: KHÔNG xóa original.bin.
            accepted = True
            system_logger.warning("Yêu cầu %s %s %s đã được enqueue nhưng chưa chắc chắn kết quả, vui lòng poll job_id trước khi gửi lại", request.method, request.url.path, job_id)
            return JSONResponse(
                {'job_id': job_id, 'status': 'acceptance_unknown',
                    'detail': 'Database outcome unknown; poll this job ID before resubmitting',
                    'status_url': f'/jobs/{job_id}'}, status_code=503,
                headers={'Retry-After': '5'}
            )
        except JobConflict as exc:
            accepted = True  # Không xóa dữ liệu có thể thuộc job đã tồn tại.
            raise HTTPException(409, 'Job ID conflict') from exc
        except QueueFull as exc:  # Capacity DB được kiểm tra nguyên tử.
            raise HTTPException(503, 'Queue full; retry later') from exc
        finally:
            # Đóng file upload để giải phóng tài nguyên
            await file.close()
            # Nếu job chưa được enqueue thành công, xóa folder rác để tránh chiếm dung lượng lưu trữ.
            if not accepted:
                await asyncio.to_thread(shutil.rmtree, job_folder, True)

    @staticmethod
    def _artifacts(job) -> list[str]:
        """
        Cột artifacts trong SQL Server là chuỗi JSON, cần parse thành list.
        """
        raw = job.get('artifacts') or '[]'
        return json.loads(raw) if isinstance(raw, str) else list(raw)

    def get_job(self, job_id: str):
        """
        Lấy thông tin job từ database dựa theo job_id.
        :param job_id: ID của job cần lấy.
        :return: dict chứa thông tin job nếu tồn tại, None nếu không tìm thấy.
        """
        # Lấy thông ti job từ DB
        job = self.database.get(job_id)
        system_logger.info("Lấy thông tin job %s: %s", job_id, job)

        if job is None:
            system_logger.warning("Không tìm thấy job với ID %s", job_id)
            raise HTTPException(404, f'Không tìm thấy job với ID {job_id}')

        # Chỉ trả ra các thông tin public cần thiết để client có thể hiển thị trạng thái job, tránh lộ thông tin nhạy cảm.
        public = {
            key: job[key]
            for key in [
                'id', 'status', 'target', 'owner', 'task',
                'original_name', 'created', 'started',
                'finished', 'attempt', 'error'
            ]
        }

        # Thêm artifacts và download_url_template để client có thể tải các artifact đã publish.
        public['artifacts'] = [ORIGINAL_NAME] + self._artifacts(job)
        public['download_url_template'] = f'/jobs/{job_id}/files?name=ARTIFACT_NAME'

        return public

    def download_artifact(self, job_id: str, name: str):
        """
        Tải về các tài liệu (artifact) của job, chỉ cho phép tải các file đã publish.
        :param job_id: ID của job cần tải artifact.
        :param name: Tên artifact cần tải, chỉ cho phép 'original.bin' và các artifact đã publish.
        :return: dict chứa đường dẫn file nếu tồn tại, HTTPException nếu không tìm thấy.
        """
        # Lấy thông tin job từ database dựa theo job_id
        job = self.database.get(job_id)

        if job is None or name not in [ORIGINAL_NAME] + self._artifacts(job):
            system_logger.warning("Không tìm thấy artifact %s của job %s", name, job_id)
            raise HTTPException(404, 'Không tìm thấy artifact với tên này')

        system_logger.info("Tải về artifact %s của job %s", name, job_id)
        base = (self.job_root / job_id).resolve()
        path = (base / name).resolve()

        if not path.is_relative_to(base) or not path.is_file():
            system_logger.warning("Artifact %s của job %s không tồn tại trên disk", name, job_id)
            raise HTTPException(404, 'Không tìm thấy thư mục hoặc file artifact này')

        return FileResponse(str(path), filename=path.name)