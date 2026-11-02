"""
Gateway và địa chỉ worker dùng chung API
"""
import asyncio
import hmac
import os
import shutil
import tempfile
import time
import signal
import logging
import uuid
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal
import subprocess
import sys
import httpx
import portalocker
import psutil
import pyodbc
from fastapi import FastAPI, File, UploadFile, Request, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from config.config import load_config, ROOT
from log.logger import delete_old_logs, log_file_path
from utils.resources import check_disk
from utils.processes import stop_process
from workers.worker import Worker
from services.database_service import MyDatabase, JobConflict, QueueFull, EnqueueUncertain

logger = logging.getLogger()
# Dòng dưới sẽ ngăn chặn việc có những log không mong muốn từ thư viện PILLOW
# ví dụ: 2020-12-16 15:21:30,829 - DEBUG - PngImagePlugin - STREAM b'PLTE' 41 768
logging.getLogger("PIL.PngImagePlugin").propagate = False
# Cấu hình file log:
logging.basicConfig(filename=log_file_path, filemode= 'a',
                    format='%(asctime)s %(levelname)s:\t %(filename)s - Line: %(lineno)d message: %(message)s',
                    datefmt='%d/%m/%Y %I:%M:%S %p', encoding = 'utf-8', force=True)
# Cấu hình mức độ ghi log
# logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)
# Gọi hàm kiểm tra thư mục log tồn tại bao lâu trước khi tạo thư mục mới
delete_old_logs()

class Guard:
    """
    Middleware ASGI kiểm soát truy cập HTTP.
    """
    def __init__(self, app, cfg, role):
        """
        :param app: FastAPI app.
        :param cfg: dict cấu hình.
        :param role: 'gateway' hoặc worker id.
        """
        # Active request count được dùng để giới hạn số lượng upload đồng thời.
        self.app, self.cfg, self.role, self.active = app, cfg, role, 0

    async def __call__(self, scope, receive, send):
        """
        Điểm vào của middleware theo chuẩn ASGI
        """
        # Nếu không phải HTTP, chuyển tiếp cho app mà không kiểm tra.
        # Ví dụ WebSocket hoặc lifespan không cần kiểm tra API key hay upload concurrency.
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)

        # Tạo request_id duy nhất cho mỗi request
        request_id = uuid.uuid4().hex
        # Lưu mã request_id để các endpoint có thể truy cập và log
        scope.setdefault('state', {})['request_id'] = request_id

        # Ghi lại thời gian bắt đầu và status_code mặc định là 500
        start, status = time.perf_counter(), 500
        # Đánh dấu chưa bắt đầu phần response, nếu đã bắt đầu thì không thể gửi reject nữa.
        response_started = False
        # Biến xác nhận request đã được tính vào self.active, và spool là nơi chứa dữ liệu tạm
        admitted, spool = False, None

        async def traced_send(message):
            """
            Tạo hàm để thêm thông tin theo dõi trước khi gửi message cho HTTP server  
            Ví dụ chèn x-request-id vào header response.
            """
            # Sử dụng nonlocal để thay đổi biến status và response_started trong phạm vi hàm cha.
            nonlocal status, response_started

            # Khi message type là http.response.start sẽ chứa status code và headers, ta chèn x-request-id vào headers để client có thể tra log.
            # Nếu message type là http.response.body thì không cần chèn header nữa, chỉ cần gửi message cho HTTP server.
            if message['type'] == 'http.response.start':
                response_started = True                                                                     # Đánh dấu response đã bắt đầu.
                status = message['status']                                                                  # Ghi trạng thái HTTP để log cuối request.
                message['headers'] = list(message['headers']) + [(b'x-request-id', request_id.encode())]    # Chèn x-request-id vào header response để client có thể tra log.

            # Sau khi chèn x-request-id, gửi message cho HTTP server.
            await send(message)

        async def reject(code, detail):
            """
            Hàm chung để từ chối request với status code và detail, trả về JSONResponse.
            """
            await JSONResponse({'detail': detail, 'request_id': request_id}, status_code=code)(scope, receive, traced_send)

        try:
            # Chuyển đổi headers từ list sang dict để tra cứu nhanh, key là bytes.
            headers = dict(scope['headers'])

            # Kiểm tra API key, nếu role là gateway thì dùng api_key, nếu role là worker thì dùng admin_key. Nếu không có API key trong cấu hình, từ chối request với 401.
            expected = self.cfg['server']['api_key' if self.role == 'gateway' else 'admin_key']

            # Nếu path không phải /health, /docs, /openapi.json thì kiểm tra API key. Vì các endpoint này là public và không cần xác thực, ví dụ /health để kiểm tra trạng thái của worker/gateway.
            # if scope['path'] not in {'/health', '/docs', '/openapi.json'}:
            #     # Nếu không có API key trong header, từ chối request hoặc nếu có nhưng không khớp với expected, từ chối request với 401. Sử dụng hmac.compare_digest để tránh timing attack.
            #     if not expected or not hmac.compare_digest(headers.get(b'x-api-key', b''), expected.encode()):
            #         return await reject(401, 'Invalid X-API-Key')

            # Xử lý các request POST
            if scope['method'] == 'POST':
                # Kiểm tra số POST đang xử lý đã đạt giới hạn trong cấu hình chưa
                if self.active >= self.cfg['server']['upload_concurrency']:
                    return await reject(503, 'Hiện tại có quá nhiều upload đồng thời, vui lòng thử lại sau.')

                # Kiểm tra dung lượng ổ đĩa trước khi nhận upload
                check_disk(self.cfg)

                # Tăng thêm active để đánh dấu có một upload đang diễn ra, sẽ giảm lại trong finally.
                self.active += 1
                admitted = True

                # Đổi giới hạn dung lượng upload từ MB sang byte để so sánh với tổng số byte upload.
                limit = self.cfg['server']['max_body_mb'] * 2**20

                # Tạo thư mục tạm uploads_tmp trong storage root để lưu trữ file upload tạm thời
                upload_dir = Path(self.cfg['storage']['root']) / 'uploads_tmp'
                upload_dir.mkdir(exist_ok=True)

                # Tạo nơi chứa body tạm thời với ngưỡng RAM 1 MiB.
                # Nếu body upload nhỏ hơn 1 MiB thì lưu trong RAM, nếu lớn hơn 1 MiB thì lưu vào ổ đĩa
                spool = tempfile.SpooledTemporaryFile(max_size=1024*1024, dir=upload_dir)

                # Biến đếm tổng số byte được upload.
                total = 0
                # Bắt đầu timeout upload, nếu upload quá thời gian cấu hình, từ chối request với 408.
                async with asyncio.timeout(self.cfg['server']['upload_timeout_seconds']):
                    while True:
                        # Đọc chunk từ receive() không block event loop
                        chunk = await receive()
                        # Nếu người dùng ngắt kết nối, thoát vòng lặp và không cần xử lý gì thêm.
                        if chunk['type'] == 'http.disconnect':
                            return

                        # Lấy body từ chunk, nếu không có body thì mặc định là b'' (bytes rỗng).
                        body = chunk.get('body', b'')

                        # Tính tổng số byte upload và so sánh với giới hạn
                        total += len(body)
                        if total > limit:
                            return await reject(413, 'Yêu cầu upload vượt quá dung lượng tối đa cho phép')

                        # Ghi chunk vào spool tạm bằng luồng phụ
                        await asyncio.to_thread(spool.write, body)

                        # Nếu chunk không còn more_body, nghĩa là đã đọc hết body, thoát vòng lặp.
                        if not chunk.get('more_body', False):
                            break

                # Sau khi đọc xong body, rewind spool về đầu để chuẩn bị cho parser đọc lại.
                spool.seek(0)

                async def replay():
                    """
                    Hàm replay() được gọi bởi parser của FastAPI để đọc lại body từ spool tạm khi ta gửi request cho app.  
                    Không cần nạp toàn bộ body vào RAM, chỉ đọc từng chunk từ spool.
                    """
                    # Đọc chunk từ spool tạm, mỗi lần đọc tối đa 65536 byte (64 KiB) cho đến khi hết body
                    data = await asyncio.to_thread(spool.read, 65536)
                    return {'type': 'http.request', 'body': data, 'more_body': spool.tell() < total}
                
                # Chuyển tiếp request cho app với replay() để parser đọc lại body từ spool tạm, không cần nạp toàn bộ body vào RAM.
                await self.app(scope, replay, traced_send)

            # Nếu không phải POST, chuyển tiếp request cho app mà không kiểm tra upload concurrency hay dung lượng upload.
            else:
                await self.app(scope, receive, traced_send)

        except TimeoutError:
            await reject(408, 'Thời gian upload vượt quá giới hạn')
        except OSError:
            logger.exception("Lỗi hệ thống khi xử lý upload")
            if response_started:
                raise
            await reject(507, 'Không đủ dung lượng lưu trữ để nhận upload')

        # Giải phóng cả upload thành công và lỗi.
        finally:
            if spool is not None:
                spool.close()

            # Nếu đã tăng active thì giảm lại, đảm bảo luôn đúng số lượng upload đồng thời.
            if admitted:
                self.active -= 1

            # Ghi log
            logger.info("Request %s %s %s %d trong %.3f giây", scope['method'], scope['path'], request_id, status, time.perf_counter() - start)

def create_app(cfg=None, spec=None, factory=None, database: MyDatabase = None):
    """
    Tạo FastAPI app cho gateway hoặc worker, với cấu hình tương ứng.
    :param cfg: cấu hình chung, nếu None thì load từ config.yaml.
    :param spec: cấu hình worker, nếu None thì là gateway.
    :param factory: callable tạo backend inference, chỉ dùng trong test.
    :return: FastAPI app.
    """
    # Nếu cfg chưa được truyền vào, load cấu hình từ config.yaml.
    cfg = cfg or load_config()

    # Tạo myDatabase nếu chưa được truyền vào, để quản lý hàng đợi ở SQL Server.
    database = database or MyDatabase()

    # Nếu có id thì đây là worker, nếu không có id thì đây là gateway.
    role = spec['id'] if spec else 'gateway'

    @asynccontextmanager
    async def lifespan(app):
        """
        Lifespan context manager để khởi tạo và dọn dẹp các tài nguyên khi app start và shutdown.
        """
        logger.info("Khởi tạo FastAPI app cho role %s", role)
        # Khởi động các model inference nếu là worker, và warmup model để giảm độ trễ đầu tiên.
        if spec:
            options = {'factory': factory} if factory else {}
            # Các worker được khởi tạo trong app.state để dùng chung trong vòng đời của app
            app.state.runner = Worker(cfg, spec, database=database, **options)
            # Khởi động runner trong thread
            app.state.runner.start()

        try:
            yield
        finally:
            if spec:
                # Dọn dẹp runner khi shutdown, gọi close() trong thread để không block event loop.
                await asyncio.to_thread(app.state.runner.close)

    # Tạo FastAPI app, lifespan
    app = FastAPI(title=f'Vision queue: {role}', lifespan=lifespan)
    # Thêm middleware Guard để kiểm soát truy cập HTTP, với cfg và role được truyền vào.
    app.add_middleware(Guard, cfg=cfg, role=role)

    # Xử lý lỗi cơ sở dữ liệu, ví dụ SQL Server tạm thời không khả dụng, trả về 503 và thông báo cho client thử lại sau.
    @app.exception_handler(pyodbc.Error)
    async def database_error(request: Request, exc):
        logger.error("Cơ sở dữ liệu tạm thời không khả dụng: %s", exc, exc_info=True)
        return JSONResponse({'detail': 'Cơ sở dữ liệu tạm thời không khả dụng: %s' % str(exc),
                                'request_id': request.state.request_id},
                            status_code=503, headers={'Retry-After': '5'})

    # Xử lý lỗi không mong muốn, ví dụ lỗi lập trình hoặc ngoại lệ chưa được xử lý, trả về 500 và thông báo cho client thử lại sau.
    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception):
        logger.exception(
            "Lỗi không mong muốn khi xử lý request %s %s : %s",
            request.method,
            request.url.path,
            str(exc),
        )

        return JSONResponse(
            {
                "detail": "Server gặp lỗi nội bộ, vui lòng thử lại sau.",
                "request_id": getattr(request.state, "request_id", None),
            },
            status_code=500,
        )

    @app.get('/health')
    def health(request: Request):
        """
        Endpoint kiểm tra trạng thái của gateway hoặc worker.
        """
        return request.app.state.runner.snapshot() if spec else {
            'role': 'gateway',
            'ready': True,
            'workers': [
                {'id': w['id'], 'port': w['port']}
                for w in cfg['workers']
            ]
        }

    @app.post('/jobs', status_code=202)
    async def submit(request: Request, file: UploadFile = File(...), task: Literal['detect','ocr','analyze'] = Query('analyze')):
        """
        Endpoint nhận file upload và task, tạo job mới trong queue.
        :param file: file upload từ client, được lưu tạm trong spool tạm.
        :param task: task inference, có thể là detect/ocr/analyze.
        :return: dict chứa job_id, status, target_worker, status_url.
        """
        # Tạo job_id duy nhất
        job_id = uuid.uuid4().hex
        # Tạo folder lưu trữ job trong storage root
        folder = Path(cfg['storage']['root']) / 'jobs' / job_id
        folder.mkdir(parents=True, exist_ok=True)

        # Biến xác nhận job đã được enqueue thành công, nếu chưa enqueue thì cleanup folder rác.
        accepted = False
        try:
            # Lưu file gốc được upload vào disk với tên original.bin
            source = folder / 'original.bin'
            # Tổng dung lượng file upload, nếu vượt quá max_file_mb thì từ chối request với 413.
            total = 0

            # Mở file để ghi, sử dụng 'wb' để ghi nhị phân
            with source.open('wb') as stream:
                # Đọc từng chunk từ file upload, tối đa 1 MiB mỗi chunk, để tránh nạp toàn bộ file vào RAM.
                while chunk := await file.read(1024*1024):
                    total += len(chunk)
                    # Kiểm tra nếu tổng dung lượng file upload vượt quá max_file_mb, nếu vượt quá thì từ chối request với 413.
                    if total > cfg['server']['max_file_mb'] * 2**20:
                        raise HTTPException(413, 'File gửi lên vượt quá dung lượng tối đa cho phép')

                    # Ghi chunk vào disk
                    await asyncio.to_thread(stream.write, chunk)

                stream.flush()  # Đẩy buffer Python xuống OS.
                # Thực hiện fsync để yêu cầu OS ghi dữ liệu xuống disk
                await asyncio.to_thread(os.fsync, stream.fileno())

            if total == 0:
                raise HTTPException(400, 'File gửi lên rỗng, vui lòng kiểm tra lại')

            # Lấy id worker từ spec nếu có, vì nếu có id có nghĩa là chỉ định job này cho worker đó, còn nếu không có id thì job sẽ được queue và worker nào rảnh sẽ claim job.
            target = spec['id'] if spec else None
            # Enqueue job vào queue, sử dụng asyncio.to_thread để không block event loop khi enqueue
            await asyncio.to_thread(
                database.enqueue,                       # Gọi hàm enqueue của database để thêm job vào queue.
                job_id,                                 # job_id duy nhất của job.
                task,                                   # task: detect/ocr/analyze.
                str(source.relative_to(cfg['storage']["root"])),    # Đường dẫn tương đối của file gốc upload so với storage root, để worker có thể đọc file này.
                (file.filename or 'document')[:255],    # Tên gốc của file upload, cắt tối đa 255 ký tự để lưu vào SQLite.
                target                                  # Nếu có target worker id thì enqueue job cho worker đó, nếu không có thì job sẽ được queue và worker nào rảnh sẽ claim job.
            )

            # Ghi nhận queue đã nhận công việc thành công.
            accepted = True
            # ghi log
            logger.info("Yêu cầu %s %s %s đã được enqueue thành công cho worker %s", request.method, request.url.path, job_id, target or 'any')

            return {'job_id': job_id, 'status': 'queued', 'target_worker': target, 'status_url': f'/jobs/{job_id}'}

        except EnqueueUncertain:
            # SQL có thể đã commit nhưng ACK bị mất: KHÔNG xóa original.bin.
            accepted = True
            logger.warning("Yêu cầu %s %s %s đã được enqueue nhưng chưa chắc chắn kết quả, vui lòng poll job_id trước khi gửi lại", request.method, request.url.path, job_id)
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
                await asyncio.to_thread(shutil.rmtree, folder, True)

    @app.get('/jobs/{job_id}')
    def status(job_id: str):
        """
        Endpoint kiểm tra trạng thái của job, trả về các thông tin public của job.
        """
        # Lấy job từ database dựa theo job_id
        job = database.get(job_id)
        logger.info("Trạng thái job %s: %s", job_id, job)

        if job is None:
            raise HTTPException(404, 'Không tìm thấy yêu cầu với công việc này')

        # Tạo dict public chỉ chứa các trường cần thiết để trả về cho client, tránh lộ thông tin nhạy cảm.
        public = {
            key: job[key]
            for key in [
                'id', 'status', 'target', 'owner', 'task',
                'original_name', 'created', 'started',
                'finished', 'attempt', 'error'
            ]
        }

        # Thêm artifacts và download_url_template để client có thể tải các artifact đã publish.
        public['artifacts'] = ['original.bin'] + job['artifacts']
        public['download_url_template'] = f'/jobs/{job_id}/files?name=ARTIFACT_NAME'

        return public

    @app.get('/jobs/{job_id}/files')
    def download(job_id: str, name: str):
        """
        Endpoint tải về artifact của job, chỉ cho phép tải các file đã publish.
        :param job_id: id của job cần tải artifact.
        :param name: tên artifact cần tải, chỉ cho phép 'original.bin' và các artifact đã publish.
        :return: FileResponse để stream file từ disk, không nạp toàn bộ vào RAM.
        """
        # Lấy job từ database dựa theo job_id
        job = database.get(job_id)
        if job is None or name not in ['original.bin'] + job['artifacts']:
            raise HTTPException(404, 'Không tìm thấy artifact với tên này')
        logger.info("Tải về artifact %s của job %s", name, job_id)

        # Chuẩn hóa đường dẫn đến artifact
        base = (cfg['storage']['root'] / 'jobs' / job_id).resolve()
        path = (base / name).resolve()

        # Kiểm tra nếu path không nằm trong base hoặc không phải là file, trả về 404 để bảo vệ chống path traversal và symlink.
        if not path.is_relative_to(base) or not path.is_file():
            raise HTTPException(404, 'Không tìm thấy thư mục hoặc file artifact này')

        # Trả về FileResponse để stream file từ disk
        return FileResponse(path, filename=path.name)

    # Trả về app FastAPI đã được cấu hình với các endpoint và middleware Guard để kiểm soát truy cập HTTP.
    return app

if __name__ == '__main__':
    def main():
        """
        Chạy supervisor để quản lý worker và gateway; không dùng multiprocessing để tránh deadlock.
        """
        # Lấy cấu hình từ config.yaml
        cfg = load_config()

        # Thiết lập thư mục lưu trữ và locks.
        root = Path(cfg['storage']['root'])
        locks = root / 'locks'
        locks.mkdir(parents=True, exist_ok=True)

        # Thiết lập signal handler để dừng supervisor và child process có kiểm soát.
        stop = False
        # Dictionary lưu thông tin các process con, key là identity (worker_id hoặc 'gateway'), value là dict chứa process, console log file, spec, thời gian bắt đầu, trạng thái ready và số lần restart.
        children = {}

        def signal_stop(_signum, _frame):
            """
            Hàm này được gọi khi supervisor nhận SIGINT (Ctrl+C) hoặc SIGTERM (service manager)
            Đặt cờ stop để vòng lặp chính dừng lại và không tạo thêm process mới. Các process con sẽ được dọn dẹp trong finally block của main().
            """
            # Cho phép các hàm lồng nhau cập nhật biến của hàm bao ngoài
            # sử dụng biến stop ở hàm main, không phải tạo mới biến stop
            nonlocal stop
            # Đặt cờ stop để vòng lặp chính dừng lại, không spawn thêm process mới.
            stop = True

        # Khi nhận SIGINT (Ctrl+C) hoặc SIGTERM (service manager), gọi signal_stop để đặt cờ stop.
        signal.signal(signal.SIGINT, signal_stop)
        signal.signal(signal.SIGTERM, signal_stop)

        # Khởi tạo myDatabase để quản lý hàng đợi ở SQL Server.
        database = MyDatabase()

        def spawn(spec, restarts=0):
            """
            Tạo process con (worker hoặc gateway) với các thông tin cấu hình
            """
            # Xác định identity của process con, nếu spec là None thì là gateway.
            identity = spec['id'] if spec else 'gateway'
            # Sao chép môi trường hiện tại để truyền cho process con, loại bỏ VISION_WORKER_ID nếu có.
            env = os.environ.copy()
            # Xóa ID worker cũ nếu supervisor vô tình được khởi động từ môi trường đã có biến này.
            env.pop('VISION_WORKER_ID', None)

            # Gateway không cần cấu hình GPU. Chỉ worker mới cần thiết lập GPU
            if spec:
                # Cập nhật biến môi trường để ánh xạ GPU cho worker, thiết lập các biến liên quan đến runtime và thư mục tạm.
                env.update(VISION_WORKER_ID=identity,
                        CUDA_DEVICE_ORDER='PCI_BUS_ID',
                        CUDA_VISIBLE_DEVICES='' if spec['device']=='cpu' else str(spec['gpu_id']))

            # Cập nhật các biến môi trường khác để điều chỉnh số luồng CPU, chiến lược cấp phát bộ nhớ, và thư mục tạm thời cho uploads.
            env.update(PYTHONUNBUFFERED='1',                                # Không buffer stdout/stderr để log ra console/file ngay lập tức
                    OMP_NUM_THREADS=str(cfg['models']['cpu_threads']),   # Số luồng OpenMP cho các model CPU, tránh tranh chấp tài nguyên giữa worker
                    MKL_NUM_THREADS=str(cfg['models']['cpu_threads']),   # Số luồng MKL cho các model CPU, tránh tranh chấp tài nguyên giữa worker
                    FLAGS_allocator_strategy='auto_growth',              # Yêu cầu runtime tăng dần bộ nhớ GPU thay vì cấp phát toàn bộ, tránh OOM khi nhiều worker chạy cùng GPU
                    FLAGS_fraction_of_gpu_memory_to_use='0.9',           # Giới hạn tối đa 90% bộ nhớ GPU cho mỗi worker, tránh tranh chấp tài nguyên giữa worker
                    TMPDIR=str(root/'uploads_tmp'),                      # Thư mục tạm thời cho uploads, tránh ghi trực tiếp vào thư mục gốc
                    TEMP=str(root/'uploads_tmp'),                        # Tương tự như TMPDIR
                    TMP=str(root/'uploads_tmp'))
            
            # Đảm bảo process con tìm được các package nội bộ như config, service và worker.
            python_path = os.pathsep.join(
                filter(None, [str(ROOT / 'src'), env.get('PYTHONPATH')])
            )
            env['PYTHONPATH'] = python_path

            # Tạo thư mục tạm thời cho uploads nếu chưa có, tránh lỗi khi worker ghi file tạm.
            (root/'uploads_tmp').mkdir(exist_ok=True)
            # Mở file console log riêng cho process con, ghi cả stdout và stderr vào file này.
            logs_dir = root / 'logs'
            logs_dir.mkdir(parents=True, exist_ok=True)

            console = (logs_dir / f'{identity}-console.log').open('ab', buffering=0)

            # Tạo process con bằng subprocess.Popen, chạy worker_entry.py với Python hiện tại
            process = subprocess.Popen(
                [sys.executable, str(ROOT/"src/workers/worker_entry.py")],
                cwd=ROOT,
                env=env,
                stdout=console,
                stderr=subprocess.STDOUT
            )
            # Lưu thông tin process con vào dictionary children, bao gồm process, console log file, spec, thời gian bắt đầu, trạng thái ready và số lần restart.
            children[identity] = dict(
                process=process,
                console=console,
                spec=spec,
                started=time.time(),
                ready=False,
                restarts=restarts
            )
            # Ghi log sự kiện process_started với thông tin worker_id, pid_child và endpoint (cổng hoặc cấu hình server)
            logger.info("Process con %s (pid %d) đã được spawn với endpoint %s", identity, process.pid, spec or cfg['server'])

        def health(item):
            """
            Kiểm tra sức khỏe của process con bằng cách gọi endpoint /health.
            :param item: dict thông tin process con từ children.
            :return: True nếu process con trả về status_code 200 và JSON {'ready': True}, False nếu không.
            """
            # Lấy endpoint từ spec của process con hoặc cấu hình server nếu là gateway.
            endpoint = item['spec'] or cfg['server']
            # Nếu host là '0.0.0.0', sử dụng '127.0.0.1' thay vì '0.0.0.0' để tránh vấn đề liên quan đến bind wildcard.
            address = '127.0.0.1' if endpoint['host']=='0.0.0.0' else endpoint['host']

            try:
                # Gọi HTTP GET đến endpoint /health của process con, với timeout 1 giây và không đi qua proxy môi trường.
                response = httpx.get(f'http://{address}:{endpoint["port"]}/health', timeout=1, trust_env=False)
                # Kiểm tra nếu status_code là 200 và JSON trả về có trường 'ready' là True, trả về True. Nếu không, trả về False.
                return response.status_code == 200 and response.json().get('ready', False)

            except (httpx.HTTPError, ValueError):
                return False

        # Chỉ một supervisor (supervisor là tiến trình quản lý tất cả các process con) quản lý vùng data và queue, tránh nhiều supervisor cùng chạy → tranh chấp queue và worker.
        with portalocker.Lock(locks/'supervisor.lock', mode='a', timeout=0):
            # Supervisor giữ lock cluster để tránh nhiều instance cùng chạy, đảm bảo chỉ một tiến trình quản lý queue và worker.
            with portalocker.Lock(locks/'children.alive.lock', mode='a', timeout=0):
                # Nếu có file cluster.ready, nghĩa là supervisor cũ chưa dọn xong, xóa file này để tránh worker mới claim job khi supervisor mới chưa recover xong.
                (root/'cluster.ready').unlink(missing_ok=True)

                # Khôi phục các job dang dở từ store, tránh mất job khi supervisor restart. Chỉ recover khi không còn child cũ đang chạy.
                recovered = database.recover()
                # Ghi log sự kiện startup_recovery với danh sách job đã recover, để dễ tra cứu khi supervisor restart.
                if recovered:
                    logger.info("Supervisor khởi động lại và recover các job dang dở: %s", recovered)

            try:
                # Supervisor khởi tạo gateway (api chung) trước, sau đó spawn từng worker để tránh tranh tải model/cache. Nếu người dùng Ctrl+C trong thời gian load, supervisor sẽ dừng spawn thêm worker.
                spawn(None)
                for spec in cfg['workers']:  # Khởi động worker lần lượt để tránh tranh tải model/cache.
                    # Nếu người dùng đã Ctrl+C trong thời gian load, không spawn thêm worker mới.
                    if stop:
                        break

                    # Khởi động worker (tương đương với 1 process) mới.
                    spawn(spec)
                    # Lấy thông tin item của worker vừa spawn để theo dõi.
                    item = children[spec['id']]

                    # Kiểm tra trạng thái của worker vừa tạo, nếu chưa sẵn sàng, supervisor sẽ chờ worker warmup (load model, init runtime) trước khi spawn worker tiếp theo.
                    while not stop and not health(item):
                        # Kiểm tra nếu process con đã chết hoặc quá thời gian startup timeout, raise RuntimeError để supervisor dừng và log lỗi.
                        if (item['process'].poll() is not None    # poll() trả về None nếu process còn sống, trả về exit code nếu process đã chết.
                            or time.time()-item['started'] > cfg['queue']['startup_timeout_seconds']
                        ):
                            raise RuntimeError(f'Startup failed: {spec["id"]}; xem console log')
                        # Nếu worker chưa sẵn sàng, sleep 0.5 giây trước khi kiểm tra lại, tránh busy-spin CPU.
                        time.sleep(0.5)

                    # Nếu worker đã sẵn sàng, đánh dấu item['ready'] = True để supervisor biết worker đã qua warmup.
                    item['ready'] = not stop  # Ghi nhận worker đã qua warmup.

                # Khi tất cả worker đã spawn xong, supervisor mở barrier cluster.ready để worker có thể claim job từ queue.
                if not stop:
                    # Tạo file cluster.ready để thông báo cho worker rằng supervisor đã khởi động xong và có thể claim job từ queue.
                    (root/'cluster.ready').touch()

                # Gửi event thông báo cluster đã bắt đầu.
                logger.info("Supervisor đã khởi động xong, cluster đã sẵn sàng, worker có thể claim job từ queue")

                # Supervisor giữ vòng lặp để theo dõi các process con, restart nếu cần, và dọn dẹp khi nhận signal stop.
                # Gọi là watchdog loop vì supervisor sẽ kiểm tra trạng thái của các process con, xác định lý do nếu cần restart, và thực hiện restart nếu cần.
                while not stop:
                    #  Kiểm tra từng process con trong children, xác định lý do nếu cần restart, và thực hiện restart nếu cần.
                    for identity, item in list(children.items()):
                        # Lấy process và spec của item để kiểm tra trạng thái.
                        proc, spec = item['process'], item['spec']
                        # Lý do restart process con, mặc định là None, nếu cần restart sẽ gán giá trị tương ứng.
                        reason = None

                        # Nếu process con đã chết (poll() != None), gán reason là exit code của process.
                        if proc.poll() is not None:
                            reason = f'exit_{proc.returncode}'
                        # Nếu spec là None (gateway), không cần watchdog, chỉ cần restart khi process chết.
                        elif spec:
                            # Lấy thông tin job đang chạy của worker từ store, nếu có. Nếu không có job, trả về None.
                            job = database.running(identity)

                            # Nếu job đang chạy quá thời gian timeout, gán reason là 'job_timeout'.
                            # SQL Server trả datetime2 dạng datetime naive nhưng giá trị được lưu là UTC.
                            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                            if job and now_utc - job['started'] > timedelta(seconds=cfg['queue']['job_timeout_seconds']):
                                reason = 'job_timeout'

                            # Kiểm tra giới hạn RAM của worker. Mặc định là 8192MB, có thể cấu hình trong config.yaml. Nếu worker vượt quá giới hạn này, gán reason là 'rss_limit'.
                            limit = cfg['models'].get('max_worker_rss_mb', 8192)  # Ngân sách RAM mỗi worker.

                            try:
                                # Kiểm tra RSS của process con bằng psutil. Nếu vượt quá giới hạn, gán reason là 'rss_limit'.
                                if limit and psutil.Process(proc.pid).memory_info().rss > limit*2**20:
                                    reason = 'rss_limit'

                            except (psutil.NoSuchProcess, psutil.AccessDenied):  # Process có thể chết hoặc môi trường chặn đọc bộ nhớ.
                                # Nếu không thể đọc RSS, bỏ qua kiểm tra này, không gán reason.
                                if not item.get('rss_warning'):
                                    logger.warning("Không thể đọc RSS của process con %s (pid %d), có thể process đã chết hoặc môi trường chặn đọc bộ nhớ", identity, proc.pid)
                                    item['rss_warning'] = True  # Tránh lặp log ở mọi lần poll.

                            # Nếu worker chưa sẵn sàng (item['ready'] == False)
                            if not item['ready']:
                                # Gọi health() để kiểm tra trạng thái của worker. Nếu worker đã sẵn sàng, đánh dấu item['ready'] = True.
                                item['ready'] = health(item)
                                # Nếu worker đã quá thời gian startup timeout, gán reason là 'startup_timeout' để supervisor dừng process trước khi retry khác.
                                if not item['ready'] and time.time()-item['started'] > cfg['queue']['startup_timeout_seconds']:
                                    reason = 'startup_timeout'  # Kết thúc process trước lần retry khác.

                        # Nếu reason không None, nghĩa là cần restart process con, thực hiện dọn dẹp và spawn lại.
                        if reason:
                            # Ghi log sự kiện process_stopping với worker_id, reason, và số lần restart.
                            logger.info("Process con %s (pid %d) đang dừng vì lý do: %s; số lần restart: %d", identity, proc.pid, reason, item['restarts'])
                            # Dừng process con theo thứ tự:
                            # 1. Gọi stop_process() để gửi signal terminate
                            # 2. Chờ process dừng với thời gian chờ cấu hình trong config.yaml
                            # 3. Nếu chưa dừng, gửi signal kill để ép dừng
                            # 4. wait() để xác nhận process đã dừng hoàn toàn trước khi spawn lại.
                            stop_process(proc, cfg['queue']['shutdown_seconds'])
                            # Đóng file console log của process con để tránh rò file descriptor khi supervisor restart nhiều lần.
                            item['console'].close()

                            if spec:
                                # Nếu là worker, recover job đang chạy để requeue cho lần spawn mới.
                                logger.info("Recovering jobs for worker %s: %s", identity, database.recover(identity))
                            # Nếu số lần restart đã vượt quá giới hạn max_restarts trong config.yaml, raise RuntimeError để supervisor dừng và log lỗi.
                            if item['restarts'] >= cfg['queue']['max_restarts']:
                                raise RuntimeError(f'Restart limit reached: {identity}')
                            
                            time.sleep(1)  # Backoff ngắn, tránh nạp model liên tục không nghỉ.
                            spawn(spec, item['restarts']+1)  # Worker mới giữ cùng địa chỉ, model load lại một lần.

                    # Supervisor sleep 0.5 giây trước khi vòng lặp tiếp theo, tránh busy-spin CPU.
                    time.sleep(0.5)
            finally:
                # Dọn dẹp khi supervisor nhận signal stop hoặc gặp lỗi, đảm bảo không còn process con nào đang chạy và file cluster.ready bị xóa.
                (root/'cluster.ready').unlink(missing_ok=True)
                # duyệt tất cả process con trong children, dừng chúng theo thứ tự và đóng file console log.
                for identity, item in children.items():
                    # Dừng process con theo thứ tự:
                    # 1. Gọi stop_process() để gửi signal terminate
                    # 2. Chờ process dừng với thời gian chờ cấu hình trong config.yaml
                    # 3. Nếu chưa dừng, gửi signal kill để ép dừng
                    # 4. wait() để xác nhận process đã dừng hoàn toàn trước khi supervisor exit.
                    logger.info("Dọn dẹp process con %s (pid %d) trước khi supervisor exit", identity, item['process'].pid)
                    stop_process(item['process'], cfg['queue']['shutdown_seconds'])
                    # Đóng file console log của process con để tránh rò file descriptor khi supervisor exit.
                    item['console'].close()

                    # Nếu là worker, recover job đang chạy để requeue cho lần spawn mới.
                    if item['spec']:
                        database.recover(identity)
                # Ghi log sự kiện cluster_stopped để thông báo supervisor đã dừng và tất cả process con đã được dọn dẹp.
                logger.info("Supervisor đã dừng, cluster đã dọn dẹp xong")

    main()