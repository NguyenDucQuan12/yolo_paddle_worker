"""
TIẾN TRÌNH CON (child process) – được Supervisor trong main.py khởi chạy bằng subprocess.

Mỗi tiến trình con đóng 1 trong 2 vai trò, quyết định bởi biến môi trường VISION_WORKER_ID
do Supervisor đặt khi spawn:
    - Không có VISION_WORKER_ID   → GATEWAY: chỉ phục vụ API (upload, tra cứu job), không nạp model.
    - VISION_WORKER_ID=<id>       → WORKER : phục vụ API + chạy Worker để xử lý job bằng model.
"""
import asyncio
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

import portalocker  # OS lock, tự giải phóng khi process chết.
import pyodbc
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from api import health_check, job_api
from config.config import load_config
from log.system_log import _rotation_thread, system_logger
from middleware import logger as request_logger
from services.database_service import MyDatabase
from workers.worker import Worker


# ---------------------------------------------------------------------------
# Tạo FastAPI app cho gateway hoặc worker
# ---------------------------------------------------------------------------
def create_app(config=None, worker_info=None, database: MyDatabase = None):
    """
    Tạo FastAPI app. Gateway và worker dùng CHUNG bộ router (health, jobs),
    chỉ khác ở chỗ worker có thêm thread Worker chạy nền.

    :param config: cấu hình chung; None thì load từ config.yaml (dùng khi test).
    :param worker_info: spec của worker trong config['workers']; None nghĩa là gateway.
    :param database: MyDatabase dùng chung cho cả app; None thì tạo mới.
    :return: FastAPI app.
    """
    # Không load lại khi đã được truyền vào, vì run() đã gắn cờ _managed_cluster vào config.
    config = config or load_config()
    # Một instance MyDatabase cho toàn bộ process: router và Worker dùng chung.
    database = database or MyDatabase()
    # Tên hiển thị: id của worker, hoặc 'gateway'.
    role = worker_info['id'] if worker_info else 'gateway'

    @asynccontextmanager
    async def lifespan(app):
        """
        Chạy 1 lần khi server start (trước yield) và 1 lần khi server shutdown (sau yield).
        Chỉ worker mới tạo Worker; gateway không nạp model.
        """
        if worker_info:
            system_logger.info("Khởi tạo worker với ID: %s", role)
            # Lưu vào app.state để /health đọc được snapshot của worker.
            app.state.worker = Worker(config, worker_info, database=database)
            # start() chạy vòng lặp claim job trong thread riêng, không chặn server HTTP.
            app.state.worker.start()

        try:
            yield
        finally:
            if worker_info:
                # close() có join() nên chạy trong thread để không chặn event loop khi shutdown.
                await asyncio.to_thread(app.state.worker.close)

    app = FastAPI(
        title=f'Vision queue: {role}',
        lifespan=lifespan
    )

    # Tài nguyên dùng chung: router đọc qua request.app.state.<tên>.
    app.state.config = config
    app.state.worker_info = worker_info
    app.state.database = database

    # Middleware ghi log mọi request/response.
    app.add_middleware(BaseHTTPMiddleware, dispatch=request_logger.log_requests)

    # Đăng ký router; thêm router mới tại đây.
    app.include_router(health_check.router)
    app.include_router(job_api.router)

    # @app.get('/favicon.ico', include_in_schema=False)
    # async def favicon():
    #     file_name = "favicon.ico"
    #     file_path = os.path.join("assets", "images", "static", file_name)
    #     return FileResponse(path=file_path, headers={"Content-Disposition": "attachment; filename=" + file_name})

    # SQL Server tạm thời không khả dụng: trả 503 để client thử lại, không lộ chi tiết lỗi ODBC.
    @app.exception_handler(pyodbc.Error)
    async def database_error(request: Request, exc: pyodbc.Error):
        system_logger.error("Lỗi cơ sở dữ liệu khi xử lý %s %s: %s", request.method, request.url.path, exc)
        return JSONResponse({"detail": "Cơ sở dữ liệu tạm thời không khả dụng, vui lòng thử lại sau."},
                            status_code=503, headers={"Retry-After": "5"})

    # Lỗi chưa lường trước: ghi đầy đủ stack trace vào log, client chỉ nhận thông báo chung.
    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception):
        system_logger.exception("Lỗi không mong muốn khi xử lý request %s %s : %s",
                                request.method, request.url.path, str(exc))
        return JSONResponse({"detail": "Server gặp lỗi nội bộ, vui lòng thử lại sau."}, status_code=500)

    return app


# ---------------------------------------------------------------------------
# 2. XÁC ĐỊNH VAI TRÒ của tiến trình con này
# ---------------------------------------------------------------------------
def resolve_worker_info(cfg):
    """
    Đọc VISION_WORKER_ID (Supervisor đặt khi spawn) để biết process này là gateway hay worker.

    :return: spec của worker trong cfg['workers'], hoặc None nếu là gateway.
    :raises ValueError: có ID nhưng không có trong config → dừng ngay, không khởi động server.
    """
    worker_id = os.environ.get('VISION_WORKER_ID')
    if not worker_id:
        return None

    spec = next((worker for worker in cfg['workers'] if worker['id'] == worker_id), None)
    if spec is None:
        raise ValueError(f'Unknown VISION_WORKER_ID: {worker_id}')
    return spec


# ---------------------------------------------------------------------------
# 3. ĐIỂM VÀO của tiến trình con
# ---------------------------------------------------------------------------
def run():
    """
    Khởi động HTTP server cho gateway hoặc worker.
    """
    # Mỗi process có logger riêng nên mỗi process cần thread xoay log riêng.
    threading.Thread(target=_rotation_thread, daemon=True).start()

    cfg = load_config()
    # Báo cho Worker biết đang chạy dưới Supervisor: phải chờ file cluster.ready mới được claim job.
    cfg['_managed_cluster'] = True

    worker_info = resolve_worker_info(cfg)

    locks = Path(cfg['storage']['root']) / 'locks'
    locks.mkdir(parents=True, exist_ok=True)

    # Lock CHIA SẺ (LOCK_SH) suốt vòng đời process con. Supervisor xin lock ĐỘC QUYỀN trên cùng file
    # trước khi recover(); nếu còn process con mồ côi đang giữ lock thì Supervisor không recover nhầm job đang chạy.
    with portalocker.Lock(locks / 'children.alive.lock', mode='a', timeout=0,
                          flags=portalocker.LOCK_SH | portalocker.LOCK_NB):
        # Worker dùng host/port riêng trong spec; gateway dùng cfg['server'].
        endpoint = worker_info or cfg['server']
        # workers=1: mỗi process con chỉ 1 uvicorn worker; việc nhân bản do Supervisor quản lý.
        uvicorn.run(create_app(cfg, worker_info),
                    host=endpoint['host'],
                    port=endpoint['port'],
                    workers=1,
                    access_log=False,
                    timeout_graceful_shutdown=cfg['queue']['shutdown_seconds'])


if __name__ == '__main__':
    run()