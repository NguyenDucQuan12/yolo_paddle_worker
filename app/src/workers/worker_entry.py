"""
TIẾN TRÌNH CON (child process) – được Supervisor trong main.py khởi chạy bằng subprocess.

Mỗi tiến trình con đóng 1 trong 2 vai trò, quyết định bởi biến môi trường VISION_WORKER_ID
do Supervisor đặt khi spawn:
    - Không có VISION_WORKER_ID   → GATEWAY: chỉ phục vụ API (upload, tra cứu job, admin), KHÔNG nạp model
                                    và KHÔNG import torch/paddle (khởi động nhanh, ít RAM).
    - VISION_WORKER_ID=<id>       → WORKER : phục vụ API + chạy Worker để xử lý job bằng model.
"""
import asyncio
import os
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path

import portalocker  # OS lock, tự giải phóng khi process chết.
import pyodbc
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from api import admin_api, health_check, job_api
from config.config import load_config
from log.system_log import _rotation_thread, system_logger
from middleware import logger as request_logger
from middleware.upload_guard import UploadGuardMiddleware
from services.database_service import DatabaseUnavailable, MyDatabase
from starlette.middleware.base import BaseHTTPMiddleware
from utils.db_concurrency import DatabaseCapacityTimeout


def _service_unavailable(detail):
    return JSONResponse({"detail": detail}, status_code=503, headers={"Retry-After": "5"})


def create_app(config=None, worker_info=None, database: MyDatabase = None, worker_factory=None):
    """
    Tạo FastAPI app. Gateway và worker dùng CHUNG bộ router (health, jobs),
    worker có thêm thread Worker chạy nền; gateway có thêm router /admin.

    :param config: cấu hình chung; None thì load từ config.yaml.
    :param worker_info: spec của worker trong config['workers']; None nghĩa là gateway.
    :param database: MyDatabase dùng chung cho cả app; None thì tạo từ config.
    :param worker_factory: (test) callable(config, worker_info, database) → Worker giả.
    """
    config = config or load_config()
    database = database or MyDatabase.from_config(config)
    role = worker_info['id'] if worker_info else 'gateway'

    @asynccontextmanager
    async def lifespan(app):
        if worker_info:
            system_logger.info("Khởi tạo worker với ID: %s", role)
            if worker_factory:
                app.state.worker = worker_factory(config, worker_info, database)
            else:
                # Import trễ: chỉ process worker mới kéo torch/paddle/ultralytics vào RAM.
                from workers.worker import Worker  # pylint: disable=import-outside-toplevel
                app.state.worker = Worker(config, worker_info, database=database)
            app.state.worker.start()
        try:
            yield
        finally:
            if worker_info:
                await asyncio.to_thread(app.state.worker.close)

    docs = bool(config['server'].get('enable_docs', True))
    app = FastAPI(
        title=f'Vision queue: {role}',
        version='2.1.0',
        lifespan=lifespan,
        docs_url='/docs' if docs else None,
        redoc_url='/redoc' if docs else None,
        openapi_url='/openapi.json' if docs else None,
    )

    app.state.config = config
    app.state.worker_info = worker_info
    app.state.database = database

    # Thứ tự: middleware thêm SAU nằm NGOÀI. Logger ngoài cùng để log cả request bị UploadGuard từ chối.
    app.add_middleware(UploadGuardMiddleware, config=config)
    app.add_middleware(BaseHTTPMiddleware, dispatch=request_logger.log_requests)

    app.include_router(health_check.router)
    app.include_router(job_api.router)
    if not worker_info:
        app.include_router(admin_api.router)

    @app.exception_handler(DatabaseUnavailable)
    @app.exception_handler(DatabaseCapacityTimeout)
    async def database_unavailable(request: Request, exc: Exception):
        system_logger.warning("DB không khả dụng khi xử lý %s %s: %s",
                              request.method, request.url.path, type(exc).__name__)
        return _service_unavailable("Cơ sở dữ liệu tạm thời không khả dụng, vui lòng thử lại sau.")

    # Lỗi SQL khác: không lộ chi tiết ODBC ra client.
    @app.exception_handler(pyodbc.Error)
    async def database_error(request: Request, exc: pyodbc.Error):
        system_logger.error("Lỗi cơ sở dữ liệu khi xử lý %s %s: %s", request.method, request.url.path, exc)
        return _service_unavailable("Cơ sở dữ liệu tạm thời không khả dụng, vui lòng thử lại sau.")

    # Lỗi chưa lường trước: ghi đầy đủ stack trace vào log, client chỉ nhận thông báo chung.
    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception):
        system_logger.exception("Lỗi không mong muốn khi xử lý request %s %s : %s",
                                request.method, request.url.path, str(exc))
        return JSONResponse({"detail": "Server gặp lỗi nội bộ, vui lòng thử lại sau."}, status_code=500)

    return app


def resolve_worker_info(cfg):
    """
    Đọc VISION_WORKER_ID (Supervisor đặt khi spawn) để biết process này là gateway hay worker.
    :raises ValueError: có ID nhưng không có trong config → dừng ngay, không khởi động server.
    """
    worker_id = os.environ.get('VISION_WORKER_ID')
    if not worker_id:
        return None
    spec = next((worker for worker in cfg['workers'] if worker['id'] == worker_id), None)
    if spec is None:
        raise ValueError(f'Unknown VISION_WORKER_ID: {worker_id}')
    return spec


def run():
    """Khởi động HTTP server cho gateway hoặc worker."""
    threading.Thread(target=_rotation_thread, daemon=True).start()

    cfg = load_config()
    # Báo cho Worker biết đang chạy dưới Supervisor: phải chờ file cluster.ready mới được claim job.
    cfg['_managed_cluster'] = True
    worker_info = resolve_worker_info(cfg)

    locks = Path(cfg['storage']['root']) / 'locks'
    locks.mkdir(parents=True, exist_ok=True)

    # Lock CHIA SẺ suốt vòng đời process con. Supervisor xin lock ĐỘC QUYỀN trên cùng file trước khi
    # recover(); nếu còn process con mồ côi đang giữ lock thì Supervisor không recover nhầm job đang chạy.
    try:
        lock = portalocker.Lock(locks / 'children.alive.lock', mode='a', timeout=0,
                                flags=portalocker.LOCK_SH | portalocker.LOCK_NB)
        lock.acquire()
    except portalocker.exceptions.LockException:
        system_logger.critical("Không lấy được lock chia sẻ children.alive.lock; dừng process con")
        sys.exit(2)

    try:
        endpoint = worker_info or cfg['server']
        uvicorn.run(create_app(cfg, worker_info),
                    host=endpoint['host'],
                    port=endpoint['port'],
                    workers=1,
                    access_log=False,
                    timeout_graceful_shutdown=cfg['queue']['shutdown_seconds'])
    finally:
        lock.release()


if __name__ == '__main__':
    run()
