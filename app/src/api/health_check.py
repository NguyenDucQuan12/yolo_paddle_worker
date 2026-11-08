"""
Endpoint giám sát:
    /health   – supervisor dùng để biết process đã ready (worker: đã nạp model).
    /healthz  – liveness probe (process còn phản hồi HTTP).
    /readyz   – readiness probe (DB + quyền ghi storage); load balancer dùng để quyết định gửi traffic.
"""
import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from services.database_service import MyDatabase

router = APIRouter(tags=["Health"])


def get_database(request: Request) -> MyDatabase:
    """Lấy instance MyDatabase dùng chung đã gắn vào app.state trong create_app()."""
    return request.app.state.database


@router.get('/health', summary="Trạng thái gateway/worker")
def health(request: Request):
    """Supervisor dùng để chờ ready. Worker trả snapshot (state, job đang chạy, RAM...)."""
    state = request.app.state
    if state.worker_info:
        worker = getattr(state, 'worker', None)
        if worker is None:
            return {'worker_id': state.worker_info['id'], 'ready': False, 'state': 'starting'}
        return worker.snapshot()
    return {
        'role': 'gateway',
        'ready': True,
        'workers': [{'id': w['id'], 'port': w['port']} for w in state.config['workers']],
    }


@router.get("/healthz", summary="Liveness probe")
async def healthz():
    """Kiểm tra sống/chết cơ bản của tiến trình."""
    return {"status": "ok"}


@router.get("/readyz", summary="Readiness probe")
def readyz(request: Request, database: MyDatabase = Depends(get_database)):
    """
    Kiểm tra sẵn sàng: DB + quyền ghi thư mục upload (+ worker đã nạp model nếu là worker).
    Trả 200 nếu ok, 503 nếu có bất kỳ lỗi nào.
    """
    checks = {"db": "ok" if database._check_connection() else "error: database unavailable"}

    try:
        upload_directory = Path(request.app.state.config['storage']['root']) / 'uploads_tmp'
        os.makedirs(upload_directory, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=upload_directory,
                                         prefix=".readyz.", suffix=".tmp") as handle:
            handle.write("ok")
        checks["fs"] = "ok"
    except Exception as exc:  # pylint: disable=broad-except
        checks["fs"] = f"error: {exc.__class__.__name__}"

    if request.app.state.worker_info:
        worker = getattr(request.app.state, 'worker', None)
        checks["model"] = "ok" if worker is not None and worker.state.get('ready') else "error: not ready"

    ok = all(val == "ok" for val in checks.values())
    return JSONResponse(status_code=200 if ok else 503,
                        content={"status": "ok" if ok else "error", "checks": checks})
