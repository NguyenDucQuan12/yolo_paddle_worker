import os
from pathlib import Path
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from services.database_service import MyDatabase

router = APIRouter(
    tags= ["Health"]
)


def get_database(request: Request) -> MyDatabase:
    """
    Lấy instance MyDatabase dùng chung đã gắn vào app.state trong create_app().
    """
    return request.app.state.database


@router.get('/health', summary="Trạng thái gateway/worker")
def health(request: Request):
    """
    Endpoint kiểm tra trạng thái của gateway hoặc worker (supervisor dùng để chờ ready).
    """
    state = request.app.state
    if state.worker_info:
        return state.worker.snapshot()
    return {
        'role': 'gateway',
        'ready': True,
        'workers': [
            {'id': w['id'], 'port': w['port']}
            for w in state.config['workers']
        ]
    }


@router.get("/healthz", summary="Liveness probe")
async def healthz():
    """
    Kiểm tra sống/chết cơ bản của tiến trình.
    """
    return {"status": "ok"}

@router.get("/readyz", summary="Readiness probe")
def readyz(request: Request, database: MyDatabase = Depends(get_database)):
    """
    Kiểm tra sẵn sàng: DB + quyền ghi thư mục upload.
    Trả 200 nếu ok, 503 nếu có bất kỳ lỗi nào.
    """
    checks = {}

    # Kiểm tra DB
    try:
        checks["db"] = "ok" if database._check_connection() else "error: SELECT 1 failed"
    except Exception as e:
        checks["db"] = f"error: {e.__class__.__name__}"

    # Kiểm tra quyền ghi thư mục upload
    try:
        upload_directory = Path(request.app.state.config['storage']['root']) / 'uploads_tmp'
        os.makedirs(upload_directory, exist_ok=True)
        probe_file = os.path.join(upload_directory, ".readyz.tmp")
        with open(probe_file, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(probe_file)
        checks["fs"] = "ok"
    except Exception as e:
        checks["fs"] = f"error: {e.__class__.__name__}"

    ok = all(val == "ok" for val in checks.values())
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"status": "ok" if ok else "error", "checks": checks},
    )