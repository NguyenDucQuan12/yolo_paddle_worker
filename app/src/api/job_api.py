from typing import Literal
from fastapi import APIRouter, Depends, Request, UploadFile, Query
from controller.job_controller import JobController


router = APIRouter(
    prefix = "/jobs",
    tags= ["Jobs"]
)


def get_controller(request: Request) -> JobController:
    """
    Tạo một instance của JobController với database, config và worker_id lấy từ app.state.
    """
    # Lấy các thành phần cần thiết từ app.state để khởi tạo JobController
    database = request.app.state.database
    config = request.app.state.config
    worker_info = request.app.state.worker_info

    # Lấy worker_id từ worker_info nếu có, nếu không có worker_info thì worker_id là None.
    worker_id = worker_info['id'] if worker_info else None
    return JobController(database=database, config=config, worker_id=worker_id)

@router.post("", status_code=202, summary="Tạo job mới")
async def create_job(request: Request, file: UploadFile, task: Literal['detect','ocr','analyze'] = Query('analyze'), controller: JobController = Depends(get_controller)):
    # Trả dict hoặc JSONResponse (trường hợp 503 acceptance_unknown) trực tiếp, FastAPI tự serialize.
    return await controller.upload_job_file(request, file, task)

# Dùng def (không async) vì truy vấn DB là đồng bộ; FastAPI sẽ chạy trong threadpool, không chặn event loop.
@router.get("/{job_id}", summary="Kiểm tra trạng thái job")
def get_job_status(job_id: str, controller: JobController = Depends(get_controller)):
    return controller.get_job(job_id)

@router.get("/{job_id}/files", summary="Tải về artifact của job")
def download_job_file(job_id: str, name: str, controller: JobController = Depends(get_controller)):
    return controller.download_artifact(job_id, name)