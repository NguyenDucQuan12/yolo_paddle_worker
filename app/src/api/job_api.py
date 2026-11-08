from typing import Literal

from fastapi import APIRouter, Depends, Query, Request, UploadFile

from api.security import require_client_key
from controller.job_controller import JobController

router = APIRouter(
    prefix="/jobs",
    tags=["Jobs"],
    dependencies=[Depends(require_client_key)],
)


def get_controller(request: Request) -> JobController:
    """Tạo JobController với database, config và worker_id lấy từ app.state."""
    worker_info = request.app.state.worker_info
    return JobController(database=request.app.state.database,
                         config=request.app.state.config,
                         worker_id=worker_info['id'] if worker_info else None)


@router.post("", status_code=202, summary="Tạo job mới")
async def create_job(request: Request, file: UploadFile,
                     task: Literal['detect', 'ocr', 'analyze'] = Query('analyze'),
                     controller: JobController = Depends(get_controller)):
    return await controller.upload_job_file(request, file, task)


# def (không async) vì truy vấn DB là đồng bộ; FastAPI chạy trong threadpool, không chặn event loop.
@router.get("/{job_id}", summary="Kiểm tra trạng thái job")
def get_job_status(job_id: str, controller: JobController = Depends(get_controller)):
    return controller.get_job(job_id)


@router.get("/{job_id}/result", summary="Lấy kết quả JSON của job đã xong")
def get_job_result(job_id: str, controller: JobController = Depends(get_controller)):
    return controller.get_result(job_id)


@router.get("/{job_id}/files", summary="Tải về artifact của job")
def download_job_file(job_id: str, name: str, controller: JobController = Depends(get_controller)):
    return controller.download_artifact(job_id, name)
