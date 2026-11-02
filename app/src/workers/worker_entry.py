"""
Entry nội bộ
"""
import os  # Nhận ID được supervisor truyền cho child.
from pathlib import Path  # Đường dẫn khóa vòng đời.
import portalocker  # OS locks được giải phóng khi process chết.
import uvicorn  # Chạy HTTP server bằng API Python.
from config.config import load_config  # Đọc chung file cấu hình.
from service import create_app  # Tạo worker hoặc gateway app.

if __name__ == '__main__':
    # Cấu hình được đọc trong process mới.
    cfg = load_config()
    # Cờ này cho phép các worker biết rằng chúng đang chạy trong môi trường được quản lý
    cfg['_managed_cluster'] = True

    # Lấy ID worker từ biến môi trường, nếu không có ID thì đây là gateway.
    worker_id = os.environ.get('VISION_WORKER_ID')

    # Tìm spec worker trong cấu hình nếu có ID.
    if worker_id:
        spec = next(
            (worker for worker in cfg['workers']
            if worker['id'] == worker_id),
            None
        )
    else:
        spec = None

    # Nếu có ID nhưng không tìm thấy spec, dừng rõ ràng trước khi server khởi động.
    if worker_id and spec is None:
        raise ValueError('Unknown VISION_WORKER_ID')  # Dừng rõ ràng trước server.

    # Khởi tạo thư mục locks và giữ lock vòng đời để tránh recovery khi còn orphan.
    locks = Path(cfg['storage']['root']) / 'locks'
    locks.mkdir(parents=True, exist_ok=True)

    # Giữ lock để đảm bảo rằng child không bị recovery khi còn orphan.
    with portalocker.Lock(locks / 'children.alive.lock', mode='a', timeout=0, flags=portalocker.LOCK_SH | portalocker.LOCK_NB):
        endpoint = spec or cfg['server']  # Mỗi worker có host/port riêng.

        # Chạy server HTTP bằng Uvicorn, không tạo thêm worker ngoài supervisor.
        uvicorn.run(create_app(cfg, spec), host=endpoint['host'], port=endpoint['port'], workers=1, access_log=False, timeout_graceful_shutdown=cfg['queue']['shutdown_seconds'])
