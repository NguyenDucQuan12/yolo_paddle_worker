"""Khóa GPU bằng OS file lock: process chết thì lock được hệ điều hành trả lại."""
import gc  # Thu gom tham chiếu Python đã không còn dùng.
import shutil  # Đọc dung lượng trống ổ đĩa.
from contextlib import contextmanager  # Quản lý acquire/release theo phạm vi.
from pathlib import Path  # Tạo lock file trong storage chung.
import portalocker  # File lock hoạt động trên Linux và Windows.


def check_disk(cfg):
    """
    Kiểm tra dung lượng trống ổ đĩa, raise OSError nếu không đủ."""
    root = Path(cfg['storage']['root'])  # Chỉ kiểm tra ổ chứa dữ liệu thực tế.
    free = shutil.disk_usage(root).free  # Số byte còn trống.
    if free < cfg['storage']['min_free_disk_mb'] * 1024 * 1024:  # Giữ vùng dự phòng cấu hình.
        raise OSError('Ổ dữ liệu không đủ vùng trống dự phòng')  # API/worker ghi log và dừng tác vụ.
    return free  # Dùng làm telemetry khi cần.


@contextmanager
def gpu_slot(cfg, worker, stop):
    """
    Cấp quyền truy cập GPU cho worker, dựa trên OS file lock.
    Nếu worker device=cpu thì không cần lock, cho phép chạy đồng thời.
    Nếu worker device=gpu thì tìm một trong các slot GPU còn rảnh, chờ nếu tất cả bận, hoặc dừng ngay nếu stop event được set.
    """
    if worker['device'] == 'cpu':
        yield  # Cho phép CPU worker làm việc đồng thời.
        return

    # Lấy đường dẫn thư mục locks trong storage root, tạo nếu chưa tồn tại.
    root = Path(cfg['storage']['root']) / 'locks'
    root.mkdir(parents=True, exist_ok=True)

    # Lấy số lượng slot GPU được cấu hình cho gpu_id của worker, mặc định là 1 nếu không có trong gpu_slots.
    count = cfg['gpu_slots'].get(str(worker['gpu_id']), 1)
    # Đánh dấu rằng chưa có slot nào được acquire, sẽ tìm kiếm trong vòng lặp.
    acquired = None
    while not stop.is_set() and acquired is None:
        # Chạy vòng lặp từ 0 đến count-1 để thử acquire từng slot GPU.
        for index in range(count):
            # Tạo file lock cho slot GPU hiện tại, tên file dựa trên gpu_id và index, mode='a' để append, timeout=0 để không chờ.
            candidate = portalocker.Lock(root / f'gpu-{worker["gpu_id"]}-{index}.lock', mode='a', timeout=0)
            try:
                # Thử acquire không chờ.
                candidate.acquire()
                # Nếu thành công, đánh dấu acquired là candidate và thoát vòng lặp for.
                acquired = candidate
                break
            except portalocker.exceptions.LockException:  # Slot đã có process khác giữ.
                continue  # Thử slot tiếp theo.

        # Nếu không có slot nào được acquire, dừng thread hiện tại trong thời gian poll_seconds, để tránh busy wait.
        if acquired is None:
            stop.wait(cfg['queue']['poll_seconds'])

    # Nếu sau vòng lặp vẫn không acquire được slot GPU và stop event được set, raise InterruptedError để caller biết worker đang dừng.
    if acquired is None:
        raise InterruptedError('Worker stopping')
    
    try:
        # Nếu acquire thành công, yield control cho caller để thực hiện inference.
        yield 
    finally:
        # Khi caller xong việc, release lock để slot GPU được giải phóng cho worker khác.
        acquired.release()


def release_cache(torch, paddle, enabled):
    """
    Giải phóng cache GPU và thu gom Python objects sau khi inference xong.
    Nếu enabled=False thì không giải phóng cache, chỉ thu gom Python objects.
    """
    # Thu gom Python objects đã không còn tham chiếu, giảm memory footprint của worker.
    gc.collect()
    # Nếu enabled=True và có GPU, giải phóng cache GPU của Torch và Paddle.
    if enabled and torch.cuda.is_available():
        # Giải phóng cache GPU của Torch, tránh OOM khi inference nhiều model.
        torch.cuda.empty_cache()
        # Giải phóng cache GPU của Paddle, tránh OOM khi inference nhiều model.
        if paddle.is_compiled_with_cuda():
            paddle.device.cuda.empty_cache()
