"""
Cấu hình chung cho server và worker, nạp từ file YAML do người dùng chỉnh sửa.
"""
import os
from pathlib import Path
import yaml

# Lùi 2 cấp từ file này để lấy ROOT của dự án, ví dụ 
# D:/Project/yolo_paddleocr_server/app/src/config/config.py → D:/Project/yolo_paddleocr_server
ROOT = Path(__file__).resolve().parents[2]


def load_config():
    """
    Đọc file cấu hình YAML, kiểm tra tính hợp lệ, trả dict.
    """
    # Lấy đường dẫn file cấu hình từ biến môi trường, mặc định là config.yaml trong ROOT.
    path = Path(os.getenv('VISION_CONFIG', str(ROOT / 'config.yaml')))
    with path.open(encoding='utf-8') as stream:
        # safe_load() đọc YAML an toàn, không thực thi code Python trong file.
        cfg = yaml.safe_load(stream)

    # Lấy đường dẫn tuyệt đối của thư mục dữ liệu, đảm bảo thống nhất giữa worker và gateway.
    root = Path(cfg['storage']['root'])
    cfg['storage']['root'] = str((ROOT / root).resolve())

    # Chuyển đổi key GPU thành chuỗi để chấp nhận cả dạng 0 và '0' trong YAML.
    # gpu_slots={0:1, '1':2} → {'0':1, '1':2}; worker tra bằng chuỗi '0' sẽ tìm được.
    cfg['gpu_slots'] = {str(key): value for key, value in cfg['gpu_slots'].items()}

    # Lấy danh sách cổng và ID worker để kiểm tra trùng lặp.
    # Gateway 8000, worker_1 9001, worker_2 9002 → ports=[8000,9001,9002].
    ports = [cfg['server']['port']] + [w['port'] for w in cfg['workers']]
    # Lấy danh sách ID worker để kiểm tra trùng lặp.
    ids = [w['id'] for w in cfg['workers']]

    # len(set(ids)) sẽ loại bỏ trùng lặp, nếu khác len(ids) thì có trùng.
    if not ids or len(ids) != len(set(ids)) or len(ports) != len(set(ports)):
        raise ValueError('Cần ít nhất 1 worker; ID và port không được trùng')

    if any(not isinstance(p, int) or not 1024 <= p <= 65535 for p in ports):
        raise ValueError('Port phải là số nguyên 1024..65535')

    for worker in cfg['workers']:
        # Kiểm tra device và gpu_id của worker.
        if worker['device'] not in {'auto', 'cpu', 'gpu'}:
            raise ValueError('device chỉ nhận auto/cpu/gpu')

        if not isinstance(worker['gpu_id'], int) or worker['gpu_id'] < 0:
            raise ValueError('gpu_id phải là số nguyên >= 0')

        if not worker['id'].replace('_', '').isalnum():
            raise ValueError('Worker ID chỉ gồm chữ, số, dấu gạch dưới')

    # Kiểm tra các tham số phải lớn hơn 0.
    positive = [
        ('server', 'upload_concurrency'),
        ('server', 'upload_timeout_seconds'),
        ('server', 'max_body_mb'),
        ('server', 'max_file_mb'),
        ('queue', 'max_pending'),
        ('queue', 'max_attempts'),
        ('queue', 'poll_seconds'),
        ('queue', 'job_timeout_seconds'),
        ('queue', 'startup_timeout_seconds'),
        ('queue', 'shutdown_seconds'),
        ('storage', 'max_pixels'),
        ('storage', 'max_edge'),
        ('storage', 'max_pdf_pages'),
        ('storage', 'max_crops_per_page'),
        ('models', 'cpu_threads'),
        ('models', 'yolo_imgsz')
    ]
    if any(cfg[section][key] <= 0 for section, key in positive):
        raise ValueError('Các giới hạn dung lượng/thời gian/số lượng phải > 0')

    if any(not isinstance(value, int) or value < 1 for value in cfg['gpu_slots'].values()):
        raise ValueError('gpu_slots phải là số nguyên >= 1')

    # Trả dict để module khác dùng cùng cấu trúc.
    return cfg
