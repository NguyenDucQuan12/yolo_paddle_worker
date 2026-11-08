"""
Cấu hình chung cho server và worker, nạp từ file YAML do người dùng chỉnh sửa.

Thứ tự ưu tiên (cao → thấp):
    1. Biến môi trường (VISION_API_KEY, VISION_DB_PASSWORD, ...) – dùng cho secret.
    2. File app/.env (nạp bằng python-dotenv, không commit lên git).
    3. File YAML (mặc định app/config.yaml, đổi bằng VISION_CONFIG).
    4. Giá trị mặc định trong DEFAULTS bên dưới – file YAML cũ thiếu khóa mới vẫn chạy được.
"""
import copy
import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Lùi 2 cấp từ file này để lấy ROOT của ứng dụng, ví dụ
# D:/Project/yolo_paddleocr_server/app/src/config/config.py → D:/Project/yolo_paddleocr_server/app
ROOT = Path(__file__).resolve().parents[2]

# Nạp app/.env (nếu có) TRƯỚC khi đọc biến môi trường. override=False: biến môi trường thật luôn thắng.
load_dotenv(ROOT / '.env', override=False)

logger = logging.getLogger(__name__)

# Các giá trị bảo mật mặc định trong file mẫu; dùng để cảnh báo khi quên đổi.
INSECURE_KEYS = {'', 'change-this-key', 'change-admin-key', '123456789'}

DEFAULTS = {
    'server': {
        'host': '127.0.0.1',
        'port': 8000,
        'api_key': None,
        'admin_key': None,
        'upload_concurrency': 4,
        'upload_timeout_seconds': 60,
        'max_body_mb': 26,
        'max_file_mb': 25,
        # Có bật /docs, /redoc, /openapi.json không. Nên tắt khi public ra Internet.
        'enable_docs': True,
    },
    'queue': {
        'max_pending': 200,
        'poll_seconds': 0.25,
        # Khi queue rỗng, worker giãn dần chu kỳ poll tới mức này để giảm tải SQL Server.
        'idle_poll_max_seconds': 2.0,
        'max_attempts': 2,
        'job_timeout_seconds': 300,
        'startup_timeout_seconds': 900,
        'shutdown_seconds': 15,
        # Số lần restart tối đa trong cửa sổ restart_window_seconds trước khi tạm ngắt (circuit breaker).
        'max_restarts': 5,
    },
    'supervisor': {
        # Backoff giữa các lần restart: initial * 2^n, tối đa max.
        'restart_backoff_initial_seconds': 2,
        'restart_backoff_max_seconds': 120,
        # Cửa sổ trượt để đếm số lần restart. Worker chạy ổn định lâu hơn cửa sổ này thì bộ đếm tự về 0.
        'restart_window_seconds': 900,
        # Khi vượt max_restarts trong cửa sổ: tạm ngưng process đó trong cooldown, rồi thử lại. KHÔNG dừng cả cluster.
        'circuit_cooldown_seconds': 600,
        # Health check định kỳ cho process đã ready; quá ngưỡng lỗi liên tiếp → coi là treo và restart.
        'health_interval_seconds': 5,
        'health_timeout_seconds': 3,
        'health_fail_threshold': 6,
        # Chu kỳ kiểm tra job quá hạn (truy vấn SQL), tránh spam DB mỗi 0.5 giây.
        'job_check_interval_seconds': 5,
        # Chu kỳ dọn job hết hạn lưu trữ; 0 = tắt dọn tự động.
        'cleanup_interval_minutes': 60,
    },
    'storage': {
        'root': 'data',
        'min_free_disk_mb': 1024,
        'max_pixels': 12_000_000,
        'max_edge': 1600,
        'max_pdf_pages': 30,
        'max_crops_per_page': 50,
        'retention_days': 30,
        'cleanup_dry_run': True,
    },
    'models': {
        'yolo_path': 'yolo26n.pt',
        'yolo_imgsz': 640,
        'confidence': 0.25,
        'ocr_lang': 'vi',
        'ocr_version': 'PP-OCRv3',
        'ocr_det_dir': None,
        'ocr_rec_dir': None,
        'cpu_threads': 1,
        'max_worker_rss_mb': 8192,
        'min_free_gpu_mb': 512,
        'release_cache_after_job': True,
        # Thư mục cache model của PaddleX/PaddleOCR (tương đối so với app/).
        'paddle_cache_dir': 'models',
    },
    'database': {
        'server': None,
        'name': None,
        'user': None,
        'password': None,
        'encrypt': True,
        'trust_server_certificate': True,
        'connect_timeout': 5,
        'query_timeout': 15,
        'max_connections': 4,
        'acquire_timeout': 10,
        'deadlock_retries': 3,
        'max_manifest_bytes': 1_048_576,
        'app_name': 'VisionQueue',
    },
    'gpu_slots': {},
    'workers': [],
}

# Biến môi trường → (section, key). Secret nên đặt ở đây hoặc trong app/.env thay vì YAML.
ENV_OVERRIDES = {
    'VISION_API_KEY': ('server', 'api_key'),
    'VISION_ADMIN_KEY': ('server', 'admin_key'),
    'VISION_DB_SERVER': ('database', 'server'),
    'VISION_DB_NAME': ('database', 'name'),
    'VISION_DB_USER': ('database', 'user'),
    'VISION_DB_PASSWORD': ('database', 'password'),
}


def _deep_merge(base, override):
    """
    Trộn dict override vào bản sao của base (đệ quy). List và giá trị đơn được thay thế nguyên khối.
    """
    result = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _apply_env(cfg, environ):
    """
    Ghi đè cấu hình bằng biến môi trường (chỉ khi biến có giá trị khác rỗng).
    """
    for name, (section, key) in ENV_OVERRIDES.items():
        value = environ.get(name)
        if value not in (None, ''):
            cfg[section][key] = value


def validate_config(cfg):
    """
    Kiểm tra tính hợp lệ của cấu hình đã trộn. Raise ValueError với thông báo rõ ràng.
    Lỗi cấu hình là lỗi triển khai → dừng ngay lúc khởi động (fail-fast), không để lỗi âm thầm lúc chạy.
    """
    # Lấy danh sách cổng và ID worker để kiểm tra trùng lặp.
    ports = [cfg['server']['port']] + [w.get('port') for w in cfg['workers']]
    ids = [w.get('id') for w in cfg['workers']]

    if not ids or len(ids) != len(set(ids)) or len(ports) != len(set(ports)):
        raise ValueError('Cần ít nhất 1 worker; ID và port không được trùng')

    if any(not isinstance(p, int) or isinstance(p, bool) or not 1024 <= p <= 65535 for p in ports):
        raise ValueError('Port phải là số nguyên 1024..65535')

    for worker in cfg['workers']:
        for key in ('id', 'host', 'port', 'device', 'gpu_id'):
            if key not in worker:
                raise ValueError(f'Worker thiếu khóa bắt buộc: {key}')

        if worker['device'] not in {'auto', 'cpu', 'gpu'}:
            raise ValueError('device chỉ nhận auto/cpu/gpu')

        if not isinstance(worker['gpu_id'], int) or worker['gpu_id'] < 0:
            raise ValueError('gpu_id phải là số nguyên >= 0')

        if not isinstance(worker['id'], str) or not worker['id'].replace('_', '').isalnum():
            raise ValueError('Worker ID chỉ gồm chữ, số, dấu gạch dưới')

    positive = [
        ('server', 'upload_concurrency'),
        ('server', 'upload_timeout_seconds'),
        ('server', 'max_body_mb'),
        ('server', 'max_file_mb'),
        ('queue', 'max_pending'),
        ('queue', 'max_attempts'),
        ('queue', 'poll_seconds'),
        ('queue', 'idle_poll_max_seconds'),
        ('queue', 'job_timeout_seconds'),
        ('queue', 'startup_timeout_seconds'),
        ('queue', 'shutdown_seconds'),
        ('queue', 'max_restarts'),
        ('supervisor', 'restart_backoff_initial_seconds'),
        ('supervisor', 'restart_backoff_max_seconds'),
        ('supervisor', 'restart_window_seconds'),
        ('supervisor', 'circuit_cooldown_seconds'),
        ('supervisor', 'health_interval_seconds'),
        ('supervisor', 'health_timeout_seconds'),
        ('supervisor', 'health_fail_threshold'),
        ('supervisor', 'job_check_interval_seconds'),
        ('storage', 'max_pixels'),
        ('storage', 'max_edge'),
        ('storage', 'max_pdf_pages'),
        ('storage', 'max_crops_per_page'),
        ('storage', 'retention_days'),
        ('models', 'cpu_threads'),
        ('models', 'yolo_imgsz'),
        ('database', 'connect_timeout'),
        ('database', 'query_timeout'),
        ('database', 'max_connections'),
        ('database', 'acquire_timeout'),
    ]
    for section, key in positive:
        value = cfg[section][key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError(f'{section}.{key} phải là số > 0')

    if cfg['supervisor']['cleanup_interval_minutes'] < 0:
        raise ValueError('supervisor.cleanup_interval_minutes phải >= 0 (0 = tắt)')

    if cfg['server']['max_file_mb'] > cfg['server']['max_body_mb']:
        raise ValueError('server.max_file_mb phải <= server.max_body_mb')

    if cfg['queue']['idle_poll_max_seconds'] < cfg['queue']['poll_seconds']:
        raise ValueError('queue.idle_poll_max_seconds phải >= queue.poll_seconds')

    if any(not isinstance(value, int) or value < 1 for value in cfg['gpu_slots'].values()):
        raise ValueError('gpu_slots phải là số nguyên >= 1')

    for key in ('server', 'name'):
        if not cfg['database'][key]:
            raise ValueError(f'Thiếu database.{key} (đặt trong config.yaml hoặc biến VISION_DB_{key.upper()})')

def load_config(path=None, environ=None):
    """
    Đọc file cấu hình YAML, trộn với mặc định và biến môi trường, kiểm tra hợp lệ, trả dict.
    """
    environ = os.environ if environ is None else environ
    # Lấy đường dẫn file cấu hình từ biến môi trường, mặc định là config.yaml trong ROOT.
    path = Path(path or environ.get('VISION_CONFIG', str(ROOT / 'config.yaml')))
    with path.open(encoding='utf-8') as stream:
        # safe_load() đọc YAML an toàn, không thực thi code Python trong file.
        raw = yaml.safe_load(stream) or {}

    cfg = _deep_merge(DEFAULTS, raw)
    _apply_env(cfg, environ)

    # Lấy đường dẫn tuyệt đối của thư mục dữ liệu, đảm bảo thống nhất giữa worker và gateway.
    cfg['storage']['root'] = str((ROOT / cfg['storage']['root']).resolve())
    cfg['models']['paddle_cache_dir'] = str((ROOT / cfg['models']['paddle_cache_dir']).resolve())

    # Chuyển đổi key GPU thành chuỗi để chấp nhận cả dạng 0 và '0' trong YAML.
    cfg['gpu_slots'] = {str(key): value for key, value in (cfg['gpu_slots'] or {}).items()}

    validate_config(cfg)
    return cfg

def security_warnings(cfg):
    """
    Trả danh sách cảnh báo bảo mật (không chặn khởi động) để log lúc startup.
    """
    warnings = []
    for key in ('api_key', 'admin_key'):
        value = cfg['server'].get(key)
        if not value:
            warnings.append(f'server.{key} chưa đặt → endpoint tương ứng KHÔNG yêu cầu xác thực')
        elif str(value) in INSECURE_KEYS or len(str(value)) < 16:
            warnings.append(f'server.{key} quá yếu hoặc là giá trị mẫu; hãy dùng chuỗi ngẫu nhiên >= 16 ký tự')
    if cfg['server']['host'] == '0.0.0.0' and not cfg['server'].get('api_key'):
        warnings.append('Gateway bind 0.0.0.0 nhưng không có api_key → ai trong mạng cũng gửi job được')
    return warnings
