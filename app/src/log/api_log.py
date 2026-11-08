import logging
import datetime as _dt
import os
import time
import threading
import shutil
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # Tự động tìm và nạp file .env ở thư mục hiện tại


# Đường dẫn thư mục lưu trữ file log
# Mặc định: app/log/api_log (tuyệt đối, không phụ thuộc thư mục làm việc hiện tại).
API_LOG_DIRECTORY = os.getenv("API_LOG_DIRECTORY") or str(Path(__file__).resolve().parents[2] / "log" / "api_log")

# Tạo thư mục nếu chưa có
Path(API_LOG_DIRECTORY).mkdir(parents=True, exist_ok=True)


class CustomFilter(logging.Filter):
    def filter(self, record):
        # Các trường sẽ có trong log, nếu ko có giá trị thì mặc định là "-"
        record.ip = getattr(record, "ip", "None")
        record.hostname = getattr(record, "hostname", "None")
        record.api_name = getattr(record, "api_name", "None")
        record.params = getattr(record, "params", "None")
        record.result = getattr(record, "result", "None")
        record.method = getattr(record, "method", "None")
        record.status = getattr(record, "status", "None")
        record.duration_ms = getattr(record, "duration_ms", "None")
        record.user_agent = getattr(record, "user_agent", "None")
        record.correlation_id = getattr(record, "correlation_id", "None")
        record.request_body = getattr(record, "request_body", "None")
        return True

def _today_str():
    # Định dạng thư mục theo ngày: DD-MM-YY
    return _dt.datetime.now().strftime("%d-%m-%y")

def _log_file_path(day_str=None):
    """
    Tạo thư mục logs/<DD-MM-YY>/ nếu chưa có.
    Trả về đường dẫn file 'api_log.txt' bên trong.
    Có fallback khi lỗi IO.
    """
    try:
        day = day_str or _today_str()
        log_dir = os.path.join(API_LOG_DIRECTORY, day)
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, "api_log.log")
    
    except Exception:
        # fallback
        fb_dir = os.path.join(API_LOG_DIRECTORY, "fallback")
        os.makedirs(fb_dir, exist_ok=True)
        return os.path.join(fb_dir, "api_log.log")

def _remove_old_logs(logs_root= API_LOG_DIRECTORY, max_days=30):
    """
    Xoá thư mục ngày cũ hơn max_days.
    Bỏ qua thư mục không đúng định dạng DD-MM-YY.
    """
    try:
        if not os.path.exists(logs_root):
            return
        
        now = _dt.datetime.now()
        # Duyệt các thư mục trong đường dẫn chứa các thư mục log theo ngày
        for entry in os.listdir(logs_root):
            
            entry_path = os.path.join(logs_root, entry)
            if not os.path.isdir(entry_path):
                continue
            try:
                folder_date = _dt.datetime.strptime(entry, "%d-%m-%y")
            except ValueError:
                # Không phải thư mục ngày -> bỏ qua (vd: 'fallback')
                continue

            # Kiểm tra thời gian đã tạo thư mục
            age_days = (now - folder_date).days
            if age_days > max_days:
                shutil.rmtree(entry_path, ignore_errors=True)

    except Exception:
        # Dọn rác không nên gây crash app
        pass


# =========================
# Cấu hình logger chính & thread xoay theo ngày
# =========================

# Formatter: Tạo log theo các trường thông tin yêu cầu ban đầu
_formatter = logging.Formatter(
    "%(asctime)s - %(hostname)s - %(ip)s - %(method)s %(api_name)s - "
    "status: %(status)s - duration: %(duration_ms)s ms - cid: %(correlation_id)s - ua: %(user_agent)s - "
    "params: %(params)s - request_body: %(request_body)s - result: %(result)s",
    datefmt="%d-%m-%Y %H:%M:%S %p",  # Định dạng thời gian (ngày-tháng-năm giờ:phút:giây AM/PM)
)

# Logger ứng dụng
logger = logging.getLogger("api_logger")
logger.setLevel(logging.INFO)
logger.propagate = False  # Không đẩy lên root

# Handler đầu tiên khi khởi chạy phần mềm (ngày hiện tại)
_file_handler_lock = threading.Lock()
_current_day = _today_str()
_file_handler = logging.FileHandler(_log_file_path(_current_day), encoding="utf-8")
_file_handler.addFilter(CustomFilter())
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)


def _rotate_if_new_day():
    """
    Kiểm tra nếu sang ngày mới:
    - Gỡ handler cũ, đóng file.
    - Dọn rác thư mục cũ.
    - Tạo handler mới cho ngày mới.
    Dùng lock để thay handler an toàn.
    """
    global _current_day, _file_handler
    day_now = _today_str()
    if day_now == _current_day:
        return

    with _file_handler_lock:
        # Kiểm tra lại trong lock để tránh race
        if day_now == _current_day:
            return

        # Tháo & đóng handler cũ
        try:
            logger.removeHandler(_file_handler)
            _file_handler.close()
        except Exception:
            pass

        # Dọn rác log cũ
        _remove_old_logs(max_days=30)

        # Tạo handler mới
        _current_day = day_now
        new_handler = logging.FileHandler(_log_file_path(_current_day), encoding="utf-8")
        new_handler.addFilter(CustomFilter())
        new_handler.setFormatter(_formatter)
        logger.addHandler(new_handler)
        _file_handler = new_handler


def _rotation_thread():
    """
    Thread nền: mỗi 1 tiếng kiểm tra xem có sang ngày mới chưa.
    (Không cần tick quá dày, tránh overhead)
    """
    while True:
        try:
            _rotate_if_new_day()
        except Exception:
            # Tuyệt đối không để thread chết âm thầm vì exception
            pass

        time.sleep(3600)


# Khởi động thread nền (daemon)
_t = threading.Thread(target=_rotation_thread, name="DailyLogRotationThread", daemon=True)
_t.start()

# ===== có thể thêm Console handler ở mức WARNING để thấy lỗi ngay trong stdout =====
# _console = logging.StreamHandler()
# _console.setLevel(logging.WARNING)
# _console.setFormatter(_formatter)
# _console.addFilter(CustomFilter())
# logger.addHandler(_console)