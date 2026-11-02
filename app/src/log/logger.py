"""
Cấu hình logger cho ứng dụng, bao gồm việc tạo thư mục log theo ngày tháng năm, xóa các log cũ quá hạn, và thay đổi vị trí lưu log khi sang ngày mới.
"""
import sys
import os
import shutil
import datetime
import logging

# Thư mục lưu trữ log, mặc định là "DucQuan_log" trong AppData/Roaming của người dùng
APP_FOLDER_LOG = "AI_Vision_log"
# Tạo thời gian ghi log
TODAY = str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
DATE = str(datetime.datetime.now().strftime("%d-%m-%y"))
# Số ngày lưu trữ log, nếu quá thời gian này thì tự động xóa tệp
LOG_EXPIRATION_DAYS = 15


# Khai báo logger theo tên module hiện tại, để có thể ghi log riêng cho từng module nếu cần thiết
logger = logging.getLogger(__name__)

# Lấy đường dẫn đến thư mục AppData/Roaming của người dùng
appdata_dir = os.getenv('APPDATA')
# thư mục chứa log: AppData/Roaming/LOG_FOLDER/log
log_root_dir = os.path.join(appdata_dir, APP_FOLDER_LOG, "log")

# Tạo thư mục chứa nhật ký theo ngày tháng năm, nếu thư mục đã tồn tại thì không tạo nữa
# C:\\Users\\238299\\AppData\\Roaming\QL nhân sự\\log\\03-03-25
save_dir_log = os.path.join(appdata_dir, APP_FOLDER_LOG, "log", DATE)
os.makedirs(save_dir_log, exist_ok=True)

# Tạo 2 tệp chứa log và các câu lệnh print nếu tồn tại
log_file_path = save_dir_log +"/log.log"
log_print_app = save_dir_log + "/system_out.log"

# Với phương thức open thì sẽ tự động tạo file log nếu nó chưa tồn tại
# ghi các câu lệnh print ra tệp log, để tiện theo dõi nhật ký khi chạy ứng dụng
sys.stdout = open(log_print_app, encoding="utf-8", mode="a")
sys.stderr = open(log_print_app, encoding="utf-8", mode="a")

def delete_old_logs():
    """
    Xóa các thư mục log đã tồn tại quá thời gian lưu trữ cho phép.
    """
    # Không có thư mục log thì không cần làm gì
    if not os.path.exists(log_root_dir):
        return

    # Lấy ngày hiện tại
    now = datetime.datetime.now()

    # Duyệt tất cả các thư mục con trong thư mục `log` chứa các thư mục log con
    for folder in os.listdir(log_root_dir):
        folder_path = os.path.join(log_root_dir, folder)

        # Kiểm tra nếu nó là thư mục (vì mỗi ngày là một thư mục riêng)
        if os.path.isdir(folder_path):
            try:
                # Lấy thời gian chỉnh sửa cuối cùng của thư mục
                folder_mod_time = datetime.datetime.fromtimestamp(os.stat(folder_path).st_mtime)

                # Kiểm tra nếu thư mục đã tồn tại quá thời gian lưu trữ cho phép thì xóa nó đi
                if (now - folder_mod_time).days > LOG_EXPIRATION_DAYS:
                    # Xóa toàn bộ thư mục và nội dung bên trong
                    logger.info("🗑️ Xóa thư mục log cũ: %s", folder_path)
                    # os.remove(folder_path)  # os gặp lỗi Access is denied, chuyển sang shutil
                    shutil.rmtree(folder_path)

            except Exception as e: #pylint: disable=broad-except
                logger.error("⚠️ Không thể xóa thư mục %s: %s", folder_path, e, exc_info=True)

def change_log_file_path(logger_root: logging.Logger, new_log_file_path=None):
    """
    Thay đổi vị trí tệp log mà không thay đổi các thiết lập khác của logger.  
    Dùng cho trường hợp sang 1 ngày mới, tự động chuyển sang vị trí log mới

    - Nếu logger_root được truyền vào là `logger = logging.getLogger(__name__)` thì nó chỉ ảnh hưởng mỗi tệp đó
    """
    # Nếu không chỉ định vị trí mới thì tự tạo thư mục theo ngày tháng năm
    if new_log_file_path is None:
        # data test:
        # current_today = str((datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%d-%m-%y")) # Tăng thêm 1 ngày

        # Tạo thời gian ghi log
        current_today = str(datetime.datetime.now().strftime("%d-%m-%y"))
        # C:\\Users\\238299\\AppData\\Roaming\QLNS\\log\\03-03-25
        new_dir_log = os.path.join(appdata_dir, APP_FOLDER_LOG, "log", current_today)

        # Tạo 2 tệp chứa log và các câu lệnh print nếu tồn tại
        new_log_file_path = new_dir_log +"/log.log"
        new_log_print_app = new_dir_log + "/system_out.log"
    else:
        # Nếu người dùng tự truyền file log vào, ta tự động trích xuất thư mục cha của file đó
        new_log_file_path = os.path.abspath(new_log_file_path)
        new_dir_log = os.path.dirname(new_log_file_path)
        # File chứa print sẽ nằm chung thư mục với file log đó
        new_log_print_app = os.path.join(new_dir_log, "system_out.log")

    # Kiểm tra xem thư mục chứa log đã được tạo chưa, nếu chưa có thì mới thực hiện tạo và thay đổi log, nếu có thì bỏ qua
    if not os.path.exists(new_dir_log):

        # Ghi log
        current_log_file_path = next(
            (handler.baseFilename for handler in logger_root.handlers
             if isinstance(handler, logging.FileHandler)),
            "<chưa xác định>",
        )
        logger.info("Thay đổi vị trí lưu log mới từ: %s sang vị trí: %s",
                    current_log_file_path, new_log_file_path)

        # Kiểm tra và xóa các thư mục cũ đã tồn tại quá lâu
        delete_old_logs()

        try:
            # Tao thư mục mới
            os.makedirs(new_dir_log)

            # Xóa tất cả các handler cũ của logger
            for handler in logger_root.handlers[:]:
                handler.close()  # Đóng handler cũ
                logger_root.removeHandler(handler)  # Xóa handler cũ

            # Tạo một FileHandler mới với vị trí tệp log mới
            file_handler = logging.FileHandler(new_log_file_path, mode='a', encoding='utf-8')
            # Định dạng cho log
            formatter = logging.Formatter('%(asctime)s %(levelname)s:\t %(filename)s - Line: %(lineno)d message: %(message)s',
                                           datefmt='%d/%m/%Y %I:%M:%S %p')
            file_handler.setFormatter(formatter)

            # Thêm FileHandler mới vào logger
            logger_root.addHandler(file_handler)
            # Thiết lập mức độ log
            logger_root.setLevel(logging.INFO)

            # Đóng file cũ nếu đang redirect
            try:
                if sys.stdout not in (sys.__stdout__, sys.__stderr__):
                    sys.stdout.close()
            except Exception as e: #pylint: disable=broad-except
                logger.error("Không thể đóng print đầu ra mặc định: %s", e, exc_info=True)
            try:
                if sys.stderr not in (sys.__stdout__, sys.__stderr__):
                    sys.stderr.close()
            except Exception as e: #pylint: disable=broad-except
                logger.error("Không thể error đóng đầu ra mặc định: %s", e, exc_info=True)

            # Trả lại về mặc định trước khi mở file mới
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

            # Mở file mới cho ngày mới
            sys.stdout = open(new_log_print_app, encoding="utf-8", mode="a")
            sys.stderr = open(new_log_print_app, encoding="utf-8", mode="a")

            return True

        except Exception as e: #pylint: disable=broad-except
            logger.error("Không thể thay đổi vị trí lưu tệp log. Lỗi: %s", e, exc_info=True)
            return False

    else:
        # Nếu thư mục này đã tồn tại thì không làm gì cả
        return True
