"""
Dọn tài liệu cũ bằng python cleanup.py; mặc định dry-run, không tự xóa.
"""
import shutil
import time
import portalocker
from config.config import load_config
from services.database_service import MyDatabase


def cleanup(cfg, database : MyDatabase = None):
    """
    Xóa các job đã hoàn thành quá hạn; không xóa queued/running.
    :param cfg: dict cấu hình nạp từ config.yaml.
    :return: số job phù hợp điều kiện.
    """
    locks=cfg['storage']['root']/'locks'  # Khóa chung với supervisor/worker.

    # Tạo folder locks nếu chưa có, tránh lỗi khi chạy lần đầu.
    locks.mkdir(exist_ok=True)
    # Tính thời gian cutoff để xóa job đã hoàn thành quá hạn. Là thời gian hiện tại trừ đi retention_days (ngày lưu trữ) * 86400 (số giây trong 1 ngày).
    cutoff=time.time()-cfg['storage']['retention_days']*86400

    # Dùng portalocker để tránh xung đột với server đang chạy. Nếu server đang chạy, sẽ không dọn.
    with portalocker.Lock(locks/'supervisor.lock',mode='a',timeout=0):
        # Dùng lock để tránh xung đột với các child process còn sống. Nếu có child còn sống, sẽ không dọn.
        with portalocker.Lock(locks/'children.alive.lock',mode='a',timeout=0):

            rows=database.expired(cutoff)
            if not rows:  # Không có job nào phù hợp điều kiện.
                return 0  # Không có job nào phù hợp điều kiện.

            # Dọn job đã hoàn thành quá hạn.
            for row in rows:  # Mỗi job có một folder chứa original và mọi attempt.
                print(dict(row),'DRY-RUN' if cfg['storage']['cleanup_dry_run'] else 'DELETE')  # Hiển thị chính xác job bị tác động.

                # Nếu dry-run, chỉ in ra thông tin mà không xóa gì cả.
                if cfg['storage']['cleanup_dry_run']:
                    continue

                # Xác định folder của job dựa trên ID, đảm bảo không xóa ra ngoài storage.
                folder=(cfg['storage']['root']/'jobs'/row['id']).resolve()
                # Chặn xóa ra ngoài storage.
                if not folder.is_relative_to((cfg['storage']['root']/'jobs').resolve()):  # Chặn metadata bị sửa dẫn ra ngoài storage.
                    raise ValueError('Unsafe job path')
                
                if folder.exists():
                    shutil.rmtree(folder)  # Xóa cả original, crops, text và attempts của job hết hạn.

                # Xóa metadata của job khỏi cơ sở dữ liệu, chỉ xóa nếu folder đã xóa thành công.
                database.delete_terminal(row['id'], cutoff)

            return len(rows)

if __name__=='__main__':  # Không tự dọn lúc server import module.
    print('Matched jobs:',cleanup(load_config()))  # Không truyền argument; cấu hình ở YAML.
