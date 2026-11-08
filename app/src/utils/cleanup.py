"""
Dọn job đã kết thúc (succeeded/failed) quá storage.retention_days.

- Supervisor tự gọi định kỳ (supervisor.cleanup_interval_minutes).
- Chạy tay:  cd app/src && python -m utils.cleanup   (an toàn cả khi server đang chạy:
  chỉ đụng job terminal, và DeleteTerminal kiểm tra lại trạng thái trong SQL trước khi xoá).
- storage.cleanup_dry_run=true → chỉ log job sẽ bị xoá, không xoá gì.
"""
import logging
import shutil
import time
from pathlib import Path

import portalocker

from services.database_service import MyDatabase

logger = logging.getLogger(__name__)


def cleanup(cfg, database: MyDatabase, *, now=None) -> int:
    """
    Xóa file + metadata của job terminal quá hạn; không đụng queued/running.
    :return: số job phù hợp điều kiện (dry-run vẫn đếm).
    """
    # Lấy đường dẫn thư mục storage, jobs, locks. Tạo thư mục locks nếu chưa có.
    root = Path(cfg['storage']['root'])
    jobs_root = (root / 'jobs').resolve()
    locks = root / 'locks'
    locks.mkdir(parents=True, exist_ok=True)
    dry_run = bool(cfg['storage']['cleanup_dry_run'])

    # Tính thời điểm cắt (cutoff) dựa trên retention_days. Job kết thúc trước thời điểm này sẽ bị xoá.
    cutoff = (now or time.time()) - cfg['storage']['retention_days'] * 86400

    try:
        lock = portalocker.Lock(locks / 'cleanup.lock', mode='a', timeout=0)
        lock.acquire()
    except portalocker.exceptions.LockException:
        logger.info('Một tiến trình cleanup khác đang chạy; bỏ qua lượt này')
        return 0

    try:
        rows = database.expired(cutoff)
        for row in rows:
            job_id = row['id']
            if dry_run:
                logger.info('[DRY-RUN] Job hết hạn sẽ bị xoá: %s (%s, kết thúc %s)',
                            job_id, row.get('status'), row.get('finished'))
                continue

            folder = (jobs_root / job_id).resolve()
            if not folder.is_relative_to(jobs_root) or folder == jobs_root:
                logger.error('Bỏ qua job %s: đường dẫn không an toàn', job_id)
                continue
            try:
                if folder.exists():
                    shutil.rmtree(folder)
            except OSError as exc:
                # Giữ metadata để lượt sau thử lại; không để một file bị khoá chặn cả quá trình.
                logger.warning('Không xoá được thư mục job %s: %s', job_id, exc)
                continue
            database.delete_terminal(job_id, cutoff)
            logger.info('Đã xoá job hết hạn %s', job_id)
        return len(rows)
    finally:
        lock.release()


if __name__ == '__main__':
    from config.config import load_config

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    config = load_config()
    print('Matched jobs:', cleanup(config, MyDatabase.from_config(config)))
