"""
WORKER – logic xử lý job, KHÔNG biết gì về HTTP hay Supervisor.

Vị trí trong kiến trúc:

    main.py (Supervisor) ──spawn──▶ workers/worker_entry.py ──tạo──▶ Worker (file này)

worker_entry.create_app() tạo Worker trong lifespan và gọi start(); mỗi Worker có 1 thread riêng:
    nạp model (Engine) → hoà giải job mồ côi → chờ cluster.ready → lặp: claim → engine.process → finish

Chính sách lỗi (mục tiêu: lỗi tạm thời KHÔNG làm chết process):
    - SQL Server mất kết nối khi claim/finish  → chờ backoff rồi thử lại, worker vẫn sống.
    - VRAM trống thấp trước khi claim          → không nhận job, chờ, thử lại (job để worker khác làm).
    - File đầu vào lỗi (InputError)            → job failed với thông điệp rõ ràng cho client.
    - Lỗi xử lý khác                           → job failed, worker tiếp tục.
    - OOM/CUDA error hoặc release() thất bại   → job failed, process exit 71 để supervisor tạo worker sạch.
    - Lỗi không lường trước khi nạp model/vòng lặp → exit 70, supervisor restart có backoff.
"""
import os
import threading
import time
import traceback
from pathlib import Path

import psutil
import pyodbc

from engine.errors import InputError
from log.system_log import system_logger
from services.database_service import DatabaseError, MyDatabase
from utils.db_concurrency import DatabaseCapacityTimeout
from utils.files import write_json
from utils.resources import check_disk, gpu_slot

# Lỗi "hạ tầng DB" mà worker sẽ chờ rồi thử lại thay vì chết.
DB_ERRORS = (DatabaseError, DatabaseCapacityTimeout, pyodbc.Error)
# Chuỗi trong thông điệp lỗi cho thấy CUDA/bộ nhớ đã ở trạng thái không tin cậy.
FATAL_MARKERS = ('out of memory', 'cuda error', 'cudaerror', 'cudnn_status', 'cublas_status')


def _default_factory(cfg, spec):
    """Nạp Engine thật. Import trễ để module này không kéo torch/paddle khi chỉ cần test/gateway."""
    from engine.engine import Engine  # pylint: disable=import-outside-toplevel
    return Engine(cfg, spec)


class _Throttle:
    """Giới hạn tần suất log cho cùng một loại sự kiện (tránh spam log mỗi 0.25 giây)."""

    def __init__(self):
        self._last = {}

    def ready(self, key, interval):
        now = time.monotonic()
        if now - self._last.get(key, -interval - 1) >= interval:
            self._last[key] = now
            return True
        return False


class Worker:
    """
    Worker chạy vòng lặp claim job từ queue, gọi engine để inference, và báo kết quả về DB.
    """

    def __init__(self, cfg, spec, factory=None, database: MyDatabase = None):
        # factory=None → dùng Engine thật; test có thể truyền engine giả.
        self.cfg, self.spec, self.factory = cfg, dict(spec), factory or _default_factory
        self.database = database
        self.stop = threading.Event()
        self.state = {
            'worker_id': spec['id'],
            'ready': False,
            'state': 'loading',
            'job_id': None,
            'device': spec['device'],
            'gpu_id': spec['gpu_id'],
            'started_at': time.time(),
            'jobs_succeeded': 0,
            'jobs_failed': 0,
            'last_error': None,
        }
        # Lần đầu chạy (hoặc sau lỗi claim không chắc chắn) phải hoà giải job 'running' mồ côi của chính mình.
        self._needs_reconcile = True
        self._throttle = _Throttle()
        self.thread = threading.Thread(target=self.run, name='model-thread', daemon=True)

    # ------------------------------------------------------------------
    # Vòng đời
    # ------------------------------------------------------------------
    def start(self):
        """Bắt đầu thread inference, không block main thread."""
        self.thread.start()

    def close(self):
        """Dừng vòng lặp inference, chờ thread kết thúc tối đa shutdown_seconds."""
        self.stop.set()
        if self.thread.is_alive():
            self.thread.join(timeout=self.cfg['queue']['shutdown_seconds'])

    def snapshot(self):
        """Lấy snapshot trạng thái hiện tại của worker (dùng cho /health)."""
        try:
            memory_info = psutil.Process().memory_info()
            rss = round(memory_info.rss / 2**20, 1)
            vms = round(memory_info.vms / 2**20, 1)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            rss, vms = None, None

        return dict(
            self.state,
            pid=os.getpid(),
            alive=self.thread.is_alive(),
            uptime_seconds=round(time.time() - self.state['started_at'], 1),
            rss_mb=rss,
            vms_mb=vms,
        )

    # ------------------------------------------------------------------
    # Vòng lặp chính
    # ------------------------------------------------------------------
    def _cluster_open(self):
        """Dưới Supervisor, chỉ claim job khi file cluster.ready tồn tại."""
        if not self.cfg.get('_managed_cluster'):
            return True
        return (Path(self.cfg['storage']['root']) / 'cluster.ready').exists()

    def _db_backoff(self, failures):
        """Thời gian chờ khi DB lỗi: 1, 2, 4, ... tối đa 30 giây."""
        return min(2 ** max(failures - 1, 0), 30)

    def _log_db_problem(self, action, exc, failures):
        self.state['last_error'] = f'{action}: {type(exc).__name__}'
        if self._throttle.ready(f'db:{action}', 30):
            system_logger.warning("Worker %s: lỗi DB khi %s (%s), lần lỗi liên tiếp=%d; sẽ thử lại",
                                  self.spec['id'], action, type(exc).__name__, failures)

    def run(self):
        """
        Vòng lặp claim job từ queue, gọi engine để inference, và báo kết quả về DB.
        """
        engine = None
        try:
            system_logger.info("Bắt đầu nạp model cho worker %s", self.spec['id'])
            # Nạp model + warmup, giữ GPU slot để không tranh VRAM với worker khác đang nạp/chạy.
            with gpu_slot(self.cfg, self.spec, self.stop):
                engine = self.factory(self.cfg, self.spec)

            self.spec['device'] = engine.device
            self.state.update(ready=True, state='idle', device=engine.device)

            poll = self.cfg['queue']['poll_seconds']
            idle_max = self.cfg['queue']['idle_poll_max_seconds']
            delay = poll
            db_failures = 0

            while not self.stop.is_set():
                if not self._cluster_open():
                    self.state['state'] = 'waiting_cluster'
                    self.stop.wait(poll)
                    continue

                # Hoà giải: process này vừa khởi động (hoặc vừa gặp lỗi claim không chắc chắn) nên KHÔNG
                # đang xử lý job nào. Mọi job 'running' mang owner = worker này đều là mồ côi → đưa về queue.
                if self._needs_reconcile:
                    try:
                        recovered = self.database.recover(self.spec['id'])
                        if recovered:
                            system_logger.warning("Worker %s hoà giải job mồ côi: %s", self.spec['id'], recovered)
                        self._needs_reconcile = False
                        db_failures = 0
                    except DB_ERRORS as exc:
                        db_failures += 1
                        self.state['state'] = 'db_unavailable'
                        self._log_db_problem('recover', exc, db_failures)
                        self.stop.wait(self._db_backoff(db_failures))
                        continue

                job = None
                wait_seconds = None
                self.state['state'] = 'waiting_resource'
                with gpu_slot(self.cfg, self.spec, self.stop):
                    if self.stop.is_set():
                        break

                    # Kiểm tra VRAM TRƯỚC khi nhận job: thiếu VRAM thì nhường job cho worker khác.
                    try:
                        engine.memory_guard()
                    except MemoryError as exc:
                        self.state.update(state='waiting_vram', last_error=str(exc))
                        if self._throttle.ready('vram', 60):
                            system_logger.warning("Worker %s: %s; tạm không nhận job", self.spec['id'], exc)
                        wait_seconds = 5
                    else:
                        try:
                            job = self.database.claim(self.spec['id'])
                            db_failures = 0
                        except DB_ERRORS as exc:
                            # Claim có thể đã commit nhưng mất ACK → lần sau phải hoà giải trước khi claim tiếp.
                            self._needs_reconcile = True
                            db_failures += 1
                            self.state['state'] = 'db_unavailable'
                            self._log_db_problem('claim', exc, db_failures)
                            wait_seconds = self._db_backoff(db_failures)

                        if job is not None:
                            self.execute(engine, job)

                self.state.update(state='idle', job_id=None)
                if wait_seconds is None:
                    # Có job → poll lại ngay chu kỳ ngắn; queue rỗng → giãn dần để giảm tải SQL Server.
                    delay = poll if job is not None else min(delay * 2, idle_max)
                    wait_seconds = delay
                self.stop.wait(wait_seconds)

        except InterruptedError:
            system_logger.info("Worker %s đã dừng", self.spec['id'])

        except BaseException:  # pylint: disable=broad-except
            self.state.update(ready=False, state='fatal')
            system_logger.critical("Worker %s gặp lỗi nghiêm trọng: %s", self.spec['id'], traceback.format_exc())
            # Supervisor thấy exit 70 → restart có backoff.
            os._exit(70)

        finally:
            self.state.update(ready=False, state='stopped')

    # ------------------------------------------------------------------
    # Xử lý một job
    # ------------------------------------------------------------------
    def execute(self, engine, job):
        """
        Thực hiện inference cho một job, gọi engine.process và báo kết quả về DB.
        """
        self.state.update(state='running', job_id=job['id'])

        base = Path(self.cfg['storage']['root']) / 'jobs' / job['id']
        folder = base / f'attempt_{job["attempt"]:03d}'
        start = time.perf_counter()
        fatal = False
        error = None

        try:
            folder.mkdir(parents=True, exist_ok=True)
            check_disk(self.cfg)
            memory = engine.memory_guard() if hasattr(engine, 'memory_guard') else {}
            system_logger.info("Bắt đầu xử lý job %s, attempt %d, worker %s, memory=%s",
                               job['id'], job['attempt'], self.spec['id'], memory)
            engine.process(job, folder)

        except InputError as exc:
            # Lỗi do file của client: thông điệp an toàn để trả về, không phải lỗi server.
            error = f'invalid_input: {exc}'
            system_logger.info("Job %s bị từ chối do đầu vào không hợp lệ: %s", job['id'], exc)
            self._write_error(folder, job, 'InputError', str(exc))

        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc).lower()
            fatal = isinstance(exc, MemoryError) or any(marker in message for marker in FATAL_MARKERS)
            error = 'memory_or_cuda_error' if fatal else f'processing_error: {type(exc).__name__}'
            system_logger.error("Job %s, attempt %d, worker %s failed: %s",
                                job['id'], job['attempt'], self.spec['id'], traceback.format_exc())
            self._write_error(folder, job, type(exc).__name__, 'Processing failed; see server logs')

        finally:
            try:
                # Bỏ tham chiếu/cache tạm; model vẫn giữ cho job kế tiếp.
                engine.release()
            except Exception:  # pylint: disable=broad-except
                fatal = True
                system_logger.error("Failed to release engine for job %s: %s", job['id'], traceback.format_exc())

        artifacts = self.artifacts(base, folder) if folder.exists() else []
        accepted = self._finish(job, artifacts, error)

        elapsed = round((time.perf_counter() - start) * 1000)
        if error is None and accepted:
            self.state['jobs_succeeded'] += 1
            system_logger.info("Hoàn thành job %s, attempt %d, worker %s, elapsed_ms=%d, artifacts=%d",
                               job['id'], job['attempt'], self.spec['id'], elapsed, len(artifacts))
        else:
            self.state['jobs_failed'] += 1
            self.state['last_error'] = error or 'finish_not_accepted'

        if fatal:
            # CUDA/bộ nhớ không còn tin cậy → thoát để supervisor tạo process sạch. Kết quả đã ghi DB ở trên.
            system_logger.critical("Worker %s tự thoát (71) sau lỗi bộ nhớ/CUDA ở job %s", self.spec['id'], job['id'])
            os._exit(71)

    @staticmethod
    def _write_error(folder, job, error_type, message):
        try:
            folder.mkdir(parents=True, exist_ok=True)
            write_json(folder / 'error.json', {'job_id': job['id'], 'error_type': error_type, 'message': message})
        except OSError:
            pass

    def _manifest_safe(self, artifacts):
        """Đảm bảo manifest không vượt giới hạn: bỏ crop trước, cuối cùng mới bỏ hết."""
        try:
            self.database.build_manifest(artifacts)
            return artifacts, None
        except ValueError:
            reduced = [path for path in artifacts if '/crops/' not in path]
            try:
                self.database.build_manifest(reduced)
                return reduced, 'artifact_manifest_truncated'
            except ValueError:
                return [], 'artifact_manifest_too_large'

    def _finish(self, job, artifacts, error):
        """
        Ghi kết quả về DB. Kết quả đã nằm trên đĩa nên nếu DB mất kết nối, worker CHỜ và thử lại
        (Finish idempotent theo token + payload) thay vì bỏ job ở trạng thái running.
        :return: True nếu SQL chấp nhận kết quả.
        """
        artifacts, note = self._manifest_safe(artifacts)
        if note:
            system_logger.warning("Job %s: %s", job['id'], note)
            if note == 'artifact_manifest_too_large' and error is None:
                error = note

        failures = 0
        while True:
            try:
                accepted = self.database.finish(job, artifacts=artifacts, error=error)
                if not accepted:
                    system_logger.warning("Job %s attempt %d: kết quả không được chấp nhận (job đã đổi chủ)",
                                          job['id'], job['attempt'])
                return accepted
            except DB_ERRORS as exc:
                failures += 1
                if self.stop.is_set():
                    # Đang tắt: supervisor sẽ recover job này, lần chạy sau sẽ xử lý lại.
                    system_logger.warning("Job %s: bỏ ghi kết quả vì worker đang dừng", job['id'])
                    return False
                if isinstance(exc, pyodbc.Error) and not isinstance(exc, pyodbc.OperationalError):
                    system_logger.error("Job %s: Finish bị SQL từ chối, không thử lại: %s", job['id'], exc)
                    return False
                self.state['state'] = 'finishing_retry'
                self._log_db_problem('finish', exc, failures)
                self.stop.wait(self._db_backoff(failures))
            except ValueError as exc:
                system_logger.error("Job %s: dữ liệu Finish không hợp lệ: %s", job['id'], exc)
                return False

    @staticmethod
    def artifacts(base, folder):
        """
        Lấy danh sách artifact của một job attempt (đường dẫn tương đối so với base, dùng '/').
        """
        return sorted(str(p.relative_to(base)).replace('\\', '/')
                      for p in folder.rglob('*') if p.is_file() and not p.name.endswith('.tmp'))


if __name__ == '__main__':
    # Chỉ để debug 1 worker độc lập (không HTTP, không Supervisor, không chờ cluster.ready).
    # Chạy thật: python src/main.py
    from config.config import load_config
    config = load_config()
    Worker(config, config['workers'][0], database=MyDatabase.from_config(config)).run()
