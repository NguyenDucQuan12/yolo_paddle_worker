"""
SUPERVISOR – tiến trình cha, chịu trách nhiệm:
    1. Chờ SQL Server sẵn sàng + kiểm tra schema (không crash nếu DB chưa lên, chỉ chờ và log).
    2. Dọn tiến trình con mồ côi của lần chạy trước, recover job dang dở.
    3. Spawn gateway và các worker (worker nạp model TUẦN TỰ từng cái một).
    4. Mở barrier `cluster.ready` khi lượt khởi động đầu tiên kết thúc để worker bắt đầu claim job.
    5. Watchdog: restart tiến trình con khi chết / treo / job quá hạn / vượt RAM / khởi động quá lâu,
       với exponential backoff + circuit breaker RIÊNG cho từng process.
       Một worker hỏng KHÔNG làm dừng gateway hay các worker khác.
    6. Dọn job hết hạn lưu trữ định kỳ (storage.retention_days).
    7. Khi Ctrl+C / SIGTERM: dừng toàn bộ tiến trình con và recover job đang chạy.
"""
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import portalocker
import psutil
import pyodbc

from config.config import ROOT, load_config, security_warnings
from log.system_log import _formatter as system_formatter
from log.system_log import _rotation_thread, system_logger
from services.database_service import DatabaseError, MyDatabase
from utils.cleanup import cleanup
from utils.db_concurrency import DatabaseCapacityTimeout
from utils.files import write_json
from utils.processes import stop_process
from utils.restart_policy import RestartPolicy

# File khởi tạo gateway và worker (tiến trình con).
CHILD_ENTRY = ROOT / 'src' / 'workers' / 'worker_entry.py'
GATEWAY_ID = 'gateway'
# Chu kỳ vòng watchdog.
POLL_SECONDS = 0.5
# Lỗi hạ tầng DB: supervisor bỏ qua lượt kiểm tra đó và thử lại sau, KHÔNG dừng.
DB_ERRORS = (DatabaseError, DatabaseCapacityTimeout, pyodbc.Error)


class Child:
    """
    Trạng thái của một tiến trình con (gateway hoặc worker).
    Supervisor giữ thông tin này trong bộ nhớ, ghi ra file supervisor_status.json để endpoint /admin/workers
    có thể báo cáo trạng thái.
    """

    def __init__(self, identity, spec, policy):
        """
        Chỉ cần khai báo identity (gateway/worker_x), spec (None = gateway, dict = worker) và policy là bộ đếm lỗi riêng của từng process.  
        Các thông tin còn lại được cập nhật bởi Supervisor.
        """
        self.identity = identity
        self.spec = spec                  # None = gateway
        self.policy = policy
        self.process = None
        self.console = None
        self.started = 0.0
        self.ready = False
        self.state = 'pending'            # pending/starting/ready/backoff/circuit_open/stopped
        self.next_spawn_at = 0.0
        self.next_health_at = 0.0
        self.next_job_check_at = 0.0
        self.health_failures = 0
        self.needs_recover = False
        self.attempted = False            # đã qua ít nhất một lượt khởi động (thành công hoặc lỗi)
        self.restarts_total = 0
        self.last_reason = None
        self.rss_warned = False

    @property
    def is_worker(self):
        """
        :return: True nếu là worker, False nếu là gateway.
        """
        return self.spec is not None

    def to_status(self):
        """
        :return: dict trạng thái để ghi ra file supervisor_status.json.
        """
        return {
            'id': self.identity,
            'state': self.state,
            'pid': self.process.pid if self.process else None,
            'ready': self.ready,
            'restarts_total': self.restarts_total,
            'recent_failures': self.policy.recent_failures,
            'circuit_trips': self.policy.circuit_trips,
            'last_reason': self.last_reason,
            'next_spawn_in_seconds': (max(0.0, round(self.next_spawn_at - time.monotonic(), 1))
                                      if self.process is None else None),
        }


class Supervisor:
    """Quản lý vòng đời gateway và các worker."""

    def __init__(self, cfg, database: MyDatabase = None):
        """
        Khởi tạo câu hình supervisor.
        :param: 
        cfg cấu hình từ config/config.py
        database đối tượng MyDatabase
        """
        # Đọc cấu hình từ config/config.py
        self.cfg = cfg

        # Thư mục lưu trữ
        self.root = Path(cfg['storage']['root'])
        self.locks = self.root / 'locks'
        self.locks.mkdir(parents=True, exist_ok=True)
        # Khi file này tồn tại, worker mới được phép claim job (xem Worker._cluster_open).
        self.ready_flag = self.root / 'cluster.ready'
        self.status_file = self.root / 'supervisor_status.json'

        # Các cờ trạng thái
        self.stop = False                 # cờ dừng; _on_signal đặt True khi bấm Ctrl+C
        self.barrier_open = False         # đã tạo cluster.ready chưa (chỉ mở 1 lần)
        self._next_status_write = 0.0     # mốc thời gian được ghi status lần tới (tối đa 2s ghi một lần)


        # Khởi tạo Database service (MyDatabase) nếu chưa có.
        self.database = database or MyDatabase.from_config(cfg)

        # Tạo danh sách tiến trình con: 1 gateway + N worker.
        self.children = [Child(GATEWAY_ID, None, RestartPolicy.from_config(cfg))]
        self.children += [Child(spec['id'], spec, RestartPolicy.from_config(cfg)) for spec in cfg['workers']]

    def install_signal_handlers(self):
        """
        Cài đặt xử lý Ctrl+C / SIGTERM để supervisor dừng có trật tự.
        Chương trình không lập tức thoát, mà supervisor khi nhận tín hiệu sẽ dừng các tiến trình con, recover job đang chạy, rồi mới thoát.
        """
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)
        if hasattr(signal, 'SIGBREAK'):  # Windows: Ctrl+Break / đóng cửa sổ console.
            signal.signal(signal.SIGBREAK, self._on_signal)

    def _on_signal(self, _signum, _frame):
        """
        Đặt cờ dừng, vòng watchdog sẽ dừng các tiến trình con và thoát.
        """
        self.stop = True

    def _sleep(self, seconds):
        """
        Dừng vòng watchdog trong khoảng thời gian nhất định, nhưng vẫn kiểm tra cờ stop.
        """
        deadline = time.monotonic() + seconds
        while not self.stop and time.monotonic() < deadline:
            time.sleep(min(POLL_SECONDS, max(0.0, deadline - time.monotonic())))

    # ------------------------------------------------------------------
    # Luồng chính
    # ------------------------------------------------------------------
    def run(self):
        """
        Chạy vòng đời supervisor: chờ DB, recover job, spawn tiến trình con, watchdog.
        """
        try:
            # Lấy lock độc quyền để tránh chạy 2 supervisor trên cùng thư mục storage.
            # Thời gian chờ 0 giây: nếu lock đang bị giữ bởi supervisor khác, thoát ngay.
            supervisor_lock = portalocker.Lock(self.locks / 'supervisor.lock', mode='a', timeout=0)
            supervisor_lock.acquire()
        except portalocker.exceptions.LockException:
            system_logger.critical("Đã có một Supervisor khác đang chạy trên thư mục %s; thoát.", self.root)
            return 1

        try:
            for warning in security_warnings(self.cfg):
                system_logger.warning("CẢNH BÁO BẢO MẬT: %s", warning)

            # Chờ SQL Server sẵn sàng + schema hợp lệ. Nếu DB chưa lên → log + chờ (backoff), KHÔNG crash.
            # Nếu schema/cấu hình sai → raise (lỗi triển khai, cần người sửa
            if not self._wait_for_database():
                return 0

            # Đưa các job 'running' của lần chạy trước về lại hàng đợi. Chỉ làm khi giữ được lock children.alive.lock tức không còn process con cũ nào sống.
            self._recover_on_startup()
            if self.stop:
                return 0

            # Dọn tiến trình con mồ côi của lần chạy trước (chạy worker_entry.py của chính thư mục này) còn sống.
            threading.Thread(target=self._cleanup_loop, name='retention-cleanup', daemon=True).start()
            try:
                self._watchdog_loop()
            finally:
                self._shutdown()
            return 0
        finally:
            supervisor_lock.release()

    def _wait_for_database(self):
        """
        Chờ SQL Server phản hồi và schema hợp lệ. DB chưa lên → log + chờ (backoff), KHÔNG crash.
        Schema/cấu hình sai → raise (lỗi triển khai, cần người sửa).
        :return: False nếu bị yêu cầu dừng trong lúc chờ.
        """
        # thời gian chờ ban đầu 2 giây, tăng gấp đôi mỗi lần lỗi, tối đa 30 giây.
        delay = 2
        while not self.stop:
            try:
                self.database.verify_schema(self.cfg)
                system_logger.info("SQL Server sẵn sàng, schema Queue_job hợp lệ")
                return True
            except DB_ERRORS as exc:
                # Nếu là các lỗi như sau IP, thiếu procedure, sai mật khẩu thì raise vì có thử lại cũng không sửa được
                if isinstance(exc, pyodbc.Error) and not isinstance(exc, pyodbc.OperationalError):
                    raise

                # Các lỗi hạ tầng DB (chưa lên, timeout, quá tải) → log + chờ (backoff), KHÔNG crash.
                system_logger.warning("Chưa kết nối được SQL Server (%s); thử lại sau %ds", type(exc).__name__, delay)
                self._sleep(delay)
                # Tăng gấp đôi thời gian chờ, tối đa 30 giây.
                delay = min(delay * 2, 30)
        return False

    def _find_orphans(self):
        """
        Tìm tiến trình con cũ (chạy worker_entry.py của chính thư mục này) còn sống.
        :return: danh sách psutil.Process
        """
        # Lấy đường dẫn tới tệp worker_entry.py
        entry = str(CHILD_ENTRY).lower()
        # Danh sách tiến trình liên quan đến worker_entry.py nhưng pid khác pid hiện tại (mồ côi).
        orphans = []

        # Duyệt tất cả tiến trình, tìm các tiến trình có cmdline chứa entry và pid khác pid hiện tại.
        for process in psutil.process_iter(['pid', 'cmdline']):
            try:
                # cmdline là list[str], nối lại thành chuỗi để tìm kiếm. Nếu tiến trình đã chết → bỏ qua.
                cmdline = ' '.join(process.info['cmdline'] or []).lower()
                # Chỉ lấy các tiến trình con mồ côi của lần chạy trước (khác pid hiện tại) có cmdline chứa worker_entry.py.
                if entry in cmdline and process.pid != os.getpid():
                    orphans.append(process)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        return orphans

    def _kill_orphans(self):
        """
        Khi supervisor bị crash, các tiến trình con (gateway/worker) vẫn hoạt động tiếp tục → mồ côi. Supervisor mới sẽ dừng chúng.
        """
        # Lấy danh sách process mồ côi
        orphans = self._find_orphans()
        for process in orphans:
            system_logger.warning("Dừng tiến trình con mồ côi pid=%d từ lần chạy trước", process.pid)
            try:
                # Dừng tiến trình con mồ côi bằng kill
                process.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        # Chờ tối đa 15 giây để các tiến trình con mồ côi dừng hẳn.
        psutil.wait_procs(orphans, timeout=15)
        return len(orphans)

    def _recover_on_startup(self):
        """
        Khôi phục các job 'running' của lần chạy trước về lại hàng đợi.  
        Tránh trường hợp 1 worker đang chạy job, nếu recover thì job đó sẽ chạy 2 lần  
        (1 lần ở worker cũ, 1 lần ở worker mới). Vì vậy chỉ recover khi chắc chắn không còn process con cũ nào sống (giữ được lock children.alive.lock).
        """
        # Lấy đường dẫn tới lock children.alive.lock để đảm bảo không còn process con cũ nào sống.
        lock_path = self.locks / 'children.alive.lock'
        try:
            # Xin lock độc quyền để đảm bảo không còn process con cũ nào sống
            # Nếu lock đang bị giữ bởi process con cũ → xin lock sẽ timeout → đang có process con cũ sống → không recover.
            lock = portalocker.Lock(lock_path, mode='a', timeout=0)
            lock.acquire()
        except portalocker.exceptions.LockException:
            # Nếu bị giữ lock thì cần dừng các process con mồ côi trước khi recover, vì nếu recover khi process con cũ vẫn sống → job sẽ chạy 2 lần.
            killed = self._kill_orphans()
            system_logger.warning("Đã dừng %d tiến trình con mồ côi; chờ hệ điều hành trả lock", killed)

            # Xin lại lock, chờ tối đa 30 giây. Nếu vẫn lỗi → raise: không thể recover an toàn, cần người kiểm tra.
            lock = portalocker.Lock(lock_path, mode='a', timeout=30)
            lock.acquire()

        try:
            # Xóa file cluster.ready để worker mới không claim job trong lúc supervisor đang recover.
            self.ready_flag.unlink(missing_ok=True)
            delay = 2
            while not self.stop:
                try:
                    # Khôi phục các job dang dở từ lần chạy trước (chỉ recover các job của worker, KHÔNG recover job của gateway).
                    recovered = self.database.recover()
                    if recovered:
                        system_logger.info("Recover các job dang dở từ lần chạy trước: %s", recovered)
                    return
                except DB_ERRORS as exc:
                    system_logger.warning("Recover lúc khởi động lỗi DB (%s); thử lại sau %ds",
                                          type(exc).__name__, delay)
                    self._sleep(delay)
                    delay = min(delay * 2, 30)
        finally:
            lock.release()

    # ------------------------------------------------------------------
    # Spawn / health
    # ------------------------------------------------------------------
    def _child_env(self, child):
        """
        Tạo biến môi trường cho tiến trình con.  
        """
        # Tạo thư mục uploads_tmp để Paddle/Torch lưu tệp tạm, tránh dùng thư mục làm việc hiện tại.
        tmp_dir = str(self.root / 'uploads_tmp')
        # Copy biến môi trường hiện tại và lấy ra biến môi trường VISION_WORKER_ID (nếu có)
        env = os.environ.copy()
        env.pop('VISION_WORKER_ID', None)          # Nếu không lấy ra, gateway sẽ nhận nhầm VISION_WORKER_ID và tưởng mình là worker.
        threads = str(self.cfg['models']['cpu_threads'])

        # Tại đây mới kiểm tra là worker hay gateway để set biến môi trường VISION_WORKER_ID và CUDA_VISIBLE_DEVICES phù hợp
        if child.is_worker:
            spec = child.spec
            env.update(
                VISION_WORKER_ID=spec['id'],
                CUDA_DEVICE_ORDER='PCI_BUS_ID',
                CUDA_VISIBLE_DEVICES='' if spec['device'] == 'cpu' else str(spec['gpu_id']),
            )
        else:
            # Gateway không dùng GPU.
            env['CUDA_VISIBLE_DEVICES'] = ''

        env.update(
            VISION_ROLE=child.identity,
            PYTHONUNBUFFERED='1',
            OMP_NUM_THREADS=threads,
            MKL_NUM_THREADS=threads,
            FLAGS_allocator_strategy='auto_growth',
            FLAGS_fraction_of_gpu_memory_to_use='0.9',
            PADDLE_PDX_CACHE_HOME=self.cfg['models']['paddle_cache_dir'],
            TMPDIR=tmp_dir, TEMP=tmp_dir, TMP=tmp_dir,
        )

        # Đặt PYTHONPATH = src để tiến trình con import được các module trong src. Nếu không set → tiến trình con sẽ import module từ thư mục làm việc hiện tại (có thể là thư mục khác).
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [str(ROOT / 'src'), env.get('PYTHONPATH')]))
        return env

    def _spawn(self, child):
        """
        Khởi chạy 1 tiến trình con. Lỗi spawn được coi như process lỗi (backoff), không crash.
        """
        # Tạo thư mục uploads_tmp và logs nếu chưa có.
        (self.root / 'uploads_tmp').mkdir(exist_ok=True)
        logs_dir = self.root / 'logs'
        logs_dir.mkdir(parents=True, exist_ok=True)

        kwargs = {}
        # Nếu Windows → tạo nhóm process riêng: Ctrl+C trên console chỉ tới supervisor, supervisor tự dừng con có trật tự.
        if os.name == 'nt':
            kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs['start_new_session'] = True

        try:
            # Tạo thư mục logs/<identity> nếu chưa có, mở file console log, spawn tiến trình con.
            child.console = (logs_dir / f'{child.identity}-console.log').open('ab', buffering=0)

            # Spawn tiến trình con bằng cách chạy tệp worker_entry.py với PYTHONPATH = src. Nếu spawn lỗi → log + lên lịch spawn lại (backoff).
            child.process = subprocess.Popen(
                [sys.executable, str(CHILD_ENTRY)],
                cwd=ROOT,
                env=self._child_env(child),
                stdout=child.console,
                stderr=subprocess.STDOUT,
                **kwargs
            )
        except OSError as exc:
            system_logger.error("Không spawn được process %s: %s", child.identity, exc)
            if child.console:
                child.console.close()
            child.console = child.process = None

            # Lên lịch spawn lại (backoff) như process lỗi.
            self._schedule_restart(child, f'spawn_error: {type(exc).__name__}')
            return

        # Cập nhật trạng thái tiến trình con để không dính trạng thái cũ
        now = time.monotonic()
        child.started = now                  # mốc để tính startup_timeout
        child.ready = False
        child.state = 'starting'             # _maybe_spawn dựa vào trạng thái này để chặn worker khác cùng nạp model
        child.health_failures = 0
        child.next_job_check_at = now        # kiểm tra job ngay khi tiến trình ready
        child.rss_warned = False
        system_logger.info("Process con %s (pid %d) đã được spawn", child.identity, child.process.pid)

    def _endpoint(self, child):
        """
        :return: endpoint HTTP của tiến trình con (gateway hoặc worker).
        """
        endpoint = child.spec or self.cfg['server']
        host = '127.0.0.1' if endpoint['host'] == '0.0.0.0' else endpoint['host']
        return f'http://{host}:{endpoint["port"]}'

    def is_healthy(self, child, timeout=1.0):
        """
        Gọi GET /health; True khi process trả 200 và ready=true.
        """
        try:
            # Bỏ qua biến môi trường HTTP_PROXY. Nếu không, trên máy công ty có proxy, request tới localhost có thể bị gửi qua proxy và luôn lỗi.
            response = httpx.get(f'{self._endpoint(child)}/health', timeout=timeout, trust_env=False)
            return response.status_code == 200 and bool(response.json().get('ready', False))
        except (httpx.HTTPError, ValueError):
            return False

    # ------------------------------------------------------------------
    # Watchdog
    # ------------------------------------------------------------------
    def _watchdog_loop(self):
        """
        Vòng lặp chính của supervisor: spawn tiến trình con, kiểm tra sức khỏe, restart khi cần.
        Mỗi tiến trình con có backoff + circuit breaker RIÊNG → một worker hỏng KHÔNG làm dừng gateway hay các worker khác.
        """
        while not self.stop:
            now = time.monotonic()
            # Duyệt tất cả tiến trình con, spawn nếu chưa có process, restart nếu cần.
            for child in self.children:
                if self.stop:
                    break
                try:
                    self._tick(child, now)
                except Exception:  # pylint: disable=broad-except
                    # Lỗi bất ngờ khi giám sát MỘT process không được làm chết supervisor.
                    system_logger.exception("Lỗi khi giám sát process %s", child.identity)

            self._maybe_open_barrier()
            self._write_status(now)
            time.sleep(POLL_SECONDS)

    def _tick(self, child, now):
        """
        Kiểm tra tiến trình con: spawn nếu chưa có process, restart nếu cần.
        """
        # Nếu chưa có process → spawn (nếu đã qua backoff).
        if child.process is None:
            self._maybe_spawn(child, now)
            return

        # Kiểm tra lý do restart (exit code, treo, job timeout, RSS limit, startup timeout).
        reason = self._restart_reason(child, now)
        if reason and not self.stop:
            self._restart(child, reason)

    def _maybe_spawn(self, child, now):
        """
        Spawn tiến trình con nếu chưa có process và đã qua backoff.
        """
        # Nếu tiến trình con là worker và cần recover job dang dở
        if child.is_worker and child.needs_recover:
            # Nếu recover job không thành công thì bỏ qua lượt này, sẽ thử lại lượt sau.
            if not self._recover_worker(child):
                return

        # Nếu chưa tới thời điểm spawn (backoff) → bỏ qua lượt này.
        if now < child.next_spawn_at:
            return

        # Kiểm tra xem có worker nào khác đang khởi động (state=starting) → không spawn worker mới, tránh nạp model đồng thời.
        if child.is_worker and any(other.is_worker and other.state == 'starting'
                                   for other in self.children if other is not child):
            return

        # Nếu circuit breaker đang mở → log cảnh báo, nhưng vẫn spawn lại (vì có thể lỗi tạm thời).
        if child.state == 'circuit_open':
            system_logger.warning("Hết thời gian tạm ngắt process %s; thử khởi động lại", child.identity)

        # tiến hành spawn tiến trình con.
        self._spawn(child)

    def _restart_reason(self, child, now):
        """
        Xác định lý do restart của tiến trình con.
        :return: lý do restart hoặc None.
        """
        # Nếu process con đã chết sẽ trả về mã exit code
        process = child.process
        if process.poll() is not None:
            return f'exit_{process.returncode}'

        queue_cfg, sup_cfg = self.cfg['queue'], self.cfg['supervisor']

        if not child.ready:
            # Kiểm tra startup_timeout và health check lần đầu (ready=false) → nếu process đã sẵn sàng → ready=true.
            if self.is_healthy(child):
                child.ready = True
                child.state = 'ready'
                child.attempted = True
                child.next_health_at = now + sup_cfg['health_interval_seconds']
                system_logger.info("Process %s khởi động thành công sau %.1fs", child.identity, now - child.started)
                return None

            # Nếu thời gian khởi động quá lâu thì báo timeout và khởi động lại
            if now - child.started > queue_cfg['startup_timeout_seconds']:
                return 'startup_timeout'
            return None

        # Health check định kỳ: phát hiện process còn sống nhưng treo (deadlock, event loop bị chặn...).
        if now >= child.next_health_at:
            # thời điểm kiểm tra sức khỏe lần tới = bây giờ + health_interval_seconds
            child.next_health_at = now + sup_cfg['health_interval_seconds']
            if self.is_healthy(child, timeout=sup_cfg['health_timeout_seconds']):
                child.health_failures = 0
            else:
                child.health_failures += 1
                if child.health_failures >= sup_cfg['health_fail_threshold']:
                    return 'unresponsive'

        if not child.is_worker:
            return None

        # Tính toán RSS limit: nếu tiến trình con dùng RAM vượt quá max_worker_rss_mb → restart.
        limit = self.cfg['models'].get('max_worker_rss_mb') or 0
        try:
            if limit and psutil.Process(process.pid).memory_info().rss > limit * 2**20:
                return 'rss_limit'
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            if not child.rss_warned:
                system_logger.warning("Không thể đọc RSS của process %s (pid %d)", child.identity, process.pid)
                child.rss_warned = True

        # 5s một lần kiểm tra job (tránh query DB quá nhiều).
        if now >= child.next_job_check_at:
            child.next_job_check_at = now + sup_cfg['job_check_interval_seconds']
            try:
                job = self.database.running(child.identity)
            except DB_ERRORS as exc:
                system_logger.debug("Bỏ qua kiểm tra job timeout của %s: %s", child.identity, type(exc).__name__)
                return None

            # SQL Server lưu datetime2 UTC dạng naive → so sánh với UTC naive.
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            if job and now_utc - job['started'] > timedelta(seconds=queue_cfg['job_timeout_seconds']):
                system_logger.warning("Job %s trên %s chạy quá %ss", job['id'], child.identity,
                                      queue_cfg['job_timeout_seconds'])
                return 'job_timeout'
        return None

    def _recover_worker(self, child):
        """
        Khôi phục job dang dở của worker (nếu có) trước khi spawn lại.
        :return: True nếu recover thành công hoặc không cần recover, False nếu lỗi DB → thử lại lượt sau.
        """
        try:
            # Khôi phục job dang dở của worker này (nếu có) trước khi spawn lại.
            recovered = self.database.recover(child.identity)
            child.needs_recover = False
            if recovered:
                system_logger.info("Recover job của worker %s: %s", child.identity, recovered)
            return True
        except DB_ERRORS as exc:
            system_logger.warning("Chưa recover được job của %s (%s); sẽ thử lại",
                                  child.identity, type(exc).__name__)
            return False

    def _restart(self, child, reason):
        """
        Dừng hẳn tiến trình con → recover job của nó → lên lịch spawn lại có backoff.
        """
        system_logger.warning("Process %s (pid %d) bị dừng, lý do: %s",
                              child.identity, child.process.pid, reason)
        try:
            # terminate → chờ → kill. Chỉ recover SAU khi chắc chắn process đã chết.
            stop_process(child.process, self.cfg['queue']['shutdown_seconds'])
        except Exception:  # pylint: disable=broad-except
            system_logger.exception("Lỗi khi dừng process %s", child.identity)
        self._close_console(child)

        # Cập nhật trạng thái tiến trình con để không dính trạng thái cũ
        child.process = None
        child.ready = False

        if child.is_worker:
            # Bật cờ need_recover trước, để nếu recover lỗi thì cờ vẫn còn và lần sau sẽ thử recover lại.
            child.needs_recover = True
            self._recover_worker(child)

        # Lên lịch spawn lại tiến trình con với backoff + circuit breaker.
        self._schedule_restart(child, reason)

    def _schedule_restart(self, child: Child, reason: str):
        """
        Lên lịch spawn lại tiến trình con với backoff + circuit breaker.
        """
        # Lấy delay từ policy
        delay, circuit = child.policy.record_failure()
        # Cập nhật mốc thời gian spawn lại, tổng số lần restart, lý do restart, cờ attempted.
        child.next_spawn_at = time.monotonic() + delay
        child.restarts_total += 1
        child.last_reason = reason
        child.attempted = True

        # Nếu đang trong Circuit breaker → log cảnh báo
        if circuit:
            child.state = 'circuit_open'
            system_logger.critical(
                "Process %s lỗi quá %d lần trong %ss → TẠM NGẮT %ds. Các process khác vẫn chạy bình thường.",
                child.identity, child.policy.max_restarts, int(child.policy.window), int(delay))
        else:
            child.state = 'backoff'
            system_logger.warning("Process %s sẽ được khởi động lại sau %.0fs (lỗi gần đây: %d)",
                                  child.identity, delay, child.policy.recent_failures)

    def _maybe_open_barrier(self):
        """
        Mở barrier khi mọi worker đã qua lượt khởi động đầu (ready hoặc lỗi).
        """
        if self.barrier_open:
            return
        # Lấy danh sách worker
        workers = [child for child in self.children if child.is_worker]
        # Nếu tất cả worker đã qua lượt khởi động đầu (ready hoặc lỗi) → mở barrier.
        if all(child.attempted for child in workers):
            self.ready_flag.touch()
            self.barrier_open = True
            # Đếm tổng số worker đã ready để log.
            ready = sum(1 for child in workers if child.ready)
            system_logger.info("Cluster sẵn sàng: %d/%d worker ready; worker bắt đầu claim job", ready, len(workers))

    def _write_status(self, now):
        """
        Ghi trạng thái supervisor ra file supervisor_status.json để endpoint /admin/workers có thể báo cáo trạng thái.
        Ghi tối đa 1 lần / 2 giây.
        """
        # Nếu chưa tới thời điểm ghi trạng thái → bỏ qua lượt này.
        if now < self._next_status_write:
            return
        # Thời gian ghi trạng thái lần tới = bây giờ + 2 giây
        self._next_status_write = now + 2
        try:
            write_json(self.status_file, {
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'supervisor_pid': os.getpid(),
                'barrier_open': self.barrier_open,
                'children': [child.to_status() for child in self.children],
            })
        except OSError:
            pass

    # ------------------------------------------------------------------
    # Dọn dẹp định kỳ & shutdown
    # ------------------------------------------------------------------
    def _cleanup_loop(self):
        """
        Dọn job hết hạn lưu trữ định kỳ (storage.retention_days).
        Chạy trong luồng riêng, KHÔNG làm chết supervisor nếu lỗi.
        """
        # Lấy thời gian dọn dẹp từ cấu hình, tính ra giây. Nếu <=0 → không dọn.
        interval = self.cfg['supervisor']['cleanup_interval_minutes'] * 60
        if interval <= 0:
            return
        self._sleep(60)  # Để cluster khởi động xong trước.
        while not self.stop:
            try:
                count = cleanup(self.cfg, self.database)
                if count:
                    system_logger.info("Retention cleanup: %d job hết hạn (dry_run=%s)",
                                       count, self.cfg['storage']['cleanup_dry_run'])
            except Exception:  # pylint: disable=broad-except
                system_logger.exception("Retention cleanup lỗi; sẽ thử lại ở chu kỳ sau")

            # Chờ interval giây, nhưng vẫn kiểm tra cờ stop để thoát sớm.
            self._sleep(interval)

    @staticmethod
    def _close_console(child):
        """
        Đóng file console log của tiến trình con.
        """
        if child.console:
            try:
                child.console.close()
            except OSError:
                pass
            child.console = None

    def _shutdown(self):
        """
        Đóng barrier, dừng mọi tiến trình con, recover job worker đang chạy để lần sau chạy lại.
        """
        # Xóa file cluster.ready để worker không claim job trong lúc supervisor đang dừng.
        self.ready_flag.unlink(missing_ok=True)
        for child in self.children:
            if child.process is None:
                continue
            system_logger.info("Dừng process %s (pid %d) trước khi supervisor exit", child.identity, child.process.pid)
            try:
                stop_process(child.process, self.cfg['queue']['shutdown_seconds'])
            except Exception:  # pylint: disable=broad-except
                system_logger.exception("Lỗi khi dừng process %s", child.identity)
            self._close_console(child)
            child.process = None
            child.state = 'stopped'
            if child.is_worker:
                self._recover_worker(child)
        self._write_status(float('inf'))
        system_logger.info("Supervisor đã dừng, cluster đã dọn dẹp xong")


def _enable_console_logging():
    """Supervisor in log ra console để người vận hành thấy ngay (file log vẫn giữ nguyên)."""
    handler = logging.StreamHandler()
    handler.setFormatter(system_formatter)
    handler.setLevel(logging.INFO)
    system_logger.addHandler(handler)


def main():
    """
    Chạy supervisor: load config, enable console logging, start luồng dọn dẹp định kỳ, tạo Supervisor và chạy vòng watchdog.
    :return: exit code
    """
    # Enable console logging để người vận hành thấy log ngay trên console, file log vẫn giữ nguyên.
    _enable_console_logging()
    # Luồng xoay log, xóa log cũ. Chạy trong luồng riêng, KHÔNG làm chết supervisor nếu lỗi.
    threading.Thread(target=_rotation_thread, daemon=True).start()
    # Đọc cấu hình từ config/config.py. Nếu lỗi → log + exit code 2.
    try:
        cfg = load_config()
    except (OSError, ValueError, KeyError) as exc:
        system_logger.critical("Cấu hình không hợp lệ: %s", exc)
        return 2

    # Khởi tạo Supervisor
    supervisor = Supervisor(cfg)
    # Cài đặt xử lý Ctrl+C / SIGTERM để supervisor dừng có trật tự.
    supervisor.install_signal_handlers()
    # Chạy vòng watchdog: spawn tiến trình con, kiểm tra sức khỏe, restart khi cần.
    return supervisor.run()


if __name__ == '__main__':
    sys.exit(main())
