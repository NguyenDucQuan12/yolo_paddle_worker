"""
SUPERVISOR – tiến trình cha, nó chịu trách nhiệm:
    1. Recover các job dang dở từ lần chạy trước.
    2. Spawn gateway, rồi spawn lần lượt từng worker và chờ worker ready.
    3. Mở barrier `cluster.ready` để worker bắt đầu claim job.
    4. Watchdog: restart tiến trình con khi chết / job quá hạn / vượt RAM / khởi động quá lâu.
    5. Khi Ctrl+C hoặc SIGTERM: dừng toàn bộ tiến trình con và recover job đang chạy.
"""
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

from config.config import load_config, ROOT
from log.system_log import _rotation_thread, system_logger
from services.database_service import MyDatabase
from utils.processes import stop_process

# File khởi tạo gateway và worker (tiến trình con). Supervisor sẽ chạy file này bằng subprocess.
CHILD_ENTRY = ROOT / 'src' / 'workers' / 'worker_entry.py'
# Khoá của gateway trong self.children (worker dùng id trong config).
GATEWAY_ID = 'gateway'
# Chu kỳ kiểm tra của các vòng lặp chờ, tránh busy-spin CPU.
POLL_SECONDS = 0.5


class Supervisor:
    """
    Quản lý vòng đời gateway và các worker.

    self.children là dict lưu trữ thông tin về các tiến trình con (gateway và worker), có dạng:
        {
            'gateway':  {'process', 'console', 'spec': None, 'started', 'ready', 'restarts'},
            'worker_1': {'process', 'console', 'spec': {...}, 'started', 'ready', 'restarts'},
            ...
        }
    """

    def __init__(self, cfg):
        # lấy cấu hình từ file config và lưu vào self.cfg
        self.cfg = cfg
        # Thư mục gốc lưu trữ các file tạm, lock, barrier, v.v.
        self.root = Path(cfg['storage']['root'])
        self.locks = self.root / 'locks'
        self.locks.mkdir(parents=True, exist_ok=True)

        # Khi file này tồn tại, worker mới được phép claim job (xem Worker.run).
        # Supervisor sẽ tạo file này khi tất cả worker đã sẵn sàng, mở barrier để worker bắt đầu claim job.
        # Worker sẽ chờ file này tồn tại trước khi claim job từ queue.
        # Khi tất cả worker đã claim xong job và hoàn thành, supervisor có thể xoá file này để ngăn worker mới claim job.
        self.ready_flag = self.root / 'cluster.ready'

        # Khởi tạo database để recover job và kiểm tra trạng thái job đang chạy.
        self.database = MyDatabase()
        # Lưu trữ thông tin về các tiến trình con (gateway và worker) để quản lý vòng đời.
        self.children = {}
        # Khi cờ này được bật, supervisor sẽ dừng các tiến trình con và thoát vòng lặp chính.
        self.stop = False

    def install_signal_handlers(self):
        """
        Cài đặt các signal handler để bắt các tín hiệu dừng tiến trình (Ctrl+C / SIGTERM).
        Khi nhận được tín hiệu, supervisor sẽ bật cờ self.stop để dừng các tiến trình con và thoát vòng lặp chính.
        """
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

    def _on_signal(self, _signum, _frame):
        """
        Thiết lập cờ self.stop khi nhận được tín hiệu dừng tiến trình.
        """
        self.stop = True

    def run(self):
        """
        Luồng chính của supervisor.

        Thứ tự thực hiện:
        1. recover: khôi phục các job dang dở từ lần chạy trước.
        2. start cluster: khởi động gateway và các worker.
        3. watchdog: giám sát các tiến trình con, xử lý restart nếu cần.
        4. shutdown: dừng các tiến trình con và dọn dẹp tài nguyên.

        Supervisor luôn chạy, kể cả khi có lỗi xảy ra trong quá trình quản lý cluster.
        """
        # Mở tệp lock để đảm bảo chỉ có một Supervisor đang chạy tại một thời điểm.
        # Nếu đã có Supervisor khác đang giữ lock, portalocker sẽ ném lỗi ngay lập tức do timeout=0.
        with portalocker.Lock(self.locks / 'supervisor.lock', mode='a', timeout=0):
            # Đưa các job 'running' của lần chạy trước về lại hàng đợi trước khi khởi động cluster.
            self._recover_on_startup()
            try:
                # Khởi tạo cluster: spawn gateway và các worker theo cấu hình.
                self._start_cluster()
                # Thực hiện vòng lặp watchdog: giám sát các tiến trình con, xử lý restart nếu cần.
                self._watchdog_loop()
            finally:
                # Dừng các tiến trình con và dọn dẹp tài nguyên khi supervisor kết thúc hoặc gặp lỗi.
                self._shutdown()

    def _recover_on_startup(self):
        """
        Đưa các job 'running' của lần chạy trước về lại hàng đợi.
        """
        # Lock ĐỘC QUYỀN: chỉ lấy được khi không còn tiến trình con cũ nào giữ lock CHIA SẺ
        # (xem worker_entry.run). Nhờ vậy không recover nhầm job mà process mồ côi vẫn đang xử lý.
        with portalocker.Lock(self.locks / 'children.alive.lock', mode='a', timeout=0):
            # Xoá barrier cũ để worker mới không claim job trước khi recover xong.
            self.ready_flag.unlink(missing_ok=True)
            # Khôi phục trạng thái các job 'running' từ lần chạy trước về lại hàng đợi.
            recovered = self.database.recover()
            if recovered:
                system_logger.info("Supervisor khởi động lại và recover các job dang dở: %s", recovered)

    def _start_cluster(self):
        """
        Spawn gateway trước, sau đó spawn TUẦN TỰ từng worker (chờ ready mới spawn worker tiếp theo)
        để các worker không tranh nhau nạp model/cache/GPU cùng lúc.
        """
        # Khởi tạo gateway với spec=None (gateway không có ID worker cụ thể).
        self.spawn(None)

        # Spawn gateway xong, tiếp theo spawn các worker theo cấu hình, tuần tự để tránh tranh tài nguyên.
        # Mỗi worker chỉ được spawn khi worker trước đó đã sẵn sàng (ready), đảm bảo không có worker nào tranh GPU/cache với worker khác.
        for spec in self.cfg['workers']:
            if self.stop:
                break
            self.spawn(spec)
            self._wait_until_ready(self.children[spec['id']])

        # Sau khi spawn tất cả worker, nếu supervisor chưa bị dừng, mở barrier để worker bắt đầu claim job.
        if not self.stop:
            # Mở barrier: từ đây Worker.run mới bắt đầu claim job.
            self.ready_flag.touch()
            system_logger.info("Supervisor đã khởi động xong, cluster đã sẵn sàng, worker có thể claim job từ queue")

    def _child_env(self, spec):
        """
        Tạo biến môi trường cho tiến trình con. Đây là "kênh" duy nhất Supervisor báo cho
        worker_entry.py biết nó là gateway hay worker (qua VISION_WORKER_ID).
        """
        # Tạo thư mục tạm cho upload nếu chưa tồn tại.
        tmp_dir = str(self.root / 'uploads_tmp')
        # Copy biến môi trường mới để cho tiến trình con, không sử dụng chung 1 đối tượng env với tiến trình khác.
        env = os.environ.copy()
        # Xoá ID còn sót nếu Supervisor được khởi động từ shell đã có biến này → gateway bị nhận nhầm là worker.
        env.pop('VISION_WORKER_ID', None)

        if spec:
            # Cập nhật biến môi trường đặc thù cho worker, bao gồm ID worker và cấu hình GPU.
            env.update(
                VISION_WORKER_ID=spec['id'],
                # Đánh số GPU theo bus PCI để gpu_id trong config khớp với nvidia-smi.
                CUDA_DEVICE_ORDER='PCI_BUS_ID',
                # Mỗi worker chỉ "nhìn thấy" GPU của nó; chuỗi rỗng = ẩn hết GPU (chạy CPU).
                CUDA_VISIBLE_DEVICES='' if spec['device'] == 'cpu' else str(spec['gpu_id']),
            )

        # Nếu spec là None, nghĩa là tiến trình con là gateway, không cần thiết lập ID worker hay GPU.
        env.update(
            PYTHONUNBUFFERED='1',                                   # Log ra console file ngay, không bị buffer.
            OMP_NUM_THREADS=str(self.cfg['models']['cpu_threads']),  # Giới hạn luồng CPU mỗi worker,
            MKL_NUM_THREADS=str(self.cfg['models']['cpu_threads']),  # tránh các worker tranh CPU.
            FLAGS_allocator_strategy='auto_growth',                 # Paddle: cấp phát GPU tăng dần thay vì chiếm hết.
            FLAGS_fraction_of_gpu_memory_to_use='0.9',              # Paddle: tối đa 90% VRAM mỗi worker.
            TMPDIR=tmp_dir, TEMP=tmp_dir, TMP=tmp_dir,              # File tạm của upload nằm trong storage.
        )

        # Thêm src/ vào PYTHONPATH để worker_entry.py import được api, config, services, workers...
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [str(ROOT / 'src'), env.get('PYTHONPATH')]))

        # Trả về môi trường đã chuẩn bị cho tiến trình con.
        return env

    def spawn(self, spec, restarts=0):
        """
        Khởi chạy 1 tiến trình con.
        :param spec: spec worker trong config['workers']; None nghĩa là gateway.
        :param restarts: số lần đã restart, dùng để so với queue.max_restarts.
        """
        # Xác định ID của tiến trình con: nếu là worker thì lấy từ spec, nếu là gateway thì dùng GATEWAY_ID.
        identity = spec['id'] if spec else GATEWAY_ID

        # Tạo thư mục tạm cho upload nếu chưa tồn tại.
        (self.root / 'uploads_tmp').mkdir(exist_ok=True)
        # Tạo thư mục logs nếu chưa tồn tại, dùng để lưu stdout/stderr của tiến trình con.
        logs_dir = self.root / 'logs'
        logs_dir.mkdir(parents=True, exist_ok=True)

        # stdout và stderr của tiến trình con sẽ được redirect vào file này, giúp debug khi tiến trình crash hoặc in log.
        console = (logs_dir / f'{identity}-console.log').open('ab', buffering=0)

        # Chạy tệp tin worker_entry.py để khởi tạo tiến trình con
        process = subprocess.Popen(
            [sys.executable, str(CHILD_ENTRY)],
            cwd=ROOT,
            env=self._child_env(spec),
            stdout=console,
            stderr=subprocess.STDOUT,
        )

        # Ghi thông tin tiến trình con vào self.children để quản lý
        self.children[identity] = dict(
            process=process,
            console=console,
            spec=spec,
            started=time.time(),
            ready=False,
            restarts=restarts,
        )
        system_logger.info("Process con %s (pid %d) đã được spawn với endpoint %s",
                           identity, process.pid, spec or self.cfg['server'])

    def is_healthy(self, item):
        """
        Gọi GET /health của tiến trình con để kiểm tra xem tiến trình đã sẵn sàng hay chưa.
        """
        # Lấy địa chỉ endpoint của tiến trình con: nếu là worker thì lấy từ spec, nếu là gateway thì lấy từ config['server'].
        endpoint = item['spec'] or self.cfg['server']
        # 0.0.0.0 chỉ dùng để bind, không gọi tới được → đổi sang loopback.
        address = '127.0.0.1' if endpoint['host'] == '0.0.0.0' else endpoint['host']
        try:
            # trust_env=False: không đi qua proxy hệ thống khi gọi localhost.
            response = httpx.get(f'http://{address}:{endpoint["port"]}/health', timeout=1, trust_env=False)
            return response.status_code == 200 and response.json().get('ready', False)
        except (httpx.HTTPError, ValueError):
            return False

    def _wait_until_ready(self, item):
        """
        Chặn cho đến khi tiến trình con ready (đã nạp model + warmup).
        :raises RuntimeError: tiến trình chết hoặc quá startup_timeout_seconds.
        """
        while not self.stop and not self.is_healthy(item):
            # poll() khác None nghĩa là process đã thoát.
            if (item['process'].poll() is not None
                    or time.time() - item['started'] > self.cfg['queue']['startup_timeout_seconds']):
                identity = item['spec']['id'] if item['spec'] else GATEWAY_ID
                raise RuntimeError(f'Startup failed: {identity}; xem console log')

            # Chờ một khoảng thời gian ngắn trước khi kiểm tra lại trạng thái tiến trình con.
            time.sleep(POLL_SECONDS)

        # Khi tiến trình con đã ready hoặc stop được set, đánh dấu trạng thái ready của tiến trình con.
        item['ready'] = not self.stop

    def _watchdog_loop(self):
        """
        Lặp tới khi có signal dừng: với mỗi tiến trình con, nếu có lý do thì restart.
        """
        while not self.stop:
            # list(...) vì _restart() thay thế phần tử trong self.children khi đang duyệt.
            for identity, item in list(self.children.items()):
                reason = self._restart_reason(identity, item)
                if reason:
                    self._restart(identity, item, reason)

            # Chờ một khoảng thời gian ngắn trước khi lặp lại vòng kiểm tra các tiến trình con.
            time.sleep(POLL_SECONDS)

    def _restart_reason(self, identity, item):
        """
        Kiểm tra lý do cần restart cho tiến trình con.  
        :return: lý do cần restart ('exit_<code>', 'job_timeout', 'rss_limit', 'startup_timeout') hoặc None.
        """
        # Lấy thông tin tiến trình con và spec của nó từ item.
        process, spec = item['process'], item['spec']

        # Nếu tiến trình con đã exit, lý do restart là exit code của nó.
        if process.poll() is not None:
            return f'exit_{process.returncode}'

        # Gateway chỉ restart khi chết; các kiểm tra dưới đây chỉ áp dụng cho worker.
        if not spec:
            return None

        # SQL Server trả datetime2 naive nhưng giá trị lưu là UTC → so sánh với UTC naive.
        job = self.database.running(identity)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

        # Nếu thời gian chạy của job vượt quá giới hạn timeout, lý do restart là 'job_timeout'.
        if job and now_utc - job['started'] > timedelta(seconds=self.cfg['queue']['job_timeout_seconds']):
            return 'job_timeout'

        # Ngân sách RAM mỗi worker (MB); 0 hoặc None = không giới hạn.
        limit = self.cfg['models'].get('max_worker_rss_mb', 8192)
        try:
            # Nếu tiến trình con sử dụng RAM vượt quá giới hạn, lý do restart là 'rss_limit'.
            if limit and psutil.Process(process.pid).memory_info().rss > limit * 2**20:
                return 'rss_limit'
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Chỉ cảnh báo 1 lần cho mỗi process để không spam log mỗi 0.5 giây.
            if not item.get('rss_warning'):
                system_logger.warning("Không thể đọc RSS của process con %s (pid %d)", identity, process.pid)
                item['rss_warning'] = True

        # Worker được spawn lại bởi watchdog không đi qua _wait_until_ready nên kiểm tra ready tại đây.
        if not item['ready']:
            item['ready'] = self.is_healthy(item)
            if not item['ready'] and time.time() - item['started'] > self.cfg['queue']['startup_timeout_seconds']:
                return 'startup_timeout'

        return None

    def _restart(self, identity, item, reason):
        """
        Dừng hẳn tiến trình con → recover job của nó → spawn lại với cùng spec.
        """
        system_logger.info("Process con %s (pid %d) đang dừng vì lý do: %s; số lần restart: %d",
                           identity, item['process'].pid, reason, item['restarts'])
        # terminate → chờ shutdown_seconds → kill nếu cần → wait. Chỉ recover SAU khi chắc chắn process đã chết.
        stop_process(item['process'], self.cfg['queue']['shutdown_seconds'])
        # Đóng console của tiến trình con sau khi đã chắc chắn process đã dừng.
        item['console'].close()

        if item['spec']:
            system_logger.info("Recover job của worker %s: %s", identity, self.database.recover(identity))

        if item['restarts'] >= self.cfg['queue']['max_restarts']:
            raise RuntimeError(f'Restart limit reached: {identity}')

        # Backoff ngắn để không nạp model liên tục khi worker crash lặp lại.
        time.sleep(1)
        self.spawn(item['spec'], item['restarts'] + 1)

    def _shutdown(self):
        """
        Đóng barrier, dừng mọi tiến trình con, recover job worker đang chạy để lần sau chạy lại.
        """
        self.ready_flag.unlink(missing_ok=True)

        for identity, item in self.children.items():
            system_logger.info("Dọn dẹp process con %s (pid %d) trước khi supervisor exit", identity, item['process'].pid)
            stop_process(item['process'], self.cfg['queue']['shutdown_seconds'])
            item['console'].close()
            if item['spec']:
                self.database.recover(identity)

        system_logger.info("Supervisor đã dừng, cluster đã dọn dẹp xong")


def main():
    """
    Điểm vào của chương trình
    """
    # Khởi động luồng tạo log để ghi log định kỳ và quản lý luồng log.
    threading.Thread(target=_rotation_thread, daemon=True).start()

    # Tải cấu hình và khởi tạo supervisor với cấu hình đã load.
    supervisor = Supervisor(load_config())
    # Cài đặt các signal handler để supervisor có thể xử lý tín hiệu dừng.
    supervisor.install_signal_handlers()
    # Chạy vòng lặp chính của supervisor.
    supervisor.run()


if __name__ == '__main__':
    main()
