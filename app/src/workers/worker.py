"""
WORKER – logic xử lý job, KHÔNG biết gì về HTTP hay Supervisor.

Vị trí trong kiến trúc:

    main.py (Supervisor) ──spawn──▶ workers/worker_entry.py ──tạo──▶ Worker (file này)

worker_entry.create_app() tạo Worker trong lifespan và gọi start(); mỗi Worker có 1 thread riêng:
    nạp model (Engine) → chờ cluster.ready → lặp: claim job → engine.process → database.finish
Supervisor theo dõi Worker gián tiếp qua GET /health (trả về snapshot()) và exit code của process:
    exit 70 = lỗi nghiêm trọng khi nạp model/vòng lặp, exit 71 = lỗi bộ nhớ/CUDA khi xử lý job.
"""
import os
import threading
import time
import traceback
from pathlib import Path
import psutil
from engine.engine import Engine, write_json
from utils.resources import gpu_slot, check_disk
from services.database_service import MyDatabase
from log.system_log import system_logger

class Worker:
    """
    Worker chạy vòng lặp claim job từ queue, gọi engine để inference, và báo kết quả về
    """
    def __init__(self, cfg, spec, factory: Engine = None, database: MyDatabase = None):
        """
        Khởi tạo worker với cấu hình, thông tin worker, factory engine và service cơ sở dữ liệu.
        """
        # Khởi tạo các thuộc tính của worker
        # factory=None → dùng Engine thật; test có thể truyền engine giả.
        self.cfg, self.spec, self.factory = cfg, dict(spec), factory or Engine
        # Lưu trữ service cơ sở dữ liệu để claim job và báo kết quả.
        self.database = database
        # Tạo event để báo thread dừng vòng lặp, và state để lưu trạng thái hiện tại của worker.
        self.stop = threading.Event()
        self.state = {
            'worker_id': spec['id'],
            'ready': False,
            'state': 'loading', 
            'job_id': None,
            'device': spec['device'],
            'gpu_id': spec['gpu_id']
        }
        # Thread chạy vòng lặp nhận job và xử lý job
        self.thread = threading.Thread(target=self.run, name='model-thread', daemon=True)

    def start(self):
        """
        Bắt đầu thread inference, không block main thread.  
        Được gọi trong lifespan startup, trước khi server HTTP nhận request.
        """
        # chạy thread inference để bắt đầu vòng lặp claim job và inference.
        self.thread.start()

    def close(self):
        """
        Dừng vòng lặp inference, chờ thread kết thúc.  
        Sau khoảng thời gian `shutdown_seconds`, nếu thread inference chưa dừng, supervisor sẽ phát hiện worker đã dừng và tạo worker mới để thay thế.
        """
        # Báo thread inference dừng vòng lặp
        self.stop.set()
        # Đợi thread dừng, timeout theo cấu hình shutdown_seconds để tránh block quá lâu.
        # Nếu thread không dừng trong thời gian này, supervisor sẽ phát hiện worker đã dừng và tạo worker mới để thay thế.
        self.thread.join(timeout=self.cfg['queue']['shutdown_seconds'])

    def snapshot(self):
        """
        Lấy snapshot trạng thái hiện tại của worker.
        """
        try:
            # Lấy thông tin bộ nhớ RSS của process hiện tại bằng psutil, chuyển đổi sang MB và làm tròn đến 1 chữ số thập phân. RSS (Resident Set Size) là lượng bộ nhớ vật lý mà process đang sử dụng.
            # có nghĩa là bao gồm cả bộ nhớ native của process, không chỉ bộ nhớ GPU. Nếu không thể lấy thông tin (ví dụ process đã chết hoặc quyền truy cập bị từ chối), trả về None.
            memory_info = psutil.Process().memory_info()
            rss = round(memory_info.rss / 2**20, 1)
            vms = round(memory_info.vms / 2**20, 1)
        except (psutil.NoSuchProcess, psutil.AccessDenied):  # Một số container không expose PID namespace đầy đủ.
            rss, vms = None, None

        # Trả về dict trạng thái hiện tại của worker cùng RSS và VMS.
        return dict(
            self.state,
            pid=os.getpid(),
            alive=self.thread.is_alive(),
            rss_mb=rss,
            vms_mb=vms,
        )

    def run(self):
        """
        Vòng lặp claim job từ queue, gọi engine để inference, và báo kết quả về DB.
        """
        # Đánh dấu model đang được nạp, trạng thái ban đầu là 'loading', chưa sẵn sàng nhận job.
        engine :Engine = None
        try:
            # Ghi log sự kiện bắt đầu nạp model, bao gồm ID worker.
            system_logger.info("Bắt đầu nạp model cho worker %s", self.spec['id'])

            # Nạp model và warmup engine, giữ GPU slot nếu worker device là GPU. Nếu worker device là CPU, không cần lock GPU slot.
            with gpu_slot(self.cfg, self.spec, self.stop):
                engine = self.factory(self.cfg, self.spec)

            # Lấy device thực tế mà engine đang sử dụng (có thể là 'cpu' hoặc 'gpu')
            self.spec['device'] = engine.device
            # Cập nhật trạng thái worker là đã sẵn sàng nhận job, trạng thái 'idle', và device thực tế mà engine đang sử dụng.
            self.state.update(ready=True, state='idle', device=engine.device)

            # chạy vòng lặp claim job từ queue, gọi engine để inference, và báo kết quả về DB. Vòng lặp sẽ tiếp tục cho đến khi stop event được set.
            while not self.stop.is_set():
                # Khi mà worker đang chạy trong môi trường được quản lý (_managed_cluster) và file 'cluster.ready' chưa tồn tại trong thư mục locks, worker sẽ chờ supervisor mở barrier trước khi claim job từ queue.
                if self.cfg.get('_managed_cluster') and not (Path(self.cfg['storage']['root'])/'cluster.ready').exists():
                    # cập nhật trạng thái worker là đang chờ cluster, và dừng thread hiện tại trong thời gian poll_seconds để tránh busy wait.
                    self.state['state'] = 'waiting_cluster'
                    self.stop.wait(self.cfg['queue']['poll_seconds'])
                    continue

                # Cập nhật trạng thái worker là đang chờ tài nguyên
                self.state['state'] = 'waiting_resource'
                # thử lấy GPU cho worker vì  có thể worker khác đang giữ slot GPU. Nếu worker device là CPU, không cần lock GPU slot.
                with gpu_slot(self.cfg, self.spec, self.stop):
                    # Kiểm tra stop event trước khi claim job từ queue, nếu stop event được set, thoát vòng lặp.
                    if self.stop.is_set():
                        break


                    # Lấy job từ queue bằng cách gọi self.database.claim với ID worker.
                    job = self.database.claim(self.spec['id'])
                    if job is not None:
                        # tiến hành xử lý công việc
                        self.execute(engine, job)
                # Cập nhật trạng thái worker là đã hoàn tất job, trả GPU slot và nhường CPU cho các worker khác có cơ hội lấy lock GPU slot.
                self.state.update(state='idle', job_id=None)
                self.stop.wait(self.cfg['queue']['poll_seconds'])

        except InterruptedError:
            # Khi stop event được set, vòng lặp inference sẽ bị InterruptedError, worker sẽ dừng vòng lặp và ghi log sự kiện worker_stopping với ID worker.
            system_logger.info("Worker %s đã dừng", self.spec['id'])

        except BaseException: # pylint: disable=broad-except
            # cập nhật trạng thái worker là fatal
            self.state.update(ready=False, state='fatal')
            # ghi log sự kiện worker_fatal với ID worker và stack trace của exception
            system_logger.critical("Worker %s gặp lỗi nghiêm trọng: %s", self.spec['id'], traceback.format_exc())
            # sau đó dừng process hiện tại với exit code 70 để supervisor biết rằng worker đã gặp lỗi nghiêm trọng và cần được khởi động lại.
            os._exit(70)

        finally:
            # Cập nhật trạng thái worker là đã dừng, không nhận job mới nữa. Nếu engine đã được khởi tạo, gọi engine.release() để giải phóng tài nguyên GPU hoặc CPU mà engine đang sử dụng.
            self.state.update(ready=False, state='stopped')
            # if 'engine' in locals():
            #     engine.release()

    def execute(self, engine:Engine, job):
        """
        Thực hiện inference cho một job, gọi engine.process và báo kết quả về store.
        """
        # Cập nhật trạng thái worker là đang chạy job, bao gồm ID job và attempt hiện tại. Health check sẽ biết worker đang làm job nào.
        self.state.update(state='running', job_id=job['id'])

        # Lấy đường dẫn thư mục base cho job, ví dụ: storage_root/jobs/job_id.
        base = Path(self.cfg['storage']['root']) / 'jobs' / job['id']
        # Tạo thư mục base nếu chưa tồn tại, để lưu trữ các artifact của job.
        folder = base / f'attempt_{job["attempt"]:03d}'
        folder.mkdir(parents=True, exist_ok=True)

        # Bắt đầu đo thời gian tổng công việc worker, để tính toán thời gian thực hiện job.
        start = time.perf_counter()
        # Biến đánh dấu nếu job gặp lỗi nghiêm trọng (fatal) hay không, để quyết định có cần recycle worker hay không.
        fatal = False
        try:
            # Kiểm tra dung lượng trống ổ đĩa trước khi inference, nếu không đủ sẽ raise OSError và dừng job.
            check_disk(self.cfg)
            # kiểm tra bộ nhớ hiện tại của process và các thông tin liên quan đến bộ nhớ
            memory = engine.memory_guard()
            memory_snapshot = self.snapshot()
            system_logger.info(
                "Worker %s: Kiểm tra bộ nhớ - RSS: %s MB, VMS: %s MB",
                self.spec['id'],
                memory_snapshot['rss_mb'],
                memory_snapshot['vms_mb'],
            )
            # Ghi log sự kiện job_started với ID job, ID worker, attempt hiện tại, thời gian chờ trong queue, dung lượng bộ nhớ RSS và thông tin bộ nhớ từ engine.memory_guard().
            system_logger.info("Bắt đầu xử lý job %s, attempt %d, worker %s, queue_wait_ms=%d, memory=%s", job['id'], job['attempt'], self.spec['id'], job.get('queue_wait_ms', 0), memory)
            # Truyền job và thư mục lưu trữ artifact cho engine để thực hiện inference, engine sẽ đọc input từ job, thực hiện xử lý và lưu kết quả vào folder.
            engine.process(job, folder)

            # Lấy danh sách artifact của job attempt hiện tại, chỉ publish các file trong attempt này, loại bỏ các file tạm thời (có đuôi .tmp).
            artifacts = self.artifacts(base, folder)
            # Kiểm tra job hoàn thành hay thất bại
            if not self.database.finish(job, artifacts=artifacts):
                raise RuntimeError('Job ownership changed before completion')
            # Ghi log sự kiện job_succeeded với ID job, ID worker, attempt hiện tại, thời gian thực hiện job và số lượng artifact được tạo ra.
            system_logger.info("Hoàn thành job %s, attempt %d, worker %s, elapsed_ms=%d, artifacts=%d", job['id'], job['attempt'], self.spec['id'], round((time.perf_counter()-start)*1000), len(artifacts))

        except Exception as exc: # pylint: disable=broad-except
            # Lấy thông tin lỗi cụ thể
            message = str(exc)
            # Xác định lỗi nghiêm trọng (fatal) nếu là MemoryError hoặc message chứa các từ khóa liên quan đến lỗi bộ nhớ hoặc CUDA.
            fatal = isinstance(exc, MemoryError) or any(s in message.lower() for s in ['out of memory', 'cuda error', 'cudaerror', 'cudnn_status'])

            # ghi log sự kiện job_failed với ID job, ID worker, fatal flag và stack trace của exception. Nếu fatal là True, supervisor sẽ tạo worker mới để thay thế.
            system_logger.error("Job %s, attempt %d, worker %s failed: %s", job['id'], job['attempt'], self.spec['id'], traceback.format_exc())
            try:
                # Ghi thông tin lỗi vào file error.json trong thư mục attempt của job, bao gồm ID job, loại lỗi và thông điệp lỗi. Nếu không thể ghi file do OSError (ví dụ ổ đĩa đầy), bỏ qua lỗi này.
                write_json(folder / 'error.json', {'job_id': job['id'], 'error_type': type(exc).__name__, 'message': 'Processing failed; see server logs'})
            except OSError:
                pass


            # Gọi self.database.finish để báo job đã kết thúc, lưu danh sách artifact và thông tin lỗi. Nếu fatal là True, supervisor sẽ tạo worker mới để thay thế.
            self.database.finish(job, artifacts=self.artifacts(base, folder), error='memory_or_cuda_error' if fatal else type(exc).__name__)

        finally:
            try:
                # Sau job, engine.release bỏ tham chiếu/cache tạm; self.yolo/self.ocr vẫn tồn tại cho job kế tiếp.
                engine.release()
            except Exception: # pylint: disable=broad-except
                fatal = True  # Supervisor sẽ tạo worker sạch.
                system_logger.error("Failed to release engine for job %s: %s", job['id'], traceback.format_exc())

        # khi tồn tại lỗi nghiêm trọng (fatal) trong quá trình inference, worker sẽ dừng ngay lập tức để tránh tiếp tục xử lý các job khác. Supervisor sẽ phát hiện worker đã dừng và tạo worker mới để thay thế.
        if fatal:
            os._exit(71)

    @staticmethod
    def artifacts(base, folder):
        """
        Lấy danh sách artifact của một job attempt, chỉ publish file trong attempt này.
        """
        # Lấy danh sách tất cả file trong folder attempt, loại bỏ các file tạm thời (có đuôi .tmp), và trả về danh sách các đường dẫn tương đối so với thư mục base, chuẩn hóa đường dẫn để sử dụng dấu gạch chéo '/' trên mọi hệ điều hành.
        return sorted(str(p.relative_to(base)).replace('\\', '/') for p in folder.rglob('*') if p.is_file() and not p.name.endswith('.tmp'))

if __name__ == '__main__':
    # Chỉ để debug 1 worker độc lập (không HTTP, không Supervisor, không chờ cluster.ready).
    # Chạy thật: python src/main.py
    from config.config import load_config
    config = load_config()
    Worker(config, config['workers'][0], database=MyDatabase()).run()
