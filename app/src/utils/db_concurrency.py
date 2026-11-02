"""
Giới hạn và đo thời gian dùng database, không chứa SQL hay tham số kết nối.
Mỗi đối tượng `MyDatabase` nên dùng một instance mixin duy nhất cho toàn bộ
các màn hình/worker trong process. Mixin này không tự mở connection; lớp con
phải cung cấp `_connect(autocommit=...)`.
"""
from contextlib import contextmanager
import logging
import math
import threading
import time

logger = logging.getLogger(__name__)


class DatabaseCapacityTimeout(RuntimeError):
    """
    Chờ semaphore quá lâu; chưa gửi truy vấn tới SQL Server.
    Đây là lỗi hết chỗ trong lớp giới hạn cục bộ, không phải lỗi SQL Server.
    """

class DatabaseReentrantCall(RuntimeError):
    """
    Không được lấy connection lồng nhau trong cùng luồng của cùng db.
    Nếu đã có ``with db._cursor()`` thì phải truyền cursor đó xuống hàm con,
    thay vì gọi thêm một thao tác database trên cùng thread.
    """

class DatabaseConcurrencyMixin:
    """
    Cung cấp semaphore và context manager dùng connection an toàn.
    Semaphore nằm trên instance, nên ``maximum=6`` có nghĩa là tối đa sáu
    context ``_cursor`` của *instance đó* cùng giữ connection. Tạo nhiều
    instance sẽ tạo nhiều semaphore độc lập và tổng giới hạn sẽ tăng theo.
    """

    def _init_concurrency(self, maximum=6, acquire_timeout=3):
        """
        Khởi tạo semaphore để giới hạn số kết nối đồng thời và đo thời gian chờ.
        """
        # Dùng type(...) thay vì isinstance(...) để không chấp nhận bool, vì bool là lớp con của int nhưng không phải cấu hình hợp lệ ở đây.
        if type(maximum) is not int or maximum < 1:
            raise ValueError('Số kết nối đồng thời tối đa phải là số nguyên dương > 0')

        # Timeout phải là số dương hữu hạn; bool cũng bị loại vì True/False không diễn đạt rõ một khoảng thời gian chờ.
        if isinstance(acquire_timeout, bool) or not isinstance(acquire_timeout, (int, float)) or not math.isfinite(acquire_timeout) or acquire_timeout <= 0:
            raise ValueError('Thời gian chờ tối đa để acquire semaphore phải là số dương hữu hạn > 0')

        # Là số kết nối đồng thời tối đa mà instance này có thể cấp, mặc định 6.
        # Để đảm bảo cả chương trình chỉ sử dụng tối đa 6 connection, chỉ khai báo một instance duy nhất của lớp MyDatabase và truyền instance đó cho tất cả màn hình/worker cần dùng Database
        self.max_concurrent_connections = maximum
        # Thời gian chờ để có thể nhận một slot truy cập database, mặc định 3 giây. Nếu hết thời gian chờ, sẽ ném DatabaseCapacityTimeout.
        self._acquire_timeout = acquire_timeout

        # Mỗi token đại diện cho một lượt được phép giữ connection. Semaphore không mở sẵn connection; nó chỉ chặn trước khi _connect được gọi.
        self._db_slots = threading.BoundedSemaphore(maximum)

        # Dữ liệu đo của mỗi thread độc lập, tránh thread này đọc/xóa số đo của thread khác khi benchmark chạy song song.
        self._metric_local = threading.local()

    def begin_db_measurement(self):
        """
        Bắt đầu gom metric cho thao tác database hiện tại của thread.  
        Cách dùng: gọi ``begin`` trước operation và ``end`` trong ``finally``.  
        Ví dụ: 
        ```python
        def load_user(db, user_id):
            # Bắt đầu gom metric cho thao tác database hiện tại của thread
            db.begin_db_measurement()
            try:
                with db._cursor() as cursor:
                    cursor.execute(
                        "SELECT UserId, Email FROM Users WHERE UserId = ?",
                        user_id,
                    )
                    rows = cursor.fetchall()

                return rows
            except Exception as e:
                logger.error("Lỗi khi load user %s: %s", user_id, e)
                raise
            finally:
                # Kết thúc gom metric cho thao tác database hiện tại của thread
                spans = db.end_db_measurement()
                # In ra metric cho từng cursor được cấp trong operation. Nếu operation mở nhiều cursor, sẽ có nhiều span.
                for span in spans:
                    print("Chờ semaphore:", span["wait_ms"], "ms")
                    print("Kết nối:", span["connect_ms"], "ms")
                    print("Thực thi:", span["work_ms"], "ms")
                    print("Dọn dẹp:", span["cleanup_ms"], "ms")
        ```
        """
        # Nếu thread này đã gọi begin nhưng chưa gọi end, thì không được gọi begin lần nữa. Điều này tránh việc gom metric lồng nhau và làm rối danh sách spans.
        if getattr(self._metric_local, 'spans', None) is not None:
            raise RuntimeError('Một phép đo đang mở trên luồng này')
        self._metric_local.spans = []

    def end_db_measurement(self):
        """
        Kết thúc gom metric cho thao tác database hiện tại của thread.  
        Trả về danh sách metric cho từng cursor được cấp trong operation.  
        Nếu operation mở nhiều cursor, sẽ có nhiều span.  
        Ví dụ:
        ```python
        def load_user(db, user_id):
            # Bắt đầu gom metric cho thao tác database hiện tại của thread
            db.begin_db_measurement()
            try:
                with db._cursor() as cursor:
                    cursor.execute("SELECT 1")

                with db._cursor() as cursor:
                    cursor.execute("SELECT 2")
            except Exception as e:
                logger.error("Lỗi khi load user %s: %s", user_id, e)
                raise
            finally:
                # Kết thúc gom metric cho thao tác database hiện tại của thread
                spans = db.end_db_measurement()
                # In ra metric cho từng cursor được cấp trong operation. Nếu operation mở nhiều cursor, sẽ có nhiều span.
                for span in spans:
                    print("Chờ semaphore:", span["wait_ms"], "ms")
                    print("Kết nối:", span["connect_ms"], "ms")
                    print("Thực thi:", span["work_ms"], "ms")
                    print("Dọn dẹp:", span["cleanup_ms"], "ms")
        ```
        """
        spans = getattr(self._metric_local, 'spans', None)
        self._metric_local.spans = None
        return spans or []

    @contextmanager
    def _cursor(self, *, transactional=False):
        """
        cấp cursor từ connection, đảm bảo commit/rollback và close connection/cursor, đồng thời đo metric.  
        Trình tự chính là:  
        ```python
        acquire semaphore -> connect -> execute/fetch ->
        commit hoặc rollback -> close cursor/connection -> release semaphore.
        ```
        Vì semaphore chỉ giới hạn instance hiện tại, toàn bộ nơi gọi `_cursor`
        phải dùng chung một instance `MyDatabase` nếu muốn có một giới hạn
        chung. Ta khai báo 1 `MyDatabase` rồi truyền instance này cho những nơi khác.

        `transactional=False` dùng autocommit, phù hợp với procedure tự
        quản lý transaction. `transactional=True` dành cho SQL phụ cần
        transaction do Python quản lý.
        """
        # Cùng thread không được giữ hai cursor của cùng database. Nếu cần nhiều câu SQL trong một transaction, dùng lại cursor đã cấp.
        if getattr(self._metric_local, 'in_cursor', False):
            raise DatabaseReentrantCall('Không mở _cursor lồng nhau; dùng cursor đã có')

        # Tạo metric trước khi acquire để cả thời gian chờ semaphore cũng được đo. Các giá trị None nghĩa là giai đoạn tương ứng chưa bắt đầu.
        started = time.perf_counter()
        # Metric mặc định, sẽ được cập nhật trong try/finally. Nếu caller không bật phép đo, metric này sẽ bị bỏ qua.
        # wait_ms: thời gian chờ semaphore;
        # connect_ms: thời gian gọi _connect;
        # work_ms: thời gian execute/fetch/commit;
        # cleanup_ms: thời gian close cursor/connection;
        # lease_ms: tổng thời gian giữ slot (từ acquire đến release);
        # acquired: có lấy được slot hay không;
        # error_type: loại lỗi nếu có exception;
        # cleanup_failed: True nếu close cursor/connection ném exception.
        # Các metric này sẽ được cập nhật trong try/finally.
        metric = dict(wait_ms=0.0, connect_ms=None, work_ms=None,
                      cleanup_ms=None, lease_ms=None, acquired=False,
                      error_type='', cleanup_failed=False)

        # Các biến này được khởi tạo trước try/finally để finally luôn có thể đo thời gian và dọn dẹp, kể cả khi connect hoặc execute/fetch ném exception.
        conn = cursor = None
        acquired = False
        lease_start = work_start = cleanup_start = None

        try:
            # Chờ tối đa _acquire_timeout để nhận một slot. Chưa acquire được cursor thì chưa gọi _connect và chưa gửi bất kỳ SQL nào.
            acquired = self._db_slots.acquire(timeout=self._acquire_timeout)
            # Bắt đầu đo thời gian chờ semaphore. Nếu không acquire được slot, sẽ ném exception.
            metric['wait_ms'] = (time.perf_counter() - started) * 1000
            metric['acquired'] = acquired
            if not acquired:
                raise DatabaseCapacityTimeout(f'Hiện đã có {self.max_concurrent_connections} cursor đang giữ connection; không thể cấp thêm trong {self._acquire_timeout} giây')

            # Đánh dấu thread đang giữ cursor trước khi gọi _connect, để mọi lời gọi lồng nhau phát hiện được trạng thái này.
            self._metric_local.in_cursor = True
            # Đo thời gian connect, kể cả khi _connect ném exception. Nếu connect thất bại, sẽ ném exception và không có cursor nào được cấp.
            lease_start = time.perf_counter()
            try:
                # Connection chỉ được tạo sau khi đã giữ slot, nên số connection đang hoạt động không vượt quá maximum.
                conn = self._connect(autocommit=not transactional)
            finally:
                # Đo cả trường hợp connect thất bại để phân biệt lỗi mạng với thời gian chờ slot.
                metric['connect_ms'] = (time.perf_counter() - lease_start) * 1000

            # Đo thời gian thực hiện SQL và commit/rollback. Nếu execute/fetch ném exception, sẽ ném exception và không commit.
            work_start = time.perf_counter()
            # Lấy cursor từ connection. Nếu connection ném exception, sẽ ném exception và không có cursor nào được cấp.
            cursor = conn.cursor()

            # Nếu transactional, bật XACT_ABORT để SQL Server tự rollback transaction khi gặp lỗi runtime, và NOCOUNT để tránh result phụ không cần.
            if transactional:
                cursor.execute('SET XACT_ABORT ON; SET NOCOUNT ON; IF @@TRANCOUNT=0 BEGIN TRANSACTION;')

            # Trả cursor cho caller. Caller phải dùng ``with db._cursor() as cursor:`` để đảm bảo cursor được close và slot được release, kể cả khi execute/fetch ném exception.
            yield cursor

            # Nếu transactional, commit transaction sau khi caller thoát khỏi yield mà không có lỗi. Nếu caller ném exception, sẽ vào except và rollback.
            if transactional:
                conn.commit()
        except BaseException as exc:
            # Ghi lại loại lỗi rồi ném lại lỗi gốc cho lớp nghiệp vụ xử lý.
            metric['error_type'] = type(exc).__name__
            if conn is not None and transactional:
                try:
                    # Rollback cả lỗi trong execute/fetch lẫn lỗi commit.
                    conn.rollback()
                except Exception: # pylint: disable=broad-except
                    # Không che lỗi gốc; chỉ đánh dấu cleanup không hoàn chỉnh.
                    metric['cleanup_failed'] = True
                    logger.warning('Không rollback được transaction sau lỗi %s', type(exc).__name__)
            raise
        finally:
            # finally đảm bảo cleanup và trả slot kể cả khi connect, SQL, commit hoặc rollback ném exception.
            # Đo thời gian cleanup, kể cả khi close ném exception. Nếu caller không bật phép đo, metric này sẽ bị bỏ qua.
            cleanup_start = time.perf_counter()
            if work_start is not None:
                metric['work_ms'] = (cleanup_start - work_start) * 1000

            try:
                # Cố đóng cả cursor và connection. Nếu một close lỗi, vẫn thử tài nguyên còn lại để tránh rò rỉ connection.
                for resource in (cursor, conn):
                    if resource is not None:
                        try:
                            resource.close()
                        except Exception as e: # pylint: disable=broad-except
                            metric['cleanup_failed'] = True
                            logger.warning('Không close được %s: %s', type(resource).__name__, e)
            finally:
                # Đo thời gian cleanup và trả slot, kể cả khi close ném exception. Nếu caller không bật phép đo, metric này sẽ bị bỏ qua.
                ended = time.perf_counter()
                metric['cleanup_ms'] = (ended - cleanup_start) * 1000
                if acquired:
                    # lease_ms là toàn bộ thời gian giữ slot, gồm connect, làm việc và cleanup; sau đó mới trả slot cho thread khác.
                    metric['lease_ms'] = (ended - lease_start) * 1000
                    self._metric_local.in_cursor = False
                    self._db_slots.release()

                # Chỉ lưu metric nếu caller đã bật phép đo. Không bật đo thì operation vẫn hoạt động nhưng không tạo danh sách vô hạn.
                spans = getattr(self._metric_local, 'spans', None)
                if spans is not None:
                    spans.append(metric)
