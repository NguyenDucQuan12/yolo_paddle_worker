"""
Lớp truy cập SQL Server
"""
from __future__ import annotations
from datetime import datetime, timezone
import json
import random
import logging
from pathlib import Path, PurePosixPath
from typing import Any, Sequence
import time
import os
import re
import uuid
import pyodbc

# Mở comment 3 dòng bên dưới mỗi khi test (Chạy trực tiếp hàm if __main__)
# import os,sys
# PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(PROJECT_DIR)
from utils.utils import get_odbc_drivers_for_sql_server
from utils.db_concurrency import DatabaseConcurrencyMixin


# Bật cơ chế tái sử dụng kết nối của ODBC.
# Theo tài liệu Microsoft, việc bật pooling sẽ giúp cải thiện hiệu suất khi làm việc với ODBC.
# Và phải thiết lập pyodbc.pooling phải được thực hiện trước kết nối đầu tiên của tiến trình
pyodbc.pooling = True

# Thiết lập logger cho database_service
logger = logging.getLogger(__name__)
UNSET = object()  # Phân biệt: bỏ qua trường với chủ ý gán NULL.


class DatabaseError(RuntimeError):
    """Lỗi cấu hình/dữ liệu; không chứa SQL hay thông tin kết nối."""

class QueueFull(Exception):
    """
    SQL Server xác nhận số job queued + running
    đã đạt giới hạn max_pending.
    """

class JobConflict(Exception):
    """
    Job ID đã tồn tại nhưng metadata của yêu cầu mới
    khác metadata đã lưu.
    """

class EnqueueUncertain(Exception):
    """
    Không xác định được yêu cầu enqueue đã commit hay chưa.

    Caller phải giữ file gốc và tra cứu lại bằng cùng job ID.
    """

    def __init__(self, job_id: str):
        # Giữ ID để caller tra cứu mà không cần phân tích chuỗi lỗi.
        self.job_id = job_id

        super().__init__(f'Chưa xác định được kết quả enqueue của job {job_id}')

# Danh sách stored procedures
_QUEUE_PROCEDURES = frozenset({
    'Settings',
    'Enqueue',
    'Claim',
    'Finish',
    'GetJob',
    'Recover',
    'Running',
    'Expired',
    'DeleteTerminal',
})

_QUEUE_SESSION_SQL = """
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    SET ANSI_NULLS ON;
    SET QUOTED_IDENTIFIER ON;
    SET ANSI_PADDING ON;
    SET ANSI_WARNINGS ON;
    SET ARITHABORT ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET NUMERIC_ROUNDABORT OFF;
"""

def _queue_native_code(exc: pyodbc.Error, number: int) -> bool:
    """
    Tìm native error code trong thông báo ODBC.

    Ví dụ:
    "... Queue full (51002) ..."
    """

    # Một exception ODBC có thể có nhiều phần trong args.
    # Ghép các phần lại để tìm mã lỗi.
    message = ' '.join(map(str, exc.args))

    # Tìm đúng mã nằm trong ngoặc.
    # Ví dụ tìm (1205), không nhầm với số 91205.
    return bool(re.search(rf'\({number}\)', message))


def _queue_job_id(value: str) -> str:
    """
    Chỉ chấp nhận UUID dạng hex gồm 32 ký tự viết thường.
    """

    # Ví dụ hợp lệ:
    # aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    #
    # Ví dụ không hợp lệ:
    # ../../file
    # abc
    # UUID có dấu gạch ngang
    if (not isinstance(value, str) or re.fullmatch(r'[0-9a-f]{32}', value) is None):
        raise ValueError('job_id phải gồm đúng 32 ký tự hex viết thường')

    return value


def _queue_text(value: str | None, maximum: int, field: str, *, nullable: bool = False) -> str | None:
    """
    Kiểm tra chuỗi trước khi truyền vào tham số NVARCHAR.  
    Độ dài được tính theo số đơn vị UTF-16.
    """

    # Một số trường được phép NULL, ví dụ target hoặc error.
    if value is None and nullable:
        return None

    # Không tự chuyển số hoặc đối tượng bất kỳ thành chuỗi.
    if not isinstance(value, str):
        raise ValueError(
            f'{field} phải là chuỗi'
        )

    # NVARCHAR dùng đơn vị UTF-16.
    # Một số ký tự ngoài BMP chiếm hai đơn vị.
    length = len(value.encode('utf-16-le')) // 2

    if length > maximum:
        raise ValueError(
            f'{field} vượt giới hạn {maximum} đơn vị UTF-16'
        )

    return value

def _queue_relative_path(value: str) -> str:
    """
    Chuẩn hóa đường dẫn tương đối dùng trong metadata.

    Không chấp nhận đường dẫn tuyệt đối, ổ đĩa hoặc '..'.
    """

    # Kiểm tra giới hạn tương ứng cột source NVARCHAR(1024).
    value = _queue_text(value, 1024, 'path')

    # Chuẩn hóa dấu phân cách Windows thành '/'.
    #
    # Ví dụ:
    # jobs\\abc\\original.bin
    # thành:
    # jobs/abc/original.bin
    value = value.replace('\\', '/')

    path = PurePosixPath(value)

    if (
        not value
        or '\x00' in value
        or path.is_absolute()
        or ':' in value
        or '..' in path.parts
        or path == PurePosixPath('.')
    ):
        raise ValueError(
            'Đường dẫn phải là đường dẫn tương đối hợp lệ'
        )

    return str(path)

def _odbc_value(value):
    """
    Bao giá trị trong {} và escape dấu } để mật khẩu chứa ; không phá connection string.
    """
    # Bao giá trị trong {} và escape dấu } để mật khẩu chứa ; không phá connection string.
    value = str(value)
    if '\x00' in value:
        logger.error('Giá trị kết nối không hợp lệ: %s', value)
        raise ValueError('Giá trị kết nối không hợp lệ')
    return '{' + value.replace('}', '}}') + '}'

def _positive(value, name, maximum=None):
    """
    Kiểm tra số nguyên dương, có thể giới hạn tối đa.
    """
    if type(value) is not int or value < 1 or (maximum is not None and value > maximum):
        raise ValueError(f'{name} phải là số nguyên dương trong giới hạn cho phép')
    return value

class MyDatabase(DatabaseConcurrencyMixin):
    """
    Lớp quản lý kết nối đến cơ sở dữ liệu SQL Server.  
    Kế thừa DatabaseConcurrencyMixin để quản lý đồng thời nhiều thread, tránh quá tải kết nối.
    """
    def __init__(self, server_name='localhost', database_name="Test", user_name="sa", password="123456789",
                 *, connect_timeout=5, query_timeout=15, trust_server_certificate=True, max_concurrent_connections=4, acquire_timeout=10):
        """
        Khởi tạo kết nối đến cơ sở dữ liệu SQL Server.
        """
        # Khởi tạo concurrency để giới hạn số kết nối đồng thời, tránh quá tải SQL Server.
        self._init_concurrency(max_concurrent_connections, acquire_timeout)

        # Lưu thông tin kết nối và timeout.
        self.database_name = database_name
        self._connect_timeout = _positive(connect_timeout, 'Connect timeout')
        self._query_timeout = _positive(query_timeout, 'Query timeout')
        drivers = get_odbc_drivers_for_sql_server()
        if not drivers:
            raise DatabaseError('Chưa cài ODBC Driver for SQL Server')
        driver = drivers[-1]
        user_name = user_name if user_name is not None else os.getenv('DB_USER')
        password = password if password is not None else os.getenv('DB_PASSWORD')

        # Kiểm tra thông tin đăng nhập
        if user_name:
            if password is None:
                raise DatabaseError('Thiếu mật khẩu SQL Server')
            auth = f'UID={_odbc_value(user_name)};PWD={_odbc_value(password)};'
        else:
            if password is not None:
                raise DatabaseError('Có mật khẩu nhưng thiếu tên đăng nhập SQL Server')
            auth = 'Trusted_Connection=yes;'

        # Tạo chuỗi kết nối ODBC cho SQL Server
        self._connection_string = (
            f'DRIVER={_odbc_value(driver)};'
            f'SERVER={_odbc_value(server_name)};'
            f'DATABASE={_odbc_value(database_name)};'
            f'{auth}Encrypt=yes;'
            f'APP=DucQuanApp.Benchmark;' # Tên ứng dụng để SQL Server log trace hoặc khi benchmark, có thể dùng 'APP=DucQuanApp.Benchmark;'
            f'TrustServerCertificate={"yes" if trust_server_certificate else "no"};'
        )

        # Các biến hàng đợi
        self._queue_ready = False
        self._queue_cfg = {}
        self._queue_deadlock_retries = 3
        self._queue_max_manifest_bytes = 1_048_576  # 1 MB
        self.root = Path('.')

    def _connect(self, *, autocommit=True):
        """
        Kết nối đến cơ sở dữ liệu SQL Server với các tham số đã thiết lập.
        """
        # Kết nối đến cơ sở dữ liệu SQL Server với timeout và autocommit đã thiết lập. Nếu kết nối thất bại, log lỗi và raise exception.
        conn = pyodbc.connect(self._connection_string, timeout=self._connect_timeout,
                               autocommit=autocommit)
        try:
            # Thiết lập timeout cho truy vấn SQL. Nếu truy vấn vượt quá thời gian này, sẽ ném exception.
            conn.timeout = self._query_timeout
            return conn
        except Exception as e: # pylint: disable=broad-except
            logger.error('Lỗi khi kết nối đến cơ sở dữ liệu: %s', str(e))
            conn.close()
            raise

    @staticmethod
    def _close(resource):
        """
        Đóng tài nguyên (cursor hoặc connection) nếu không phải None.
        """
        if resource is not None:
            try:
                resource.close()
            except Exception as e: # pylint: disable=broad-except
                logger.error('Lỗi khi đóng tài nguyên (cursor): %s', str(e))

    def _transaction(self):
        """
        Tạo một transaction mới cho phép thực hiện nhiều truy vấn SQL trong cùng một transaction.  
        Không gọi hàm này cho các stored procedure đã tự quản lý transaction, vì có thể gây lỗi TRANSACTION_MODE.
        """
        return self._cursor(transactional=True)

    @staticmethod
    def _read_sets(cursor: pyodbc.Cursor) -> list[dict[str, Any]]:
        """
        Khi gọi stored procedure có nhiều SELECT, mỗi SELECT sẽ là một result set.  
        Mỗi result set sẽ được chuyển thành một dict với các key là tên cột và value là dữ liệu tương ứng.  
        Ví dụ:  
        ```python
        [
            {
                'columns': ['id', 'name'], 
                'data': [
                    {'id': 1, 'name': 'Alice'},
                    {'id': 2, 'name': 'Bob'},
                ]
            },
            ...
        ]
        ```
        """
        sets = []
        while True:
            # Nếu cursor.description là None, tức là không có result set (ví dụ UPDATE/INSERT), thì bỏ qua và tiếp tục nextset.
            if cursor.description is not None:
                # Lấy tên cột và dữ liệu từ result set hiện tại.
                columns = [item[0] for item in cursor.description]
                rows = [
                    dict(zip(columns, row, strict=True))
                    for row in cursor.fetchall()
                ]
                sets.append({'columns': columns, 'data': rows})

            # Nếu không còn result set nào nữa, thoát vòng lặp. Nếu còn result set, tiếp tục đọc.
            if not cursor.nextset():
                break
        return sets

    @staticmethod
    def _success(success=True, sets=None, message='Thành công.'):
        """
        Trả kết quả theo cấu trúc:  
        ```
        {
            'success': True/False,
            'message': message,
            'data': first_result_set_data,
            'result_sets': all_result_sets
        }
        ```
        """
        sets = [] if sets is None else sets
        # Lấy result set đầu tiên để trả về dữ liệu chính.
        first = sets[0] if sets else {'columns': [], 'data': []}
        return {
                'success': success,
                'message': message,
                'data': first['data'],
                'result_sets': sets
            }

    def _execute_query(self, query, params=None, *, transactional=True, metric=False) -> dict[str, Any]:
        """
        Thực thi một truy vấn SQL với các tham số đã cho.  
        Bật transactional khi nhiều thao tác SQL phải thành công hoặc thất bại cùng nhau.  
        Hoặc bật transactional=False khi gọi stored procedure đã tự quản lý transaction như tự BEGIN/COMMIT/ROLLBACK.  
        Nếu metric=True, đo và in thời gian semaphore, kết nối, thực thi và dọn dẹp.
        """
        if not isinstance(metric, bool):
            raise TypeError('metric phải là bool')

        if metric:
            self.begin_db_measurement()

        try:
            with self._cursor(transactional=transactional) as cursor:
                # Gọi các tham số Set trước khi gọi procedure, ví dụ SET TRANSACTION ISOLATION LEVEL READ COMMITTED.
                cursor.execute(_QUEUE_SESSION_SQL)
                # Thực thi truy vấn SQL với các tham số đã cho (nêu có)
                cursor.execute(query, tuple(params or ()))
                # Đọc tất cả result set từ cursor và chuyển thành list of dicts.
                sets = self._read_sets(cursor)

            # Trả về kết quả thành công với tất cả result set.
            return self._success(success=True, sets=sets, message='Thành công.')

        except Exception as exc: # pylint: disable=broad-except
            logger.error('Lỗi khi thực thi truy vấn SQL: %s', str(exc))
            return self._success(success=False, sets=[], message=str(exc))
        finally:
            if metric:
                # Kết thúc gom metric trong mọi trường hợp, kể cả query lỗi.
                spans = self.end_db_measurement()
                # Một operation có thể mở nhiều cursor lồng nhau nên có thể có nhiều span.
                # có thể ghi log hoặc in ra console để benchmark, ví dụ:
                for span in spans:
                    print(query)
                    print("Chờ semaphore:", span["wait_ms"], "ms")
                    print("Kết nối:", span["connect_ms"], "ms")
                    print("Thực thi:", span["work_ms"], "ms")
                    print("Dọn dẹp:", span["cleanup_ms"], "ms")


    def _check_connection(self):
        """
        Kiểm tra kết nối đến cơ sở dữ liệu bằng cách thực hiện một truy vấn đơn giản.
        """
        return self._execute_query('SELECT 1', transactional=False)['success']

    def initialize_queue(self, cfg: dict[str, Any]) -> None:
        """
        Khởi tạo phần nghiệp vụ hàng đợi trên MyDatabase.

        Gọi một lần khi process khởi động, trước khi chia sẻ đối tượng cho các thread.
        """
        # Nếu đã khởi tạo thành công thì không cho khởi tạo lại. Tránh thay cấu hình trong khi các thread đang hoạt động.
        if getattr(self, '_queue_ready', False):
            raise RuntimeError('Hàng đợi đã được khởi tạo trên MyDatabase này')

        # Lưu cấu hình riêng cho nghiệp vụ queue.
        self._queue_cfg = cfg
        # Lấy cấu hình database từ cfg, nếu không có thì dùng dict rỗng.
        db_cfg = cfg.get('database', {})

        # Số lần thử lại sau lần gọi đầu tiên.
        self._queue_deadlock_retries = int(db_cfg.get('deadlock_retries', 3))

        if not 0 <= self._queue_deadlock_retries <= 5:
            raise ValueError('database.deadlock_retries phải nằm trong khoảng 0..5')

        # Giới hạn kích thước JSON danh sách kết quả.
        self._queue_max_manifest_bytes = int(db_cfg.get('max_manifest_bytes', 1_048_576))

        if self._queue_max_manifest_bytes <= 0:
            raise ValueError('database.max_manifest_bytes phải lớn hơn 0')

        # Thư mục storage
        self.root = Path(cfg['storage']['root'])
        self.root.mkdir(parents=True, exist_ok=True)

        # Kiểm tra kết nối, quyền và cấu hình SQL ngay lúc startup.
        rows = self._queue_call('Settings')

        if not rows or len(rows["data"]) != 1:
            raise RuntimeError('Queue_job.Settings phải trả đúng một dòng cấu hình')

        settings = rows["data"][0]

        if settings['schema_version'] != 1:
            raise RuntimeError('Phiên bản cấu trúc SQL không được hỗ trợ')

        # Cấu hình ở Python và SQL phải thống nhất.
        for key in ('max_pending', 'max_attempts'):
            if cfg['queue'][key] != settings[key]:
                raise ValueError(f'queue.{key} không khớp với SQL QueueSettings')

        # Chỉ đánh dấu sẵn sàng sau khi mọi kiểm tra đều thành công.
        self._queue_ready = True

    def execute_queue_procedure(self, procedure: str, parameters: Sequence[Any] = ()) -> dict[str, Any]:
        """
        Thực hiện một stored procedure trong schema Queue_job với các tham số đã cho.
        """

        # Chỉ nhận tên procedure nội bộ đã cho phép.
        if procedure not in _QUEUE_PROCEDURES:
            raise ValueError(f'Procedure không nằm trong danh sách hỗ trợ: {procedure}')

        # Chuyển tham số thành tuple để bind vào dấu ? trong SQL.
        values = tuple(parameters)

        # Ví dụ 3 tham số -> '?,?,?'.
        placeholders = ','.join('?' for _ in values)

        # Tên schema cố định, tên procedure đã kiểm tra.
        sql = f'EXEC [Queue_job].[{procedure}]'

        # Nếu có tham số, thêm placeholders vào SQL
        if placeholders:
            sql += f' {placeholders}'

        try:
            return self._execute_query(sql, values, transactional=False)
        except Exception as exc: # pylint: disable=broad-except
            logger.exception('Lỗi khi gọi procedure %s: %s', procedure, str(exc))
            return self._success(success=False, sets=[], message=str(exc))

    def _queue_call(self, procedure: str, *parameters: Any) -> dict[str, Any]:
        """
        Gọi procedure và retry có giới hạn khi chắc chắn transaction đã rollback.
        """
        for attempt in range(self._queue_deadlock_retries + 1):
            try:
                return self.execute_queue_procedure(procedure, parameters)
            except pyodbc.Error as exc:

                # Những mã lỗi này chắc chắn đã rollback transaction trong SQL, nên có thể retry.
                # 1205: transaction bị chọn làm deadlock victim.
                # 51001: procedure báo không lấy được applock.
                retryable = _queue_native_code(exc, 1205) or _queue_native_code(exc, 51001)

                if (not retryable or attempt == self._queue_deadlock_retries):
                    raise

                # Chờ tăng dần, thêm jitter để các thread không cùng thử lại đúng một thời điểm.
                delay = (min(0.1 * (2 ** attempt), 1.0) + random.uniform(0, 0.1))

                logger.warning('Thử lại procedure=%s retry=%s delay_seconds=%.3f', procedure, attempt + 1, delay)
                # execute_queue_procedure đã thoát khỏi _cursor. Lúc sleep không còn giữ connection hoặc slot.
                time.sleep(delay)

        raise AssertionError('Không thể đến được đây; vòng lặp for phải return hoặc raise')

    def enqueue(self, job_id: str, task: str, source: str, name: str, target: str | None = None) -> None:
        """
        Thêm công việc vào hàng đợi.
        """
        # Kiểm tra và chuẩn hóa job_id.
        identity = _queue_job_id(job_id)

        # chỉ chấp nhận ba loại task: detect, ocr, analyze.
        if task not in {'detect', 'ocr', 'analyze'}:
            raise ValueError('task phải là detect, ocr hoặc analyze')

        # chuẩn hóa đường dẫn source và target.
        source = _queue_relative_path(source)
        # Chuẩn hóa văn bản cùng với độ dài tối đa cho từng trường. Một số trường có thể NULL.
        name = _queue_text(name, 255, 'original_name')
        target = _queue_text(target, 128, 'target', nullable=True)

        try:
            # Thứ tự tham số phải khớp procedure Enqueue:
            # @id, @task, @source, @name, @target.
            self._queue_call('Enqueue', identity, task, source, name, target)

        except pyodbc.Error as exc:

            if _queue_native_code(exc, 51002):
                raise QueueFull('Hàng đợi đã đầy') from exc

            if _queue_native_code(exc, 51003):
                raise JobConflict('Job ID đã tồn tại với nội dung khác') from exc

            # SQL Server không chắc chắn transaction đã commit hay rollback. Caller phải tra cứu lại bằng cùng job ID.
            raise EnqueueUncertain(identity) from exc

    def claim(self, worker_id: str) -> dict[str, Any] | None:
        """
        Worker nhận tối đa một job.  
        Trả None nếu không có job phù hợp hoặc worker đã có job đang chạy.
        """
        # Kiểm tra và chuẩn hóa worker_id.
        worker_id = _queue_text(worker_id, 128, 'worker_id')

        # Token khác nhau cho mỗi lần gọi nhận việc.
        # Token này sẽ được lưu trong metadata của job để xác nhận khi gọi finish.
        token = str(uuid.uuid4())
        # Gọi procedure Claim với worker_id và token. Nếu không có job phù hợp, trả về None.
        rows = self._queue_call('Claim', worker_id, token)

        if not rows:
            return None

        return rows["data"][0] if rows["data"] else None

    def finish(self, job: dict[str, Any], artifacts: list[str] | None = None, error: str | None = None) -> bool:
        """
        Ghi kết quả của một lần chạy.

        True: SQL chấp nhận kết quả.
        False: kết quả không còn thuộc lần chạy hiện tại.
        """
        # Chuẩn hóa từng đường dẫn kết quả.
        paths = [
            _queue_relative_path(path)
            for path in (artifacts or [])
        ]

        # Chuyển list thành JSON.
        # separators bỏ các khoảng trắng không cần thiết.
        payload = json.dumps(
            paths,
            ensure_ascii=False,
            separators=(',', ':'),
        )

        # Giới hạn kích thước manifest gửi vào database.
        if (len(payload.encode('utf-8')) > self._queue_max_manifest_bytes):
            raise ValueError('Danh sách artifacts vượt giới hạn cấu hình')

        # Chuẩn hóa chuỗi lỗi, có thể NULL.
        error = _queue_text(error, 2048, 'error', nullable=True)

        # Kiểm tra và chuẩn hóa token UUID.
        token = str(uuid.UUID(str(job['claim_token'])))

        # Thứ tự khớp procedure:
        # @id, @owner, @attempt, @token, @artifacts, @error.
        rows = self._queue_call(
            'Finish',
            _queue_job_id(job['id']),
            _queue_text(job['owner'], 128, 'owner'),
            int(job['attempt']),
            token,
            payload,
            error,
        )
        # Lấy giá trị cột 'accepted' từ kết quả trả về. Nếu không có dòng nào, trả về False.
        return bool(rows["data"][0]["accepted"]) if rows["data"] else False

    def get(self, job_id: str) -> dict[str, Any] | None:
        """
        Đọc metadata của job.

        Trả None nếu ID không hợp lệ hoặc không tìm thấy.
        """
        try:
            identity = _queue_job_id(job_id)
        except ValueError:
            return None

        rows = self._queue_call('GetJob', identity)

        if not rows:
            return None

        return rows["data"][0]

    def recover(self, worker_id: str | None = None, *, confirmed_stopped: bool = True) -> list[str]:
        """
        Khôi phục job của worker đã dừng.

        worker_id=None nghĩa là xét tất cả job running.
        """
        # Không để caller vô tình recovery khi tiến trình cũ còn chạy.
        if not confirmed_stopped:
            raise ValueError('Phải xác nhận tiến trình cũ đã kết thúc trước recovery')

        worker_id = _queue_text(worker_id, 128, 'worker_id', nullable=True)

        rows = self._queue_call('Recover', worker_id, True)
        rows = rows["data"] if rows else []

        return [
            row['id']
            for row in rows
        ]

    def running(self, worker_id: str) -> dict[str, Any] | None:
        """
        Lấy ID và thời điểm bắt đầu của job worker đang chạy.
        """
        worker_id = _queue_text(worker_id, 128, 'worker_id')

        rows = self._queue_call('Running', worker_id)

        if not rows:
            return None

        return rows["data"][0] if rows["data"] else None

    def expired(self, cutoff: float) -> list[dict[str, Any]]:
        """
        Tìm job đã kết thúc trước cutoff.

        cutoff là Unix timestamp.
        """
        # Chuyển Unix timestamp sang datetime UTC.
        moment = datetime.fromtimestamp(
            cutoff,
            timezone.utc,
        )

        # SQL datetime2 không lưu timezone.
        # Bỏ tzinfo sau khi đã chuyển đúng về UTC.
        moment = moment.replace(tzinfo=None)

        rows = self._queue_call(
            'Expired',
            moment,
        )
        rows = rows["data"] if rows else []

        return rows

    def delete_terminal(self, identity: str, cutoff: float) -> bool:
        """
        Xóa metadata của job terminal đã hết hạn.

        Không xóa file trên storage.
        """
        identity = _queue_job_id(identity)
        moment = datetime.fromtimestamp(cutoff, timezone.utc).replace(tzinfo=None)
        rows = self._queue_call('DeleteTerminal', identity, moment)
        return bool(rows["data"][0]["deleted"]) if rows["data"] else False

if __name__ == '__main__':
    # Test kết nối và gọi procedure Settings.
    db = MyDatabase()
    # print(db._check_connection())
    # print(db._queue_call('Settings'))
