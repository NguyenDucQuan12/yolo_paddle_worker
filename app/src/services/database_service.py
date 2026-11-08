"""
Lớp truy cập SQL Server cho hàng đợi job (schema [Queue_job], xem README).

Nguyên tắc xử lý lỗi:
    - KHÔNG nuốt lỗi: mọi lỗi SQL được ném lên dưới dạng exception có kiểu rõ ràng để caller
      quyết định (API trả 503, worker chờ rồi thử lại, supervisor bỏ qua vòng kiểm tra...).
    - Lỗi tạm thời (mất kết nối, timeout, deadlock, không lấy được applock) được retry có giới hạn
      với backoff + jitter, chỉ với các procedure an toàn khi gửi lại.
    - Hết lượt retry mà vẫn lỗi tạm thời → DatabaseUnavailable.
"""
from __future__ import annotations

import json
import logging
import random
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Sequence

import pyodbc

# Mở comment 3 dòng bên dưới mỗi khi test (Chạy trực tiếp hàm if __main__)
import os,sys
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_DIR)
from utils.db_concurrency import DatabaseCapacityTimeout, DatabaseConcurrencyMixin
from utils.utils import get_odbc_drivers_for_sql_server

# Bật cơ chế tái sử dụng kết nối của ODBC. Phải đặt trước kết nối đầu tiên của tiến trình.
pyodbc.pooling = True

logger = logging.getLogger(__name__)


class DatabaseError(RuntimeError):
    """Lỗi cấu hình/dữ liệu; không chứa SQL hay thông tin kết nối."""


class DatabaseUnavailable(DatabaseError):
    """
    SQL Server tạm thời không phục vụ được (mất kết nối, timeout, quá tải...).
    Caller nên thử lại sau; API trả 503 + Retry-After.
    """


class QueueFull(Exception):
    """SQL Server xác nhận số job queued + running đã đạt giới hạn max_pending."""


class JobConflict(Exception):
    """Job ID đã tồn tại nhưng metadata của yêu cầu mới khác metadata đã lưu."""


class EnqueueUncertain(Exception):
    """
    Không xác định được yêu cầu enqueue đã commit hay chưa.
    Caller phải giữ file gốc và tra cứu lại bằng cùng job ID.
    """

    def __init__(self, job_id: str):
        self.job_id = job_id
        super().__init__(f'Chưa xác định được kết quả enqueue của job {job_id}')


class _ConnectFailed(Exception):
    """Lỗi ở bước mở kết nối: chắc chắn CHƯA gửi câu lệnh nào tới SQL Server."""


# Danh sách stored procedures được phép gọi.
_QUEUE_PROCEDURES = frozenset({
    'Settings', 'Enqueue', 'Claim', 'Finish', 'GetJob',
    'Recover', 'Running', 'Expired', 'DeleteTerminal',
})

# Các procedure an toàn khi gửi lại sau lỗi kết nối "không chắc chắn":
# - đọc thuần (Settings, GetJob, Running, Expired),
# - idempotent theo thiết kế (Enqueue cùng id+metadata, Finish cùng token+payload, Recover, DeleteTerminal).
# Claim KHÔNG nằm trong danh sách: gửi lại có thể làm mất dấu job đã claim nhưng mất ACK
# (worker tự hoà giải bằng recover() – xem Worker.run).
_RESEND_SAFE = frozenset({
    'Settings', 'Enqueue', 'Finish', 'GetJob',
    'Recover', 'Running', 'Expired', 'DeleteTerminal',
})

# SQLSTATE của lỗi kết nối/timeout có thể tự hết.
_TRANSIENT_SQLSTATES = frozenset({'08001', '08S01', '08004', '08007', 'HYT00', 'HYT01', '40001'})

# Mã lỗi nghiệp vụ do procedure THROW (xem README) – không bao giờ retry.
_BUSINESS_CODES = (51002, 51003, 51004, 51005, 51006, 51007, 51011)

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


def _queue_native_code(exc: BaseException, number: int) -> bool:
    """
    Tìm native error code trong thông báo ODBC, ví dụ "... Queue full (51002) ...".
    """
    message = ' '.join(map(str, getattr(exc, 'args', ())))
    return bool(re.search(rf'\({number}\)', message))


def _sqlstate(exc: BaseException) -> str:
    args = getattr(exc, 'args', ())
    return str(args[0]) if args else ''


def _is_rolled_back_retryable(exc: BaseException) -> bool:
    """Deadlock victim (1205) hoặc không lấy được applock (51001): transaction chắc chắn đã rollback."""
    return _queue_native_code(exc, 1205) or _queue_native_code(exc, 51001)


def _is_business_error(exc: BaseException) -> bool:
    return any(_queue_native_code(exc, code) for code in _BUSINESS_CODES)


def _is_transient(exc: BaseException) -> bool:
    """Lỗi có thể tự hết nếu thử lại sau."""
    if isinstance(exc, (_ConnectFailed, DatabaseCapacityTimeout)):
        return True
    if not isinstance(exc, pyodbc.Error) or _is_business_error(exc):
        return False
    return (isinstance(exc, pyodbc.OperationalError)
            or _sqlstate(exc) in _TRANSIENT_SQLSTATES
            or _is_rolled_back_retryable(exc))


def _definitely_not_sent(exc: BaseException) -> bool:
    """Lỗi xảy ra TRƯỚC khi câu lệnh tới SQL Server → chắc chắn chưa commit gì."""
    return isinstance(exc, (_ConnectFailed, DatabaseCapacityTimeout))


def _queue_job_id(value: str) -> str:
    """Chỉ chấp nhận UUID dạng hex gồm 32 ký tự viết thường."""
    if not isinstance(value, str) or re.fullmatch(r'[0-9a-f]{32}', value) is None:
        raise ValueError('job_id phải gồm đúng 32 ký tự hex viết thường')
    return value


def _queue_text(value: str | None, maximum: int, field: str, *, nullable: bool = False) -> str | None:
    """
    Kiểm tra chuỗi trước khi truyền vào tham số NVARCHAR. Độ dài tính theo đơn vị UTF-16.
    """
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise ValueError(f'{field} phải là chuỗi')
    if len(value.encode('utf-16-le')) // 2 > maximum:
        raise ValueError(f'{field} vượt giới hạn {maximum} đơn vị UTF-16')
    return value


def _queue_relative_path(value: str) -> str:
    """
    Chuẩn hóa đường dẫn tương đối dùng trong metadata. Không chấp nhận tuyệt đối, ổ đĩa hoặc '..'.
    """
    value = _queue_text(value, 1024, 'path').replace('\\', '/')
    path = PurePosixPath(value)
    if (not value or '\x00' in value or path.is_absolute() or ':' in value
            or '..' in path.parts or path == PurePosixPath('.')):
        raise ValueError('Đường dẫn phải là đường dẫn tương đối hợp lệ')
    return str(path)


def _odbc_value(value):
    """Bao giá trị trong {} và escape dấu } để mật khẩu chứa ; không phá connection string."""
    value = str(value)
    if '\x00' in value:
        raise ValueError('Giá trị kết nối không hợp lệ')
    return '{' + value.replace('}', '}}') + '}'


def _positive(value, name, maximum=None):
    """Kiểm tra số nguyên dương, có thể giới hạn tối đa."""
    if type(value) is not int or value < 1 or (maximum is not None and value > maximum):
        raise ValueError(f'{name} phải là số nguyên dương trong giới hạn cho phép')
    return value


class MyDatabase(DatabaseConcurrencyMixin):
    """
    Lớp quản lý kết nối đến SQL Server.
    Kế thừa DatabaseConcurrencyMixin để giới hạn số kết nối đồng thời của process.
    Mỗi process chỉ nên tạo MỘT instance và chia sẻ cho mọi thread.
    """

    def __init__(self, server_name: str, database_name: str, user_name: str | None = None,
                 password: str | None = None, *, connect_timeout=5, query_timeout=15,
                 encrypt=True, trust_server_certificate=True, max_concurrent_connections=4,
                 acquire_timeout=10, app_name='VisionQueue'):
        self._init_concurrency(max_concurrent_connections, acquire_timeout)

        if not server_name or not database_name:
            raise DatabaseError('Thiếu tên server hoặc database SQL Server')

        self.database_name = database_name
        self._connect_timeout = _positive(connect_timeout, 'Connect timeout')
        self._query_timeout = _positive(query_timeout, 'Query timeout')

        drivers = get_odbc_drivers_for_sql_server()
        if not drivers:
            raise DatabaseError('Chưa cài ODBC Driver for SQL Server')
        driver = drivers[-1]

        if user_name:
            if password is None:
                raise DatabaseError('Thiếu mật khẩu SQL Server (đặt VISION_DB_PASSWORD trong app/.env)')
            auth = f'UID={_odbc_value(user_name)};PWD={_odbc_value(password)};'
        else:
            if password is not None:
                raise DatabaseError('Có mật khẩu nhưng thiếu tên đăng nhập SQL Server')
            auth = 'Trusted_Connection=yes;'

        self._connection_string = (
            f'DRIVER={_odbc_value(driver)};'
            f'SERVER={_odbc_value(server_name)};'
            f'DATABASE={_odbc_value(database_name)};'
            f'{auth}'
            f'Encrypt={"yes" if encrypt else "no"};'
            f'APP={_odbc_value(app_name)};'
            f'TrustServerCertificate={"yes" if trust_server_certificate else "no"};'
        )

        self._queue_deadlock_retries = 3
        self._queue_max_manifest_bytes = 1_048_576

    # ------------------------------------------------------------------
    # Khởi tạo
    # ------------------------------------------------------------------
    @classmethod
    def from_config(cls, cfg: dict[str, Any]) -> 'MyDatabase':
        """Tạo instance từ section `database` của config đã load."""
        db = cfg['database']
        instance = cls(
            db['server'], db['name'], db.get('user'), db.get('password'),
            connect_timeout=int(db['connect_timeout']),
            query_timeout=int(db['query_timeout']),
            encrypt=bool(db.get('encrypt', True)),
            trust_server_certificate=bool(db.get('trust_server_certificate', True)),
            max_concurrent_connections=int(db['max_connections']),
            acquire_timeout=db['acquire_timeout'],
            app_name=db.get('app_name') or 'VisionQueue',
        )
        instance.configure(cfg)
        return instance

    def configure(self, cfg: dict[str, Any]) -> None:
        """Đặt tham số retry/giới hạn manifest từ config. Không chạm tới SQL Server."""
        db_cfg = cfg.get('database', {})
        retries = int(db_cfg.get('deadlock_retries', 3))
        if not 0 <= retries <= 5:
            raise ValueError('database.deadlock_retries phải nằm trong khoảng 0..5')
        manifest = int(db_cfg.get('max_manifest_bytes', 1_048_576))
        if manifest <= 0:
            raise ValueError('database.max_manifest_bytes phải lớn hơn 0')
        self._queue_deadlock_retries = retries
        self._queue_max_manifest_bytes = manifest

    def verify_schema(self, cfg: dict[str, Any]) -> None:
        """
        Kiểm tra schema SQL và cấu hình queue khớp với Python. Gọi một lần lúc supervisor khởi động.
        :raises DatabaseUnavailable: chưa kết nối được (caller nên chờ rồi thử lại).
        :raises RuntimeError/ValueError: sai schema/cấu hình (lỗi triển khai, cần sửa tay).
        """
        rows = self._queue_call('Settings')
        if len(rows['data']) != 1:
            raise RuntimeError('Queue_job.Settings phải trả đúng một dòng cấu hình')
        settings = rows['data'][0]
        if settings['schema_version'] != 1:
            raise RuntimeError('Phiên bản cấu trúc SQL không được hỗ trợ')
        for key in ('max_pending', 'max_attempts'):
            if cfg['queue'][key] != settings[key]:
                raise ValueError(
                    f'queue.{key}={cfg["queue"][key]} không khớp với SQL QueueSettings ({settings[key]})')

    # ------------------------------------------------------------------
    # Tầng kết nối
    # ------------------------------------------------------------------
    def _connect(self, *, autocommit=True):
        """Mở kết nối; lỗi ở bước này được bọc thành _ConnectFailed (chắc chắn chưa gửi SQL)."""
        try:
            conn = pyodbc.connect(self._connection_string, timeout=self._connect_timeout,
                                  autocommit=autocommit)
        except pyodbc.Error as exc:
            raise _ConnectFailed(_sqlstate(exc) or type(exc).__name__) from exc
        try:
            conn.timeout = self._query_timeout
            return conn
        except Exception:
            conn.close()
            raise

    @staticmethod
    def _read_sets(cursor: pyodbc.Cursor) -> list[dict[str, Any]]:
        """Đọc mọi result set của cursor thành list {'columns': [...], 'data': [dict, ...]}."""
        sets = []
        while True:
            if cursor.description is not None:
                columns = [item[0] for item in cursor.description]
                rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
                sets.append({'columns': columns, 'data': rows})
            if not cursor.nextset():
                break
        return sets

    @staticmethod
    def _result(sets=None) -> dict[str, Any]:
        sets = sets or []
        first = sets[0] if sets else {'columns': [], 'data': []}
        return {'success': True, 'message': 'Thành công.', 'data': first['data'], 'result_sets': sets}

    def _execute_query(self, query, params=None, *, transactional=True) -> dict[str, Any]:
        """
        Thực thi một truy vấn SQL. Lỗi được NÉM LÊN (không trả success=False âm thầm).
        transactional=False khi gọi stored procedure tự quản lý transaction.
        """
        with self._cursor(transactional=transactional) as cursor:
            cursor.execute(_QUEUE_SESSION_SQL)
            cursor.execute(query, tuple(params or ()))
            sets = self._read_sets(cursor)
        return self._result(sets)

    def _check_connection(self) -> bool:
        """Kiểm tra nhanh SQL Server có phản hồi không. Không bao giờ ném lỗi."""
        try:
            self._execute_query('SELECT 1', transactional=False)
            return True
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning('Kiểm tra kết nối SQL Server thất bại: %s', type(exc).__name__)
            return False

    def execute_queue_procedure(self, procedure: str, parameters: Sequence[Any] = ()) -> dict[str, Any]:
        """Thực hiện một stored procedure trong schema Queue_job (một lần, không retry)."""
        if procedure not in _QUEUE_PROCEDURES:
            raise ValueError(f'Procedure không nằm trong danh sách hỗ trợ: {procedure}')
        values = tuple(parameters)
        sql = f'EXEC [Queue_job].[{procedure}]'
        if values:
            sql += ' ' + ','.join('?' for _ in values)
        return self._execute_query(sql, values, transactional=False)

    def _queue_call(self, procedure: str, *parameters: Any) -> dict[str, Any]:
        """
        Gọi procedure, retry có giới hạn khi lỗi tạm thời và an toàn để gửi lại.
        :raises DatabaseUnavailable: hết lượt retry mà vẫn lỗi tạm thời.
        :raises pyodbc.Error: lỗi nghiệp vụ / lỗi không retry được (giữ nguyên để caller phân loại).
        """
        resend_safe = procedure in _RESEND_SAFE
        attempts = self._queue_deadlock_retries + 1
        for attempt in range(attempts):
            try:
                return self.execute_queue_procedure(procedure, parameters)
            except (pyodbc.Error, _ConnectFailed, DatabaseCapacityTimeout) as exc:
                transient = _is_transient(exc)
                # Claim chỉ retry khi chắc chắn chưa có gì commit.
                can_retry = transient and (resend_safe or _definitely_not_sent(exc)
                                           or _is_rolled_back_retryable(exc))
                last = attempt == attempts - 1
                if not can_retry or last:
                    if transient:
                        logger.warning('Procedure %s lỗi tạm thời sau %d lần thử: %s',
                                       procedure, attempt + 1, type(exc).__name__)
                        raise DatabaseUnavailable(f'SQL Server tạm thời không khả dụng ({procedure})') from exc
                    raise

                delay = min(0.2 * (2 ** attempt), 2.0) + random.uniform(0, 0.2)
                logger.warning('Thử lại procedure=%s retry=%d delay=%.2fs lý do=%s',
                               procedure, attempt + 1, delay, type(exc).__name__)
                time.sleep(delay)
        raise AssertionError('unreachable')

    # ------------------------------------------------------------------
    # Nghiệp vụ hàng đợi
    # ------------------------------------------------------------------
    def enqueue(self, job_id: str, task: str, source: str, name: str, target: str | None = None) -> None:
        """
        Thêm công việc vào hàng đợi.
        :raises QueueFull, JobConflict: lỗi nghiệp vụ.
        :raises DatabaseUnavailable: chắc chắn CHƯA ghi (an toàn để xoá file và báo client thử lại).
        :raises EnqueueUncertain: có thể đã ghi – phải giữ file và để client poll job_id.
        """
        identity = _queue_job_id(job_id)
        if task not in {'detect', 'ocr', 'analyze'}:
            raise ValueError('task phải là detect, ocr hoặc analyze')
        source = _queue_relative_path(source)
        name = _queue_text(name, 255, 'original_name')
        target = _queue_text(target, 128, 'target', nullable=True)

        try:
            self._queue_call('Enqueue', identity, task, source, name, target)
        except DatabaseUnavailable as exc:
            if _definitely_not_sent(exc.__cause__):
                raise
            raise EnqueueUncertain(identity) from exc
        except pyodbc.Error as exc:
            if _queue_native_code(exc, 51002):
                raise QueueFull('Hàng đợi đã đầy') from exc
            if _queue_native_code(exc, 51003):
                raise JobConflict('Job ID đã tồn tại với nội dung khác') from exc
            raise EnqueueUncertain(identity) from exc

    def claim(self, worker_id: str) -> dict[str, Any] | None:
        """
        Worker nhận tối đa một job. Trả None nếu không có job phù hợp hoặc worker đã có job đang chạy.
        """
        worker_id = _queue_text(worker_id, 128, 'worker_id')
        rows = self._queue_call('Claim', worker_id, str(uuid.uuid4()))
        return rows['data'][0] if rows['data'] else None

    def build_manifest(self, artifacts: list[str] | None) -> str:
        """Chuẩn hoá và serialize danh sách artifact; raise ValueError nếu quá giới hạn."""
        paths = [_queue_relative_path(path) for path in (artifacts or [])]
        payload = json.dumps(paths, ensure_ascii=False, separators=(',', ':'))
        if len(payload.encode('utf-8')) > self._queue_max_manifest_bytes:
            raise ValueError('Danh sách artifacts vượt giới hạn cấu hình')
        return payload

    def finish(self, job: dict[str, Any], artifacts: list[str] | None = None, error: str | None = None) -> bool:
        """
        Ghi kết quả của một lần chạy.
        True: SQL chấp nhận kết quả. False: kết quả không còn thuộc lần chạy hiện tại.
        """
        payload = self.build_manifest(artifacts)
        error = _queue_text(error, 2048, 'error', nullable=True)
        token = str(uuid.UUID(str(job['claim_token'])))
        rows = self._queue_call(
            'Finish',
            _queue_job_id(job['id']),
            _queue_text(job['owner'], 128, 'owner'),
            int(job['attempt']),
            token,
            payload,
            error,
        )
        return bool(rows['data'][0]['accepted']) if rows['data'] else False

    def get(self, job_id: str) -> dict[str, Any] | None:
        """Đọc metadata của job. Trả None nếu ID không hợp lệ hoặc không tìm thấy."""
        try:
            identity = _queue_job_id(job_id)
        except ValueError:
            return None
        rows = self._queue_call('GetJob', identity)
        return rows['data'][0] if rows['data'] else None

    def recover(self, worker_id: str | None = None, *, confirmed_stopped: bool = True) -> list[str]:
        """
        Khôi phục job running của worker đã dừng (worker_id=None: tất cả).
        Chỉ gọi khi chắc chắn không còn process nào đang xử lý job đó.
        """
        if not confirmed_stopped:
            raise ValueError('Phải xác nhận tiến trình cũ đã kết thúc trước recovery')
        worker_id = _queue_text(worker_id, 128, 'worker_id', nullable=True)
        rows = self._queue_call('Recover', worker_id, True)
        return [row['id'] for row in rows['data']]

    def running(self, worker_id: str) -> dict[str, Any] | None:
        """Lấy job mà worker đang chạy (id, started) hoặc None."""
        worker_id = _queue_text(worker_id, 128, 'worker_id')
        rows = self._queue_call('Running', worker_id)
        return rows['data'][0] if rows['data'] else None

    def expired(self, cutoff: float) -> list[dict[str, Any]]:
        """Tìm job terminal đã kết thúc trước cutoff (Unix timestamp)."""
        moment = datetime.fromtimestamp(cutoff, timezone.utc).replace(tzinfo=None)
        return self._queue_call('Expired', moment)['data']

    def delete_terminal(self, identity: str, cutoff: float) -> bool:
        """Xóa metadata của job terminal đã hết hạn. Không xóa file trên storage."""
        identity = _queue_job_id(identity)
        moment = datetime.fromtimestamp(cutoff, timezone.utc).replace(tzinfo=None)
        rows = self._queue_call('DeleteTerminal', identity, moment)
        return bool(rows['data'][0]['deleted']) if rows['data'] else False


if __name__ == '__main__':
    # Chạy từ app/src:  python -m services.database_service
    from config.config import load_config
    database = MyDatabase.from_config(load_config())
    print('SQL Server OK' if database._check_connection() else 'SQL Server KHÔNG kết nối được')
