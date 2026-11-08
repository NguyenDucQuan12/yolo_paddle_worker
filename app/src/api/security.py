"""
Xác thực bằng API key qua header X-API-Key.

- Gateway: endpoint /jobs yêu cầu server.api_key; /admin yêu cầu server.admin_key.
- Worker : MỌI endpoint /jobs yêu cầu server.admin_key (cổng worker là cổng nội bộ, gửi job thẳng
           vào một worker cụ thể – không dành cho client thường).
- Khóa rỗng/không đặt → tắt xác thực cho nhóm endpoint đó (chỉ dùng khi dev; startup sẽ log cảnh báo).
So sánh bằng secrets.compare_digest để chống timing attack.
"""
import secrets

from fastapi import HTTPException, Request
from fastapi.security import APIKeyHeader

API_KEY_HEADER = APIKeyHeader(name='X-API-Key', auto_error=False, description='API key của client')


def _check(provided, expected):
    if not expected:
        return
    if not provided or not secrets.compare_digest(str(provided).encode(), str(expected).encode()):
        raise HTTPException(status_code=401, detail='Thiếu hoặc sai X-API-Key',
                            headers={'WWW-Authenticate': 'ApiKey'})


def require_client_key(request: Request):
    """Dependency cho /jobs. Trên worker thì đòi admin_key."""
    server = request.app.state.config['server']
    expected = server.get('admin_key') if request.app.state.worker_info else server.get('api_key')
    _check(request.headers.get('X-API-Key'), expected)


def require_admin_key(request: Request):
    """Dependency cho /admin."""
    _check(request.headers.get('X-API-Key'), request.app.state.config['server'].get('admin_key'))
