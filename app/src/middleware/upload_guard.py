"""
Middleware ASGI bảo vệ endpoint upload (POST /jobs) TRƯỚC khi Starlette đọc body multipart.

Lý do phải làm ở tầng ASGI: FastAPI/Starlette parse toàn bộ multipart vào file tạm trước khi vào
handler, nên kiểm tra kích thước trong handler là quá muộn (file 10 GB đã chiếm đĩa).

Chức năng (đọc từ config['server']):
    - upload_concurrency    : số upload đồng thời tối đa; vượt → 503 ngay, không đọc body.
    - max_body_mb           : Content-Length lớn hơn → 413 ngay; body stream (chunked) vượt → 413.
    - upload_timeout_seconds: nhận body quá lâu (client chậm/slowloris) → 408.
"""
import asyncio
import json
import time

from fastapi import HTTPException


class UploadGuardMiddleware:
    """Pure ASGI middleware (không dùng BaseHTTPMiddleware để không buffer body)."""

    def __init__(self, app, config, path='/jobs'):
        self.app = app
        server = config['server']
        self.path = path.rstrip('/')
        self.max_body = int(server['max_body_mb'] * 2**20)
        self.timeout = float(server['upload_timeout_seconds'])
        self.slots = asyncio.Semaphore(int(server['upload_concurrency']))
        self.concurrency = int(server['upload_concurrency'])

    @staticmethod
    async def _reject(send, status, detail, headers=None):
        body = json.dumps({'detail': detail}, ensure_ascii=False).encode()
        raw_headers = [(b'content-type', b'application/json'), (b'content-length', str(len(body)).encode()),
                       (b'connection', b'close')]
        for key, value in (headers or {}).items():
            raw_headers.append((key.lower().encode(), str(value).encode()))
        await send({'type': 'http.response.start', 'status': status, 'headers': raw_headers})
        await send({'type': 'http.response.body', 'body': body})

    async def __call__(self, scope, receive, send):
        if (scope['type'] != 'http' or scope['method'] != 'POST'
                or scope['path'].rstrip('/') != self.path):
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get('headers') or [])
        length = headers.get(b'content-length')
        if length is not None:
            try:
                if int(length) > self.max_body:
                    await self._reject(send, 413, 'Body vượt quá giới hạn cho phép')
                    return
            except ValueError:
                await self._reject(send, 400, 'Content-Length không hợp lệ')
                return

        # Không chờ slot: hết slot thì từ chối ngay để client thử lại (backpressure rõ ràng).
        if self.slots.locked():
            await self._reject(send, 503, 'Server đang nhận quá nhiều upload, vui lòng thử lại',
                               {'Retry-After': '2'})
            return

        async with self.slots:
            received = 0
            deadline = time.monotonic() + self.timeout

            async def guarded_receive():
                nonlocal received
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise HTTPException(408, 'Upload quá thời gian cho phép')
                try:
                    message = await asyncio.wait_for(receive(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise HTTPException(408, 'Upload quá thời gian cho phép') from exc
                if message['type'] == 'http.request':
                    received += len(message.get('body', b''))
                    if received > self.max_body:
                        raise HTTPException(413, 'Body vượt quá giới hạn cho phép')
                return message

            await self.app(scope, guarded_receive, send)
