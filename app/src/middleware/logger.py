# middlewares/log_requests.py
import json, time, uuid
import socket
from typing import Dict

from fastapi import Request
from fastapi.responses import Response
from starlette.concurrency import iterate_in_threadpool
from log.api_log import logger


# Các đường dẫn ít giá trị (giảm ồn). Có thể bỏ /docs,/redoc nếu bạn muốn log cả trang docs.
EXCLUDED_PATHS = {"/redoc", "/docs", "/openapi.json", "/health", "/readyz"}

HEALTH_PATHS = {"/healthz", "/readyz"}

# Khóa nhạy cảm cần che khi log request params/body
SENSITIVE_KEYS = {"password", "token", "authorization", "apikey", "secret"}

# Giới hạn kích thước dữ liệu (request/response) đem đi log để tránh phình log/disk
MAX_LOG_BYTES = 64 * 1024  # 64KB


def sanitize_dict(d: Dict) -> Dict:
    """
    Ẩn các trường nhạy cảm trong dict (vd: query params).
    """
    out = {}
    for k, v in d.items():
        out[k] = "***" if k.lower() in SENSITIVE_KEYS else v
    return out

def get_client_ip(request: Request) -> str:
    """
    Nhận 1 request từ FastAPI và trả về địa chỉ IP của client  
    - Nếu có X-Forwarded-For: lấy phần tử đầu (client gốc). Vì nếu triển khai trên Nginx thì ưu tiên X-Forwarded-For
    - Else: request.client.host
    """
    # Lưu ý một số nginx có thể set header là "X-real-ip", cần thay đổi cho đúng
    xff = request.headers.get("x-forwarded-for") or request.headers.get("X-Forwarded-For")
    if xff:
        # format: "client, proxy1, proxy2"
        return xff.split(",")[0].strip()
    
    client = request.client
    return client.host if client else "Unknown"

async def log_requests(request: Request, call_next):
    """
    Middleware ghi log cho MỖI request/response.
    - Bắt đầu đo thời gian.
    - Thu thập thông tin caller (IP/hostname), method, path, query params, user-agent, correlation-id.
    - (Tùy chọn) đọc request body cỡ nhỏ để log.
    - Gọi handler thật, bắt exception để vẫn log khi lỗi.
    - Đọc response body -> ghi log theo content-type (JSON/text/binary) với preview <= MAX_LOG_BYTES.
    - Khôi phục body_iterator để response trả FULL về client.
    """
    start = time.perf_counter()

    # Thông tin request cơ bản
    method = request.method
    path = request.url.path
    ua = request.headers.get("user-agent", "-")
    # Correlation-ID: lấy từ header nếu có, không thì tự sinh
    cid = request.headers.get("x-request-id") or str(uuid.uuid4())

    # Lấy IP/hostname của client (gethostbyaddr có thể tốn thời gian nếu DNS ngược chậm)
    client_ip = get_client_ip(request= request)
    try:
        client_hostname = socket.gethostbyaddr(client_ip)[0]
    except Exception:
        client_hostname = "GUEST"

    # Lấy query parameters và che thông tin nhạy cảm
    params = sanitize_dict(dict(request.query_params))

    # --- Đọc request body để log (nếu nhỏ & là JSON) ---
    # Cẩn thận: sau khi đọc, PHẢI gắn lại body cho downstream qua custom receive()
    req_body_for_log = "-"
    try:
        raw_body = await request.body()
        if (
            raw_body
            and len(raw_body) <= MAX_LOG_BYTES
            and "application/json" in request.headers.get("content-type", "")
        ):
            try:
                # Parse JSON để log cho gọn gàng
                req_body_for_log = json.dumps(json.loads(raw_body), ensure_ascii=False)
            except Exception:
                # Không parse được thì log chuỗi đã decode
                req_body_for_log = raw_body.decode(errors="ignore")

        # Quan trọng: gắn lại body cho downstream, nếu không handler sẽ thấy body rỗng
        async def receive():
            return {"type": "http.request", "body": raw_body, "more_body": False}

        request = Request(request.scope, receive)
    except Exception:
        # Không để lỗi logging ảnh hưởng xử lý request
        pass

    # Không ghi log đối với các đường dẫn health check
    if path in HEALTH_PATHS:
        return await call_next(request)

    # Bỏ qua những path ít giá trị để giảm ồn (nhưng vẫn đo duration)
    if path in EXCLUDED_PATHS:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "",
            extra={
                "hostname": client_hostname,
                "ip": client_ip,
                "api_name": path,
                "params": "excluded_path",
                "result": "excluded_path",
                "method": method,
                "status": 200,  # docs thường trả 200
                "duration_ms": f"{duration_ms:.2f}",
                "user_agent": ua,
                "correlation_id": cid,
                "request_body": "-",  # không cần log body ở các path này
            },
        )
        return await call_next(request)

    # Gọi handler thật, vẫn bọc try/except để đảm bảo LUÔN có log khi lỗi xảy ra sớm
    try:
        response = await call_next(request)
    except Exception as exc:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.exception(
            "",
            extra={
                "hostname": client_hostname,
                "ip": client_ip,
                "api_name": path,
                "params": json.dumps(params, ensure_ascii=False),
                "result": f"Exception: {exc!r}",
                "method": method,
                "status": 500,
                "duration_ms": f"{duration_ms:.2f}",
                "user_agent": ua,
                "correlation_id": cid,
                "request_body": req_body_for_log,
            },
        )
        # Re-raise để FastAPI vẫn xử lý error pipeline (handlers/exception handlers)
        raise

    # --- Đọc FULL body của response để vừa log vừa trả lại cho client ---
    content_type = response.headers.get("Content-Type", "")  # ví dụ: application/json; charset=utf-8
    body = b""
    async for chunk in response.body_iterator:
        # Gom toàn bộ thành bytes để khách vẫn nhận đủ dữ liệu
        body += chunk

    # Khôi phục stream body cho client: biến bytes -> iterator một phần tử
    response.body_iterator = iterate_in_threadpool(iter([body]))

    # --- Chuẩn bị nội dung 'result' đem ghi log ---
    # Với JSON: parse và log lại ở dạng JSON đẹp (giới hạn MAX_LOG_BYTES); nếu dài, thêm 'truncated'
    if "application/json" in content_type:
        try:
            preview = body[:MAX_LOG_BYTES] or b"{}"
            log_result = json.dumps(json.loads(preview), ensure_ascii=False)
            if len(body) > MAX_LOG_BYTES:
                log_result += f' ... <truncated {len(body)-MAX_LOG_BYTES} bytes>'
        except Exception:
            # Không parse được -> log chuỗi preview
            log_result = body[:MAX_LOG_BYTES].decode(errors="ignore")

    # Với text/*: decode và giới hạn độ dài
    elif "text" in content_type:
        text_preview = body[:MAX_LOG_BYTES].decode(errors="ignore")
        log_result = (
            text_preview
            + (f' ... <truncated {len(body)-MAX_LOG_BYTES} bytes>' if len(body) > MAX_LOG_BYTES else "")
        )

    # Nhị phân: log tên file từ header + kích thước dữ liệu
    else:
        file_name = response.headers.get("content-disposition", "")
        log_result = f"Content: {file_name} ; Binary data of length: {len(body)}"

    duration_ms = (time.perf_counter() - start) * 1000

    # --- Ghi log: dùng extra để đổ vào formatter tuỳ biến ---
    logger.info(
        "",
        extra={
            "hostname": client_hostname,
            "ip": client_ip,
            "api_name": path,
            "params": json.dumps(params, ensure_ascii=False),
            "result": log_result,
            "method": method,
            "status": response.status_code,
            "duration_ms": f"{duration_ms:.2f}",
            "user_agent": ua,
            "correlation_id": cid,
            "request_body": req_body_for_log,
        },
    )

    # Trả response FULL cho client (y hệt như handler đã tạo)
    return Response(
        content=body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=content_type,
    )