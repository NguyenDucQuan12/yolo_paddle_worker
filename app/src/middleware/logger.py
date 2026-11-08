# middlewares/log_requests.py
import json, time, uuid
from typing import Dict

from fastapi import Request
from fastapi.responses import Response
from starlette.concurrency import iterate_in_threadpool
from log.api_log import logger


# Các đường dẫn ít giá trị (giảm ồn). Có thể bỏ /docs,/redoc nếu bạn muốn log cả trang docs.
EXCLUDED_PATHS = {"/redoc", "/docs", "/openapi.json", "/health", "/readyz"}

HEALTH_PATHS = {"/health", "/healthz", "/readyz"}

# Khóa nhạy cảm cần che khi log request params/body
SENSITIVE_KEYS = {"password", "token", "authorization", "apikey", "api_key", "x-api-key", "secret"}

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
    # Giới hạn độ dài/ký tự để client không chèn rác vào log qua header.
    cid = (request.headers.get("x-request-id") or "")[:64].strip()
    if not cid or not all(ch.isalnum() or ch in "-_." for ch in cid):
        cid = str(uuid.uuid4())

    # Lấy IP của client. KHÔNG tra DNS ngược (gethostbyaddr) ở đây: trên Windows/LAN lỗi DNS có thể
    # làm mỗi request chậm thêm vài giây và chiếm thread pool. Cần hostname thì tra offline từ log.
    client_ip = get_client_ip(request= request)
    client_hostname = client_ip

    # Lấy query parameters và che thông tin nhạy cảm
    params = sanitize_dict(dict(request.query_params))

    # --- Thu thập request body trong giới hạn cho JSON ---
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    is_json = content_type == "application/json" or content_type.endswith("+json")
    captured_body = bytearray()
    request_body_too_large = False
    request_body_complete = False

    if is_json:
        original_receive = request.receive

        async def receive_with_capture():
            nonlocal request_body_too_large, request_body_complete
            message = await original_receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                if not request_body_too_large:
                    if len(captured_body) + len(chunk) > MAX_LOG_BYTES:
                        captured_body.clear()
                        request_body_too_large = True
                    else:
                        captured_body.extend(chunk)
                if not message.get("more_body", False):
                    request_body_complete = True
            return message

        request = Request(request.scope, receive_with_capture)

    def get_request_body_preview():
        if not is_json or request_body_too_large or not request_body_complete or not captured_body:
            return "-"

        raw_body = bytes(captured_body)
        try:
            # Parse JSON để log cho gọn gàng
            return json.dumps(json.loads(raw_body), ensure_ascii=False)
        except Exception:
            # Không parse được thì log chuỗi đã decode
            return raw_body.decode(errors="ignore")

    # Không ghi log đối với các đường dẫn health check
    if path in HEALTH_PATHS:
        response = await call_next(request)
        response.headers["X-Request-ID"] = cid
        return response

    # Bỏ qua những path ít giá trị để giảm ồn
    if path in EXCLUDED_PATHS:
        response = await call_next(request)
        response.headers["X-Request-ID"] = cid
        return response

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
                "request_body": get_request_body_preview(),
            },
        )
        # Re-raise để FastAPI vẫn xử lý error pipeline (handlers/exception handlers)
        raise

    # --- Đọc FULL body của response để vừa log vừa trả lại cho client ---
    content_type = response.headers.get("Content-Type", "")  # ví dụ: application/json; charset=utf-8
    body = b""
    file_name = response.headers.get("content-disposition", "")
    if file_name:
        log_result = (
            f"Content: {file_name} ; Binary data of length: "
            f"{response.headers.get('content-length', 'unknown')}"
        )
    else:
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
            "request_body": get_request_body_preview(),
        },
    )

    if file_name:
        response.headers["X-Request-ID"] = cid
        return response

    # Trả response FULL cho client (y hệt như handler đã tạo) + X-Request-ID để client báo lỗi kèm mã tra log.
    headers = dict(response.headers)
    headers["X-Request-ID"] = cid
    return Response(
        content=body,
        status_code=response.status_code,
        headers=headers,
        media_type=content_type,
    )