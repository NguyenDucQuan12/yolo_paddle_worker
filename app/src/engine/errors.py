"""
Các lỗi nghiệp vụ của engine, tách riêng để worker/test import mà không cần torch/paddle.
"""


class InputError(ValueError):
    """
    File đầu vào không hợp lệ (sai định dạng, quá nhiều trang, quá nhiều pixel, PDF hỏng...).
    Thông điệp an toàn để trả cho client; không phải lỗi server nên không retry, không recycle worker.
    """
