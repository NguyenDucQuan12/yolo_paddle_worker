"""
Các hàm tiện ích dùng chung trong project
"""
import ctypes
import re
import pyodbc

def get_odbc_drivers_for_sql_server():
    """
    Lấy danh sách các ODBC Driver đã cài trên máy tính  
    Sắp xếp theo SỐ phiên bản, tránh sắp xếp 9 cao hơn 18 bằng chuỗi.
    Trả về danh sách tên driver, ví dụ: ['ODBC Driver 17 for SQL Server', 'ODBC Driver 18 for SQL Server']
    """
    pairs = []
    for name in pyodbc.drivers():
        match = re.fullmatch(r'ODBC Driver (\d+) for SQL Server', name)
        if match:
            pairs.append((int(match.group(1)), name))
    return [name for _, name in sorted(pairs)]

def get_screen_dpi(base_font_size=10, base_row_height=28):
    """
    Lấy DPI của màn hình chính và tính toán font size và row height phù hợp.
    Trả về font size và row height phù hợp với DPI hệ thống Windows.

    Trên Windows, DPI được đọc từ màn hình chính. Nếu API không khả dụng
    hoặc trả về giá trị không hợp lệ, kích thước cơ sở được sử dụng.
    """
    fallback = (int(base_font_size), int(base_row_height))
    if not hasattr(ctypes, "windll"):
        return fallback

    user32 = ctypes.windll.user32
    gdi32 = ctypes.windll.gdi32
    device_context = user32.GetDC(0)
    if not device_context:
        return fallback

    try:
        vertical_dpi = gdi32.GetDeviceCaps(device_context, 90)  # LOGPIXELSY
    finally:
        user32.ReleaseDC(0, device_context)

    if vertical_dpi <= 0:
        return fallback

    scale = vertical_dpi / 96
    font_size = max(1, round(base_font_size * scale))
    row_height = max(1, round(base_row_height * scale))
    return font_size, row_height
