"""
Tiện ích ghi file an toàn, không phụ thuộc torch/paddle để gateway và test import nhẹ.
"""
import json
import os
from pathlib import Path


def write_json(path: Path, value):
    """
    Ghi dict hoặc list ra file JSON an toàn, thông qua file tạm rồi replace nguyên tử.
    Nếu crash giữa chừng, file đích cũ (nếu có) vẫn còn nguyên.
    """
    path = Path(path)
    temp = path.with_suffix(path.suffix + '.tmp')   # data/result.json.tmp

    with temp.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())

    temp.replace(path)
