"""
Test nhanh API có dùng API key: kiểm tra xác thực → gửi file → chờ xử lý → lấy kết quả.
"""
import json
import os
import sys
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / 'app' / '.env')

# ============================== CẤU HÌNH ==============================
# Địa chỉ gateway.
BASE_URL = 'http://127.0.0.1:8000'

# API key client (cho /jobs) và admin key (cho /admin). Để None → tự đọc từ app/.env.
# Ví dụ đặt trực tiếp: API_KEY = 'abc123...'
API_KEY = None or os.getenv('VISION_API_KEY')
ADMIN_KEY = None or os.getenv('VISION_ADMIN_KEY')

# File gửi lên và loại xử lý: 'detect' | 'ocr' | 'analyze'.
FILE_PATH = ROOT / 'samples' / 'sample.png'
TASK = 'analyze'

# Số giây tối đa chờ job xử lý xong.
JOB_TIMEOUT_SECONDS = 300
# ======================================================================


def check(label, ok, detail=''):
    print(f"[{'OK ' if ok else 'FAIL'}] {label}" + (f" – {detail}" if detail else ''))
    return ok


def main():
    """Chạy lần lượt các bước test, in OK/FAIL cho từng bước."""
    # trust_env=False: bỏ qua proxy hệ thống khi gọi server nội bộ.
    client = httpx.Client(base_url=BASE_URL, timeout=60, trust_env=False)
    headers = {'X-API-Key': API_KEY} if API_KEY else {}
    passed = True

    # 1. Health không cần key.
    response = client.get('/health')
    passed &= check('GET /health (không cần key)', response.status_code == 200, response.text[:120])

    # 2. Xác thực: chỉ kiểm tra được khi server có đặt key.
    if API_KEY:
        response = client.get('/jobs/khong-ton-tai')
        passed &= check('Thiếu key → 401', response.status_code == 401, str(response.status_code))
        response = client.get('/jobs/khong-ton-tai', headers={'X-API-Key': 'sai-key'})
        passed &= check('Sai key → 401', response.status_code == 401, str(response.status_code))
        response = client.get('/jobs/khong-ton-tai', headers=headers)
        passed &= check('Đúng key → qua xác thực (404 vì job không tồn tại)',
                        response.status_code == 404, str(response.status_code))
    else:
        print('[SKIP] Chưa có API key → server đang tắt xác thực, bỏ qua test 401')

    # 3. Admin endpoint dùng admin key riêng.
    admin_headers = {'X-API-Key': ADMIN_KEY} if ADMIN_KEY else {}
    response = client.get('/admin/workers', headers=admin_headers)
    passed &= check('GET /admin/workers (admin key)', response.status_code == 200, str(response.status_code))
    if response.status_code == 200:
        data = response.json()
        print(f"       {data.get('ready_workers')}/{data.get('total_workers')} worker ready")
        for worker in data.get('workers', []):
            print(f"       {worker.get('worker_id') or worker.get('id')}: ready={worker.get('ready')} "
                  f"state={worker.get('state')}")

    # 4. Gửi file.
    path = Path(FILE_PATH)
    if not path.is_file():
        print(f'Không tìm thấy file {path}')
        return 1
    with path.open('rb') as stream:
        response = client.post('/jobs', params={'task': TASK}, headers=headers,
                               files={'file': (path.name, stream)})
    if not check(f'POST /jobs ({path.name}, task={TASK})', response.status_code == 202,
                 f'{response.status_code} {response.text[:200]}'):
        return 1
    job_id = response.json()['job_id']
    print(f'       job_id = {job_id}')

    # 5. Poll trạng thái tới khi xong.
    deadline = time.monotonic() + JOB_TIMEOUT_SECONDS
    status = None
    while time.monotonic() < deadline:
        job = client.get(f'/jobs/{job_id}', headers=headers).json()
        if job.get('status') != status:
            status = job.get('status')
            print(f'       trạng thái: {status}')
        if status in ('succeeded', 'failed'):
            break
        time.sleep(1)
    if not check('Job hoàn thành', status == 'succeeded', job.get('error') or str(status)):
        return 1

    # 6. Lấy kết quả JSON.
    response = client.get(f'/jobs/{job_id}/result', headers=headers)
    passed &= check('GET /jobs/{id}/result', response.status_code == 200, str(response.status_code))
    if response.status_code == 200:
        print(json.dumps(response.json(), ensure_ascii=False, indent=2)[:1500])

    print('\nKẾT QUẢ:', 'TẤT CẢ ĐỀU QUA' if passed else 'CÓ BƯỚC LỖI')
    return 0 if passed else 1


if __name__ == '__main__':
    sys.exit(main())
