"""
Endpoint quản trị trên gateway (yêu cầu X-API-Key = server.admin_key):
    GET /admin/workers  – tổng hợp /health của mọi worker + trạng thái supervisor (restart, circuit).
"""
import asyncio
import json
from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, Request

from api.security import require_admin_key

router = APIRouter(prefix="/admin", tags=["Admin"], dependencies=[Depends(require_admin_key)])


async def _probe(client, worker):
    host = '127.0.0.1' if worker['host'] == '0.0.0.0' else worker['host']
    try:
        response = await client.get(f'http://{host}:{worker["port"]}/health')
        data = response.json()
        data['reachable'] = True
        return data
    except (httpx.HTTPError, ValueError) as exc:
        return {'worker_id': worker['id'], 'reachable': False, 'ready': False,
                'error': type(exc).__name__}


@router.get('/workers', summary="Trạng thái các worker và supervisor")
async def workers(request: Request):
    config = request.app.state.config
    async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
        results = await asyncio.gather(*(_probe(client, worker) for worker in config['workers']))

    # Supervisor ghi trạng thái restart/circuit ra file này mỗi vài giây (xem main.Supervisor).
    supervisor = None
    status_file = Path(config['storage']['root']) / 'supervisor_status.json'
    try:
        supervisor = json.loads(status_file.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        pass

    return {
        'ready_workers': sum(1 for item in results if item.get('ready')),
        'total_workers': len(results),
        'workers': results,
        'supervisor': supervisor,
    }
