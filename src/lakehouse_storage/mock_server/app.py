"""HTTP access to immutable mock runs; no storage credentials required."""
from pathlib import Path
from threading import Lock

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response

from lakehouse_storage.mock_server.models import RunRequest
from lakehouse_storage.mock_server.store import RunStore
from lakehouse_storage.mock_server.scenarios import delivery_status


def create_app(store_root: str | Path):
    store = RunStore(store_root)
    delivery_lock = Lock()
    app = FastAPI(title='Smart Dairy storage mock', version='1.0')

    @app.get('/health')
    def health():
        if not store.root.is_dir():
            raise HTTPException(503, 'payload store unavailable')
        return {'status': 'ok'}

    @app.post('/runs', status_code=201)
    def create_run(request: RunRequest):
        return store.create(request)

    @app.get('/runs/{run_id}')
    def get_run(run_id: str):
        try:
            return store.run(run_id)
        except (FileNotFoundError, ValueError):
            raise HTTPException(404, 'run not found')

    @app.get('/runs/{run_id}/batches/{sequence}')
    def get_batch(run_id: str, sequence: int):
        try:
            payload = store.batch(run_id, sequence)
        except (FileNotFoundError, ValueError):
            raise HTTPException(404, 'batch not found')
        with delivery_lock:
            status = delivery_status(store, run_id, sequence)
        if status != 200:
            raise HTTPException(status, 'scripted transient failure')
        return Response(payload, media_type='application/json')

    return app
