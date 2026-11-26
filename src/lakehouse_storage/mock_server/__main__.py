"""Run with python -m lakehouse_storage.mock_server."""
import os

import uvicorn

from lakehouse_storage.mock_server.app import create_app
from lakehouse_storage.service_config import ServiceConfig, require_namespace

if __name__ == '__main__':
    config = ServiceConfig.from_env()
    store = os.getenv('MOCK_STORE', str(config.root / 'mock'))
    require_namespace(store, config.root)
    uvicorn.run(create_app(store), host=os.getenv('MOCK_HOST', '127.0.0.1'),
                port=int(os.getenv('MOCK_PORT', '8010')))
