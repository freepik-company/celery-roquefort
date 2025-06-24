import asyncio
from contextlib import asynccontextmanager
import logging
import time
from typing import Callable
from fastapi import FastAPI
import uvicorn
from prometheus_client import disable_created_metrics, make_asgi_app, Counter, start_http_server

from ..metrics.metrics import MetricService

class HttpServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8000, registry = None):
        self._host = host
        self._port = port
        self._registry = registry

    async def run(self):
        logging.info(f"Starting HTTP server on {self._host}:{self._port}")
        start_http_server(addr=self._host, port=self._port, registry=self._registry)
        logging.info(f"HTTP server started on {self._host}:{self._port}")
        await asyncio.sleep(1)
        
        
if __name__ == "__main__":
    server = HttpServer()
    server.run()

