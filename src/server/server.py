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
    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        self._host = host
        self._port = port

    
        
    def run(self):
        start_http_server(addr=self._host, port=self._port)
        # Keep the server running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Server stopped")
        
        
if __name__ == "__main__":
    server = HttpServer()
    server.run()

