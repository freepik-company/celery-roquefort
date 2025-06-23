import asyncio
from contextlib import asynccontextmanager
import logging
from typing import Callable
from fastapi import FastAPI
import uvicorn
from prometheus_client import disable_created_metrics, make_asgi_app, Counter

from metrics.metrics import MetricService

class HttpServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8000, lifespan: Callable = None):
        self._host = host
        self._port = port
        self._app = FastAPI(lifespan=lifespan)
        self._app.mount("/metrics", make_asgi_app())
        self._setup_health_check()
        
    def _health_check_handler(self):
        return {"status": "healthy", "service": "roquefort"}
    
    def _setup_health_check(self):
        self._app.add_api_route(
            "/health-check",
            self._health_check_handler,
            methods=["GET"],
            summary="Health check endpoint",
            description="Returns the health status of the service"
        )
        
    def run(self):
        uvicorn.run(self._app, host=self._host, port=self._port)
        
        
if __name__ == "__main__":
    server = HttpServer()
    server.run()

