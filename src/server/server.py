from abc import ABC, abstractmethod
import asyncio
from contextlib import asynccontextmanager
import logging
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import (
    start_http_server,
    make_asgi_app,
)
import uvicorn


class HttpServer(ABC):
    @abstractmethod
    async def run(self):
        pass


class PrometheusClientServer(HttpServer):
    def __init__(self, host: str = "0.0.0.0", port: int = 8000, registry=None):
        self._host = host
        self._port = port
        self._registry = registry

    async def run(self):
        logging.info(f"Starting HTTP server on {self._host}:{self._port}")
        start_http_server(addr=self._host, port=self._port, registry=self._registry)
        logging.info(f"HTTP server started on {self._host}:{self._port}")
        await asyncio.sleep(1)


class FastAPIServer(HttpServer):
    def __init__(
        self, 
        host: str = "0.0.0.0", 
        port: int = 8000, 
        registry=None,
        lifespan_method=None,
        health_check_method=None,
    ):
        self._host = host
        self._port = port
        self._registry = registry
        self._lifespan_method = lifespan_method
        self._health_check_method = health_check_method

    async def run(self):
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            logging.info("Starting FastAPI server")
            yield
            logging.info("Shutting down FastAPI server")
            
        logging.info(f"Starting FastAPI server on {self._host}:{self._port}")
        
        app = FastAPI(lifespan=lifespan)
        
        app.get("/health-check")(self.health_check_handler)
        app.mount("/metrics", make_asgi_app(registry=self._registry), name="prometheus-metrics")
        
        config = uvicorn.Config(app, host=self._host, port=self._port)
        server = uvicorn.Server(config)
        await server.serve()
        
    async def health_check_handler(self) -> JSONResponse:
        status = "ok"
        status_code = 200
        
        if not self._health_check_method:
            return JSONResponse(content={"status": status}, status_code=status_code)
        
        try:
            await self._health_check_method()
        except Exception as e:
            status = "error"
            status_code = 500
            logging.error(f"Health check failed: {e}")
            
        return JSONResponse(content={"status": status}, status_code=status_code)