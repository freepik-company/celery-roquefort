import asyncio
import logging
import random
from celery import Celery
from fastapi import FastAPI
from .metrics.metrics import MetricService
from .server.server import HttpServer

class Roquefort:
    
    def __init__(
        self, 
        broker_url: str, 
        host: str,
        port: int,
        prefix: str = "roquefort_", 
        custom_labels: dict = None
    ) -> None:
        self._broker_url = broker_url
        self._host = host
        self._port = port
        self._prefix = prefix
        self._custom_labels = custom_labels
        
        # Create the HTTP server with import string support
        self._server: HttpServer = HttpServer(host=self._host, port=self._port)
    
    def run(self):
        self._server.run()
        
        
def main():
    roquefort = Roquefort(broker_url="redis://localhost:6379/0", host="0.0.0.0", port=8001, custom_labels={"who_you_gonna_call": "ghostbusters"})
    roquefort.run()
    
    
if __name__ == "__main__":
    main()