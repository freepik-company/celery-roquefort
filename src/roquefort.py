import asyncio
import logging
import random
import socket
from celery import Celery
from fastapi import FastAPI
from metrics.metrics import MetricService
from server.server import HttpServer

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
        self._metrics: MetricService = MetricService(metric_prefix=prefix, custom_labels=custom_labels)  
        self._server: HttpServer = HttpServer(host=host, port=port, lifespan=self._lifespan())
        self._shutdown = False
        
        # Create metrics
        #   Counters
        self._metrics.create_counter("task_sent", "Sent when a task message is published.", labels=["name", "hostname", "queue_name"])
        self._metrics.create_counter("task_received", "Received when a task message is received.", labels=["name", "hostname", "queue_name"])
        self._metrics.create_counter("task_started", "Sent just before a worker runs a task.", labels=["name", "hostname", "queue_name"])
        self._metrics.create_counter("task_succeeded", "Sent if the task was executed successfully.", labels=["name", "hostname", "queue_name"])
        self._metrics.create_counter("task_failed", "Sent if the task failed.", labels=["name", "hostname", "queue_name", "exception"])
        self._metrics.create_counter("task_retried", "Sent if the task was retried.", labels=["name", "hostname", "queue_name", "exception"])
        self._metrics.create_counter("task_rejected", "Sent if the task was rejected.", labels=["name", "hostname", "queue_name", "exception"])
        self._metrics.create_counter("task_revoked", "Sent if the task was revoked.", labels=["name", "hostname", "queue_name", "exception"])
        #   Gauges
        self._metrics.create_gauge("worker_active", "Number of active workers. It indicates that the worker has recently sent a heartbeat.", labels=["hostname"])
        self._metrics.create_gauge("worker_tasks_active", "Number of tasks currently being processed by the workers.", labels=["hostname", "queue_name"])
        self._metrics.create_gauge("queue_length", "Number of tasks in the queue.", labels=["queue_name"])
        self._metrics.create_gauge("active_consumer_count", "The number of active consumer in broker queue.", labels=["queue_name"])
        self._metrics.create_gauge("active_worker_count", "The number of active workers in broker queue.", labels=["queue_name"])
        self._metrics.create_gauge("active_process_count", "The number of active processes in broker queue.", labels=["queue_name"])
        #   Histograms
        self._metrics.create_histogram("task_runtime", "Histogram of task runtime measurements.", labels=["name", "hostname", "queue_name"])
        
        
    def _lifespan(self):
        async def lifespan(app: FastAPI):
            asyncio.create_task(self.update_metrics())
            yield
        return lifespan
    
    async def update_metrics(self):
        asyncio.sleep(1)
        app = Celery(broker=self._broker_url)
        handlers = {
            "task-sent": self._handle_task_sent,
            "task-received": self._handle_task_received,
            "task-started": self._handle_task_started,
            "task-succeeded": self._handle_task_succeeded,
            "task-failed": self._handle_task_failed,
            "task-retried": self._handle_task_retried,
            "task-rejected": self._handle_task_rejected,
            "task-revoked": self._handle_task_revoked,
            "worker-heartbeat": self._handle_worker_heartbeat,
        }
        
        with app.connection() as connection:
            while True:
                try:
                    logging.warning("Updating metrics")
                    recv = app.events.Receiver(connection, handlers=handlers)
                    recv.capture(limit=None, timeout=1, wakeup=True)
                except (KeyboardInterrupt, SystemExit):
                    logging.info("Shutdown signal received, stopping metrics collection")
                    break
                except socket.timeout:
                    logging.warning("Timeout in update_metrics")
                    continue
                except Exception as e:
                    logging.exception(f"Unexpected error in update_metrics: {e}", exc_info=True)
                    raise
                await asyncio.sleep(1)
    
    def run(self):
        self._server.run()
        
    def _handle_task_generic(self, event, metric_name: str, labels: dict):
        logging.warning(f"{metric_name} received: {event}")
        try:
            self._metrics.increment_counter(name=metric_name, labels=labels)
        except Exception as e:
            logging.error(f"Error setting {metric_name} metric: {e}")
            
    def _handle_task_sent(self, event):
        self._handle_task_generic(
            event, 
            "task_sent", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            },
        )
        
    def _handle_task_received(self, event):
        self._handle_task_generic(
            event, 
            "task_received", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            }
        )
        
    def _handle_task_started(self, event):
        self._handle_task_generic(
            event, 
            "task_started", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            }
        )
        
    def _handle_task_succeeded(self, event):
        self._handle_task_generic(
            event, 
            "task_succeeded", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            }
        )
    
    def _handle_task_failed(self, event):
        self._handle_task_generic(
            event, 
            "task_failed", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            }
        )
        
    def _handle_task_retried(self, event):
        self._handle_task_generic(
            event, 
            "task_retried", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            }
        )
        
    def _handle_task_rejected(self, event):
        self._handle_task_generic(
            event, 
            "task_rejected", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            }
        )
        
    def _handle_task_revoked(self, event):
        self._handle_task_generic(
            event, 
            "task_revoked", 
            {
                "name": event.get("name", "unknown"), 
                "hostname": event.get("hostname", "unknown"), 
                "queue_name": event.get("queue", "unknown")
            }
        )
        
    def _handle_worker_heartbeat(self, event):
        logging.warning(f"Worker heartbeat received: {event}")
        try:
            self._metrics.set_gauge(name="worker_active", value=True, labels={"hostname": event["hostname"]})
        except Exception as e:
            logging.error(f"Error setting worker_active metric: {e}")
        
        
        
        
def main():
    roquefort = Roquefort(broker_url="redis://localhost:6379/0", host="0.0.0.0", port=8001, custom_labels={"who_you_gonna_call": "ghostbusters"})
    roquefort.run()
    
    
if __name__ == "__main__":
    main()