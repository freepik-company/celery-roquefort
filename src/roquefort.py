# roquefort.py
import asyncio
import logging
import socket
import time
from celery import Celery, Task
from fastapi import FastAPI

from .helpers import (
    format_queue_names,
    get_exception_name,
    get_queue_name_from_worker_metadata,
    get_worker_names,
)
from .metrics.metrics import MetricService
from .server.server import HttpServer


class Roquefort:
    def __init__(
        self,
        broker_url: str,
        host: str,
        port: int,
        prefix: str = "roquefort_",
        custom_labels: dict = None,
        default_queue_name: str = None,
        worker_ttl: int = 60,
    ) -> None:
        self._broker_url = broker_url
        self._metrics = MetricService(metric_prefix=prefix, custom_labels=custom_labels)
        self._server = HttpServer(host=host, port=port, registry=self._metrics.get_registry())
        self._app = None
        self._state = None
        self._shutdown_event = asyncio.Event()
        self._server_started = False
        self._workers_metadata = {}
        self._default_queue_name = default_queue_name
        self._worker_ttl = worker_ttl
        self._tracked_events = [
            "task-sent", "task-received", "task-started", "task-succeeded", "task-failed",
            "task-retried", "task-rejected", "task-revoked",
            "worker-heartbeat", "worker-online", "worker-offline"
        ]
        self._create_metrics()

    def _lifespan(self):
        async def lifespan(app: FastAPI):
            asyncio.create_task(self.update_metrics())
            yield
        return lifespan

    def _create_metrics(self):
        counters = [
            ("task_sent", "Sent when a task message is published.", ["name", "hostname", "queue_name"]),
            ("task_received", "Received when a task message is received.", ["name", "worker", "hostname", "queue_name"]),
            ("task_started", "Sent just before a worker runs a task.", ["name", "worker", "hostname", "queue_name"]),
            ("task_succeeded", "Sent if the task was executed successfully.", ["name", "worker", "hostname", "queue_name"]),
            ("task_failed", "Sent if the task failed.", ["name", "worker", "hostname", "queue_name", "exception"]),
            ("task_retried", "Sent if the task was retried.", ["name", "worker", "hostname", "queue_name", "exception"]),
            ("task_rejected", "Sent if the task was rejected.", ["name", "worker", "hostname", "queue_name"]),
            ("task_revoked", "Sent if the task was revoked.", ["name", "worker", "hostname", "queue_name"]),
        ]

        gauges = [
            ("worker_active", "Number of active workers.", ["hostname", "worker", "queue_name"]),
            ("worker_tasks_active", "Number of tasks currently being processed by workers.", ["hostname", "queue_name"]),
            ("queue_length", "Number of tasks in the queue.", ["queue_name"]),
            ("active_consumer_count", "Number of active consumers in broker queue.", ["queue_name"]),
            ("active_worker_count", "Number of active workers in broker queue.", ["queue_name"]),
            ("active_process_count", "Number of active processes in broker queue.", ["queue_name"]),
        ]

        for name, desc, labels in counters:
            self._metrics.create_counter(name, desc, labels)

        for name, desc, labels in gauges:
            self._metrics.create_gauge(name, desc, labels)

        self._metrics.create_histogram(
            "task_runtime", "Histogram of task runtime measurements.", ["name", "hostname", "queue_name"]
        )

    async def update_metrics(self):
        logging.info("starting metrics collection")
        self._app = Celery(broker=self._broker_url)
        self._state = self._app.events.State()

        self._update_worker_metadata()

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
            "worker-online": self._handle_worker_status,
            "worker-offline": self._handle_worker_status,
        }

        asyncio.create_task(self._expire_stale_workers())

        try:
            with self._app.connection() as connection:
                recv = self._app.events.Receiver(connection, handlers=handlers)
                loop = asyncio.get_event_loop()

                while not self._shutdown_event.is_set():
                    try:
                        await loop.run_in_executor(None, lambda: recv.capture(limit=None, timeout=1, wakeup=True))
                    except socket.timeout:
                        continue
                    except (KeyboardInterrupt, SystemExit):
                        self._shutdown_event.set()
                        raise
                    except Exception as e:
                        if not self._shutdown_event.is_set():
                            logging.exception(f"Error in update_metrics: {e}")
                    await asyncio.sleep(1)
        finally:
            logging.info("metrics collection stopped")

    async def run(self):
        logging.info("starting Roquefort")
        try:
            if not self._server_started:
                await self._server.run()
                self._server_started = True
            await self.update_metrics()
        finally:
            self._shutdown_event.set()
            logging.info("Roquefort stopped")

    def _update_worker_metadata(self):
        queues = self._get_active_queues()
        for worker_name, queue_info_list in queues.items():
            if worker_name not in self._workers_metadata:
                self._workers_metadata[worker_name] = {"queues": [], "last_seen": time.time()}
            for queue in queue_info_list:
                name = queue.get("name")
                if name and name not in self._workers_metadata[worker_name]["queues"]:
                    self._workers_metadata[worker_name]["queues"].append(name)
            self._workers_metadata[worker_name]["last_seen"] = time.time()

    def _handle_worker_heartbeat(self, event):
        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)
        self._update_worker_metadata()

        if worker_name in self._workers_metadata:
            self._workers_metadata[worker_name]["last_seen"] = time.time()

        queues = self._workers_metadata.get(worker_name, {}).get("queues", ["unknown"])

        self._metrics.set_gauge(
            "worker_active",
            True,
            labels={"hostname": hostname, "worker": worker_name, "queue_name": format_queue_names(queues)},
        )

    def _handle_worker_status(self, event):
        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)
        event_type = event.get("type")

        if event_type == "worker-online":
            self._update_worker_metadata()
            self._workers_metadata[worker_name]["last_seen"] = time.time()
            value = 1
        else:
            value = 0

        queue_name = get_queue_name_from_worker_metadata(hostname, self._workers_metadata) or self._default_queue_name

        self._metrics.set_gauge(
            "worker_active",
            value,
            labels={"hostname": hostname, "worker": worker_name, "queue_name": queue_name},
        )

    def _handle_task_generic(self, event, task, metric_name, labels):
        self._metrics.increment_counter(metric_name, labels)

    def _handle_task_sent(self, event):
        task = self._get_task_from_event(event)
        labels = {
            "name": event.get("name"),
            "hostname": event.get("hostname"),
            "queue_name": event.get("queue") or self._default_queue_name,
        }
        self._handle_task_generic(event, task, "task_sent", labels)

    def _handle_task_received(self, event):
        task = self._get_task_from_event(event)
        hostname = event.get("hostname")
        worker, _ = get_worker_names(hostname)
        queue = getattr(task, "queue") or get_queue_name_from_worker_metadata(hostname, self._workers_metadata) or self._default_queue_name
        labels = {
            "name": event.get("name"),
            "worker": worker,
            "hostname": hostname,
            "queue_name": queue,
        }
        self._handle_task_generic(event, task, "task_received", labels)

    def _handle_task_started(self, event): self._handle_task_lifecycle(event, "task_started")
    def _handle_task_succeeded(self, event): self._handle_task_lifecycle(event, "task_succeeded")
    def _handle_task_failed(self, event): self._handle_task_lifecycle(event, "task_failed", include_exception=True)
    def _handle_task_retried(self, event): self._handle_task_lifecycle(event, "task_retried", include_exception=True)
    def _handle_task_rejected(self, event): self._handle_task_lifecycle(event, "task_rejected")
    def _handle_task_revoked(self, event): self._handle_task_lifecycle(event, "task_revoked")

    def _handle_task_lifecycle(self, event, metric_name, include_exception=False):
        task = self._get_task_from_event(event)
        hostname = event.get("hostname")
        worker, _ = get_worker_names(hostname)
        queue = getattr(task, "queue") or get_queue_name_from_worker_metadata(hostname, self._workers_metadata) or self._default_queue_name
        labels = {
            "name": getattr(task, "name"),
            "worker": worker,
            "hostname": hostname,
            "queue_name": queue,
        }
        if include_exception:
            exception = getattr(task, "exception", None) or event.get("exception")
            labels["exception"] = get_exception_name(exception)
        self._handle_task_generic(event, task, metric_name, labels)

    async def _expire_stale_workers(self, check_interval: int = 10):
        while not self._shutdown_event.is_set():
            now = time.time()
            expired = []
            for worker_name, metadata in list(self._workers_metadata.items()):
                last_seen = metadata.get("last_seen", 0)
                if now - last_seen > self._worker_ttl:
                    logging.info(f"Expiring metrics for inactive worker: {worker_name}")
                    self._purge_worker_metrics(worker_name)
                    expired.append(worker_name)
            for w in expired:
                self._workers_metadata.pop(w, None)
            await asyncio.sleep(check_interval)

    def _purge_worker_metrics(self, worker_name: str):
        try:
            for metric in self._metrics.get_all_metrics():
                for sample in metric.collect()[0].samples:
                    if sample.labels.get("worker") == worker_name:
                        try:
                            metric.remove(*(sample.labels[k] for k in metric._labelnames))
                        except Exception:
                            pass  # Metric type may not support remove
        except Exception as e:
            logging.error(f"Error purging metrics for {worker_name}: {e}")

    def _get_active_queues(self):
        return self._app.control.inspect().active_queues() or {}

    def _get_task_from_event(self, event):
        self._state.event(event)
        return self._state.tasks.get(event.get("uuid"))