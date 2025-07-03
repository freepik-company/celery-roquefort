import asyncio
import logging
from pprint import pp, pformat, pprint
import time
from celery import Celery, Task
from fastapi import FastAPI
from celery.events import EventReceiver as Receiver

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
        worker_heartbeat_timeout: int = 60,
    ) -> None:
        self._broker_url = broker_url
        self._metrics: MetricService = MetricService(
            metric_prefix=prefix, custom_labels=custom_labels
        )
        self._server: HttpServer = HttpServer(
            host=host, port=port, registry=self._metrics.get_registry()
        )
        self._app: Celery = None
        self._state: Celery.events.State = None
        self._shutdown_event = asyncio.Event()
        self._server_started = False
        self._workers_metadata = {}
        self._tracked_events = []
        self._default_queue_name = default_queue_name
        self._worker_heartbeat_timeout = worker_heartbeat_timeout

        # Create metrics
        #   Counters
        self._metrics.create_counter(
            "task_sent",
            "Sent when a task message is published.",
            labels=["name", "hostname", "queue_name"],
        )
        self._metrics.create_counter(
            "task_received",
            "Received when a task message is received.",
            labels=["name", "worker", "hostname", "queue_name"],
        )
        self._metrics.create_counter(
            "task_started",
            "Sent just before a worker runs a task.",
            labels=["name", "worker", "hostname", "queue_name"],
        )
        self._metrics.create_counter(
            "task_succeeded",
            "Sent if the task was executed successfully.",
            labels=["name", "worker", "hostname", "queue_name"],
        )
        self._metrics.create_counter(
            "task_failed",
            "Sent if the task failed.",
            labels=["name", "worker", "hostname", "queue_name", "exception"],
        )
        self._metrics.create_counter(
            "task_retried",
            "Sent if the task was retried.",
            labels=["name", "worker", "hostname", "queue_name", "exception"],
        )
        self._metrics.create_counter(
            "task_rejected",
            "Sent if the task was rejected.",
            labels=["name", "worker", "hostname", "queue_name"],
        )
        self._metrics.create_counter(
            "task_revoked",
            "Sent if the task was revoked.",
            labels=["name", "worker", "hostname", "queue_name"],
        )
        #   Gauges
        self._metrics.create_gauge(
            "worker_active",
            "Number of active workers. It indicates that the worker has recently sent a heartbeat.",
            labels=["hostname", "worker", "queue_name"],
        )
        self._metrics.create_gauge(
            "worker_tasks_active",
            "Number of tasks currently being processed by the workers.",
            labels=["hostname", "queue_name"],
        )
        self._metrics.create_gauge(
            "queue_length", "Number of tasks in the queue.", labels=["queue_name"]
        )
        self._metrics.create_gauge(
            "active_consumer_count",
            "The number of active consumer in broker queue.",
            labels=["queue_name"],
        )
        self._metrics.create_gauge(
            "active_worker_count",
            "The number of active workers in broker queue.",
            labels=["queue_name"],
        )
        self._metrics.create_gauge(
            "active_process_count",
            "The number of active processes in broker queue.",
            labels=["queue_name"],
        )
        #   Histograms
        self._metrics.create_histogram(
            "task_runtime",
            "Histogram of task runtime measurements.",
            labels=["name", "hostname", "queue_name"],
        )

    def _lifespan(self):
        async def lifespan(app: FastAPI):
            asyncio.create_task(self.update_metrics())
            yield

        return lifespan

    async def _purger(self):
        logging.debug("purging metrics")
        workers_to_remove = []

        for worker, metadata in self._workers_metadata.items():
            hostname = worker
            worker_name, _ = get_worker_names(hostname)
            queue_name = (
                metadata["queues"][0]
                if metadata["queues"]
                else self._default_queue_name
            )

            if (
                time.time() - metadata["last_heartbeat"]
                > self._worker_heartbeat_timeout * 5
            ):
                logging.warning(f"purging metrics for worker {worker}")
                self._metrics.remove_gauge_by_label_value(
                    name="worker_active",
                    value=hostname,
                )
                workers_to_remove.append(worker)
                continue

            if (
                time.time() - metadata["last_heartbeat"]
                > self._worker_heartbeat_timeout
            ):
                logging.debug(f"setting worker_active to 0 for worker {worker}")
                self._metrics.set_gauge(
                    name="worker_active",
                    value=0,
                    labels={
                        "hostname": hostname,
                        "worker": worker_name,
                        "queue_name": queue_name,
                    },
                )
                continue

        for worker in workers_to_remove:
            del self._workers_metadata[worker]

    async def _purger_loop(self):
        while not self._shutdown_event.is_set():
            await self._purger()
            await asyncio.sleep(self._worker_heartbeat_timeout)

    async def _consume_events(
        self, receiver: Receiver, loop: asyncio.AbstractEventLoop
    ):
        # Use run_in_executor to avoid blocking the event loop
        await loop.run_in_executor(
            None,
            lambda: receiver.capture(limit=None, timeout=10, wakeup=True),
        )

    async def _consume_events_loop(self, handlers: dict):
        with self._app.connection() as connection:
            recv = self._app.events.Receiver(connection, handlers=handlers)
            loop = asyncio.get_event_loop()
            while not self._shutdown_event.is_set():
                await self._consume_events(recv, loop)
                await asyncio.sleep(1)

    async def update_metrics(self):
        logging.info("starting metrics collection")
        self._app = Celery(broker=self._broker_url)
        self._state = self._app.events.State()
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

        self._tracked_events = list(handlers.keys())

        # Load queue info
        self._load_worker_metadata()

        try:
            consume_task = asyncio.create_task(self._consume_events_loop(handlers))
            purge_task = asyncio.create_task(self._purger_loop())

            await asyncio.wait(
                [consume_task, purge_task], return_when=asyncio.FIRST_COMPLETED
            )

        except (KeyboardInterrupt, SystemExit):
            logging.info("Shutdown signal received, stopping metrics collection")
            self._shutdown_event.set()
        except Exception as e:
            if not self._shutdown_event.is_set():
                logging.exception(f"Fatal error in update_metrics: {e}")
                raise
        finally:
            logging.info("metrics collection stopped")

    async def run(self):
        logging.info("starting Roquefort")

        try:
            # Start HTTP server only once
            if not self._server_started:
                await self._server.run()
                self._server_started = True

            # Start metrics collection
            await self.update_metrics()
        except (KeyboardInterrupt, SystemExit):
            logging.info("shutdown signal received, stopping Roquefort gracefully")
            self._shutdown_event.set()
        except Exception as e:
            logging.exception(f"fatal error in run: {e}")
            self._shutdown_event.set()
            raise
        finally:
            logging.info("Roquefort stopped")

    def _handle_task_generic(
        self, event: dict, task: Task, metric_name: str, labels: dict = {}
    ):
        event_type = event.get("type")

        logging.debug(f"event of type {event_type} received")

        if event_type not in self._tracked_events:
            logging.warning(
                f"event {event_type} not tracked. Will be processed as {metric_name}"
            )

        try:
            self._metrics.increment_counter(name=metric_name, labels=labels)
        except Exception as e:
            logging.error(f"error setting {metric_name} metric: {e}")

    def _handle_task_sent(self, event):
        task: Task = self._get_task_from_event(event)
        queue_name = event.get("queue") or self._default_queue_name

        labels = {
            "name": event.get("name"),
            "hostname": event.get("hostname"),
            "queue_name": queue_name,
        }

        task: Task = self._get_task_from_event(event)
        queue_name = event.get("queue") or self._default_queue_name

        labels = {
            "name": event.get("name"),
            "hostname": event.get("hostname"),
            "queue_name": queue_name,
        }

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_sent",
            labels=labels,
        )

    def _handle_task_received(self, event):
        task = self._get_task_from_event(event)

        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(
                event.get("hostname"), self._workers_metadata
            )
            or self._default_queue_name
        )

        worker_name, _ = get_worker_names(event.get("hostname"))

        labels = {
            "name": event.get("name"),
            "worker": worker_name,
            "hostname": event.get("hostname"),
            "queue_name": queue_name,
        }

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_received",
            labels=labels,
        )

    def _handle_task_started(self, event):
        task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_started",
            labels={
                "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )

    def _handle_task_succeeded(self, event):
        task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_succeeded",
            labels={
                "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )

    def _handle_task_failed(self, event):
        task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        exception = getattr(task, "exception", None) or event.get("exception")
        exception_name = get_exception_name(exception)

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_failed",
            labels={
                "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
                "exception": exception_name,
            },
        )

    def _handle_task_retried(self, event):
        task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        exception = getattr(task, "exception", None) or event.get("exception")
        exception_name = get_exception_name(exception)

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_retried",
            labels={
                "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
                "exception": exception_name,
            },
        )

    def _handle_task_rejected(self, event):
        task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_rejected",
            labels={
                "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )

    def _handle_task_revoked(self, event):
        task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_revoked",
            labels={
                "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )

    def _handle_worker_heartbeat(self, event):
        logging.debug(f"worker heartbeat received from {event.get('hostname')}")

        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        if hostname in self._workers_metadata:
            self._workers_metadata[hostname]["last_heartbeat"] = time.time()
        else:
            self._load_worker_metadata(hostname)

        queue_name = (
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        value = 1

        try:
            self._metrics.set_gauge(
                name="worker_active",
                value=value,
                labels={
                    "hostname": hostname,
                    "worker": worker_name,
                    "queue_name": queue_name,
                },
            )
        except Exception as e:
            logging.error(f"error setting worker_active metric: {e}")

        # todo: add metrics handling for active processes.
        # todo: add metrics handling for processed tasks.

    def _handle_worker_status(self, event):
        event_type = event.get("type")
        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        logging.debug(f"received event {event_type} for worker {hostname}")

        if event_type == "worker-online" and hostname not in self._workers_metadata:
            self._load_worker_metadata(hostname)

        value = 0

        queue_name = (
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if event_type == "worker-online":
            value = 1

        self._metrics.set_gauge(
            name="worker_active",
            value=value,
            labels={
                "hostname": hostname,
                "worker": worker_name,
                "queue_name": queue_name,
            },
        )

    def _get_task_from_event(self, event) -> Task:
        self._state.event(event)
        return self._state.tasks.get(event.get("uuid"))

    def _load_worker_metadata(self, hostname: str = None) -> None:

        if hostname and hostname in self._workers_metadata:
            return

        destination = [hostname] if hostname else None 

        queues = self._app.control.inspect(destination=destination).active_queues() or {}

        for worker_name, queue_info_list in queues.items():
            if worker_name not in self._workers_metadata:
                self._workers_metadata[worker_name] = {"queues": [], "last_heartbeat": time.time()}

            for queue_info in queue_info_list:
                queue_name = queue_info.get("name")

                if not queue_name:
                    continue

                if queue_name not in self._workers_metadata[worker_name]["queues"]:
                    self._workers_metadata[worker_name]["queues"].append(queue_name)


async def main():
    roquefort = Roquefort(
        broker_url="redis://localhost:6379/0",
        host="0.0.0.0",
        port=8001,
        custom_labels={"who_you_gonna_call": "ghostbusters"},
    )
    await roquefort.run()


if __name__ == "__main__":
    asyncio.run(main())
