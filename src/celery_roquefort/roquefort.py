import asyncio
import re
import logging

import time
from typing import List
from celery import Celery, Task

from celery_roquefort.helpers import (
    get_queue_length,
    get_queue_name_from_worker_metadata,
    get_worker_names,
    is_valid_transport,
)

from celery_roquefort.metrics.metrics import MetricService
from celery_roquefort.server.server import FastAPIServer, HttpServer
from celery_roquefort.thread_manager import ThreadManager
from celery_roquefort.models import (
    TaskRetriedCeleryEvent, 
    WorkerHeartbeatCeleryEvent, 
    TaskSentCeleryEvent, 
    TaskReceivedCeleryEvent,
    WorkerStatusCeleryEvent,
)


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
        queue_length_interval: int = 10,
        queues: list[str] = None,
        broker_connection_timeout: int = 10,
    ) -> None:
        self._broker_url = broker_url
        self._metrics: MetricService = MetricService(
            metric_prefix=prefix, custom_labels=custom_labels
        )
        self._server: HttpServer = None
        self._app: Celery = None
        self._state: Celery.events.State = None
        self._thread_manager = ThreadManager()
        self._workers_metadata = {}
        self._tracked_events = []
        self._default_queue_name = default_queue_name
        self._worker_heartbeat_timeout = worker_heartbeat_timeout
        self._queue_length_interval = queue_length_interval
        self._queues = []
        self._monitored_queues = queues
        self._host = host
        self._port = port
        self._last_event_timestamp = None
        self._broker_connection_timeout = broker_connection_timeout

        # Create metrics
        #   Counters
        self._metrics.create_counter(
            "task_sent",
            "Sent when a task message is published.",
            labels=["name", "queue_name"],
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
            labels=["worker", "hostname", "queue_name", "exception"],
        )
        self._metrics.create_counter(
            "task_retried",
            "Sent if the task was retried.",
            labels=["name", "worker", "hostname", "queue_name", "exception"],
        )
        self._metrics.create_counter(
            "task_rejected",
            "Sent if the task was rejected.",
            labels=["worker", "hostname", "queue_name"],
        )
        self._metrics.create_counter(
            "task_revoked",
            "Sent if the task was revoked.",
            labels=["worker", "hostname", "queue_name"],
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
            labels=["hostname", "worker", "queue_name"],
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
            "active_process_count",
            "The number of active processes in broker queue.",
            labels=["queue_name"],
        )
        #   Histograms
        self._metrics.create_histogram(
            "task_runtime",
            "Histogram of task runtime measurements.",
            labels=["hostname", "queue_name"],
        )

    def _purger(self):
        """Purge metrics for inactive workers."""
        logging.debug("purging metrics")

        for worker, metadata in list(self._workers_metadata.items()):
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
                self._metrics.remove_gauge_by_label_value(
                    name="worker_tasks_active",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_sent",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_received",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_started",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_succeeded",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_failed",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_retried",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_rejected",
                    value=hostname,
                )
                self._metrics.remove_counter_by_label_value(
                    name="task_revoked",
                    value=hostname,
                )
                del self._workers_metadata[worker]
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

    def _purger_loop(self):
        """Loop for purging metrics."""
        while not self._thread_manager.shutdown_event.is_set():
            self._purger()
            time.sleep(self._worker_heartbeat_timeout)

    def _calculate_queue_length(self):
        """Calculate queue length for monitored queues."""
        try:
            with self._app.connection() as connection:
                transport = connection.info().get("transport")
                
                if not is_valid_transport(transport):
                    logging.info(f"invalid transport {transport} detected. skipping queue length calculation")
                    return
                
                for queue_name in self._queues:
                    if not self._should_monitor_queue(queue_name):
                        continue
                    
                    length = get_queue_length(connection, transport, queue_name)
                    
                    self._metrics.set_gauge(
                        name="queue_length",
                        value=length,
                        labels={"queue_name": queue_name},
                    )
        except Exception as e:
            logging.error(f"Error calculating queue length: {e}")

    def _calculate_queue_length_loop(self):
        """Loop for calculating queue length."""
        while not self._thread_manager.shutdown_event.is_set():
            self._calculate_queue_length()
            time.sleep(self._queue_length_interval)

    def start_metrics_collection(self):
        """Start metrics collection with unified threading."""
        logging.info("starting metrics collection")
        
        # Track startup time for health check
        self._startup_time = time.time()
        
        # Initialize Celery app
        self._app = Celery(broker=self._broker_url)
        self._state = self._app.events.State()
        
        # Track events
        self._tracked_events = [
            "task-sent", "task-received", "task-started", "task-succeeded", 
            "task-failed", "task-retried", "task-rejected", "task-revoked",
            "worker-heartbeat", "worker-online", "worker-offline"
        ]

        # Load worker metadata
        self._load_worker_metadata()

        # Register all event handlers with the unified thread manager
        self._thread_manager.register_event_handler("task-sent", self._handle_task_sent)
        self._thread_manager.register_event_handler("task-received", self._handle_task_received)
        self._thread_manager.register_event_handler("task-started", self._handle_task_started)
        self._thread_manager.register_event_handler("task-succeeded", self._handle_task_succeeded)
        self._thread_manager.register_event_handler("task-failed", self._handle_task_failed)
        self._thread_manager.register_event_handler("task-retried", self._handle_task_retried)
        self._thread_manager.register_event_handler("task-rejected", self._handle_task_rejected)
        self._thread_manager.register_event_handler("task-revoked", self._handle_task_revoked)
        self._thread_manager.register_event_handler("worker-heartbeat", self._handle_worker_heartbeat)
        self._thread_manager.register_event_handler("worker-online", self._handle_worker_status)
        self._thread_manager.register_event_handler("worker-offline", self._handle_worker_status)

        # Start event consumer thread
        self._thread_manager.start_event_consumer_thread(self)

        # Start the unified event handler thread
        self._thread_manager.start_unified_event_handler_thread()

        # Start background threads (these remain separate as they're different operations)
        self._thread_manager.start_background_thread("purger", self._purger_loop)
        self._thread_manager.start_background_thread("queue-length", self._calculate_queue_length_loop)

        # Mark initialization as complete
        self._thread_manager.mark_initialization_complete()

        logging.info("metrics collection started with unified threading")

    def stop_metrics_collection(self):
        """Stop metrics collection."""
        logging.info("stopping metrics collection")
        self._thread_manager.shutdown()
        logging.info("metrics collection stopped")

    def create_fastapi_server(self):
        """Create FastAPI server with lifespan integration."""
        self._server = FastAPIServer(
            host=self._host,
            port=self._port,
            registry=self._metrics.get_registry(),
            lifespan_method=self._lifespan_handler,
            health_check_method=self._health_check,
        )
        return self._server

    async def _lifespan_handler(self):
        """Lifespan handler for FastAPI server."""
        logging.info("Starting Roquefort lifespan")
        
        # Start metrics collection synchronously
        try:
            self.start_metrics_collection()
            
            # Wait for initialization to complete
            if not self._thread_manager.wait_for_initialization(timeout=10):
                raise Exception("Thread manager initialization timed out")
                
            logging.info("All threads initialized successfully")
            
            # Keep running until shutdown
            while True:
                if not self._thread_manager.is_running():
                    logging.warning("Thread manager stopped unexpectedly")
                    break
                await asyncio.sleep(1)
                
        except Exception as e:
            logging.error(f"Error in lifespan handler: {e}")
        finally:
            logging.info("Stopping Roquefort lifespan")
            self.stop_metrics_collection()

    async def _health_check(self):
        """Health check method for FastAPI server."""
        if not self._thread_manager.is_running():
            raise Exception("Thread manager is not running")
        
        # Check if Celery connection is working
        try:
            with self._app.connection() as connection:
                connection.ensure_connection()
        except Exception as e:
            raise Exception(f"Celery connection failed: {e}")
        
        # Check if events are being received regularly
        if self._last_event_timestamp is not None:
            time_since_last_event = time.time() - self._last_event_timestamp
            if time_since_last_event > 60:
                raise Exception(f"No events received for {time_since_last_event:.1f} seconds (threshold: 60 seconds)")
        else:
            # If no events have been received yet, check how long since startup
            # Allow some time for initialization
            startup_time = time.time() - getattr(self, '_startup_time', 0)
            if startup_time > 120:  # Allow 2 minutes for first event after startup
                raise Exception("No events received since startup")

    async def run(self):
        """Run the Roquefort server."""
        logging.info("starting Roquefort")
        
        try:
            server = self.create_fastapi_server()
            await server.run()
        except (KeyboardInterrupt, SystemExit):
            logging.info("shutdown signal received, stopping Roquefort gracefully")
            self.stop_metrics_collection()
        except Exception as e:
            logging.exception(f"fatal error in run: {e}")
            self.stop_metrics_collection()
            raise
        finally:
            logging.info("Roquefort stopped")

    def _handle_task_generic(
        self, event: dict, metric_name: str, task: Task=None, labels: dict = {}
    ):
        event_type = event.get("type")

        logging.debug(f"event of type {event_type} received")

        # Update last event timestamp
        self._last_event_timestamp = time.time()

        if event_type not in self._tracked_events:
            logging.warning(
                f"event {event_type} not tracked. Will be processed as {metric_name}"
            )

        try:
            self._metrics.increment_counter(name=metric_name, labels=labels)
        except Exception as e:
            logging.exception(f"error setting {metric_name} metric: {e}")

    def _handle_task_sent(self, event):

        logging.debug(f"task sent event: {event}")
        try:
            task_sent_event = TaskSentCeleryEvent(**event)
            logging.debug(f"task sent event parsed: {task_sent_event}")
        except Exception as e:
            logging.warning(f"failed to parse task sent event: {e}")
            logging.error(f"event: {event}")

        if not self._should_monitor_queue(task_sent_event.get_queue_name(self._workers_metadata, self._default_queue_name)):
            return

        self._handle_task_generic(
            event=event,
            metric_name="task_sent",
            labels=task_sent_event.get_labels(self._workers_metadata, self._default_queue_name),
        )

    def _handle_task_received(self, event):

        task_received_event = TaskReceivedCeleryEvent(**event)
        logging.info(f"task received event parsed: {task_received_event}")

        queue_name = task_received_event.get_queue_name(self._workers_metadata, self._default_queue_name)

        if not self._should_monitor_queue(queue_name):
            logging.info(f"task received event not monitored: {queue_name}")
            return

        self._handle_task_generic(
            event=event,
            metric_name="task_received",
            labels=task_received_event.get_labels(self._workers_metadata, self._default_queue_name),
        )

    def _handle_task_started(self, event):

        hostname = event.get("hostname")
        queue_name = (
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
    
        self._handle_task_generic(
            event=event,
            metric_name="task_started",
            labels={
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )

    def _handle_task_succeeded(self, event):
        logging.info(f"task succeeded event: {event}")
        
        hostname = event.get("hostname")
        queue_name = (
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
        runtime = event.get("runtime")

        self._handle_task_generic(
            event=event,
            metric_name="task_succeeded",
            labels={
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )
        
        try:
            self._metrics.register_histogram(
                name="task_runtime",
                value=runtime,
                labels={
                    "hostname": hostname,
                    "queue_name": queue_name,
                },
            )
        except Exception as e:
            logging.exception(f"error setting task_runtime metric: {e}")

    def _handle_task_failed(self, event):
        logging.info(f"task failed event: {event}")

        hostname = event.get("hostname")
        queue_name = (
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
        exception = event.get("exception")
        exception_re = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\(")


        def get_exception_name(message: str) -> str:
            match = exception_re.match(message)
            if match:
                return match.group(1)
            return "unknown"

        self._handle_task_generic(
            event=event,
            metric_name="task_failed",
            labels={
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
                "exception": get_exception_name(exception),
            },
        )

    def _handle_task_retried(self, event):
        try:
            task_retried_event = TaskRetriedCeleryEvent(**event)
            logging.info(f"task retried event parsed: {task_retried_event}")
        except Exception as e:
            logging.warning(f"failed to parse task retried event: {e}")

        if not self._should_monitor_queue(task_retried_event.get_queue_name(self._workers_metadata, self._default_queue_name)):
            return
            
        worker_name, _ = get_worker_names(task_retried_event.hostname)
    
        self._handle_task_generic(
            event=event,
            metric_name="task_retried",
            labels={
                "name": task_retried_event.name,
                "worker": worker_name,
                "hostname": task_retried_event.hostname,
                "queue_name": task_retried_event.get_queue_name(self._workers_metadata, self._default_queue_name),
                "exception": task_retried_event.get_exception_name(),
            },
        )

    def _handle_task_rejected(self, event):
        # task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            # getattr(task, "queue", None) or
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
    
        self._handle_task_generic(
            event=event,
            # task=task,
            metric_name="task_rejected",
            labels={
                # "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )

    def _handle_task_revoked(self, event):
        # task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            # getattr(task, "queue", None) or
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
        
        self._handle_task_generic(
            event=event,
             # task=task,
            metric_name="task_revoked",
            labels={
                # "name": getattr(task, "name"),
                "worker": worker_name,
                "hostname": hostname,
                "queue_name": queue_name,
            },
        )

    def _handle_worker_heartbeat(self, event):
        
        
        try:
            heartbeat_event = WorkerHeartbeatCeleryEvent(**event)
            logging.info(f"worker heartbeat event parsed: {heartbeat_event}")
        except Exception as e:
            logging.warning(f"failed to parse worker heartbeat event: {e}")
            
        logging.debug(f"worker heartbeat received from {event.get('hostname')}")

        # Update last event timestamp
        self._last_event_timestamp = time.time()

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
        
        if not self._should_monitor_queue(queue_name):
            return
        
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
            logging.exception(f"error setting worker_active metric: {e}")

        active_tasks = event.get("active", 0)
        
        try: 
            self._metrics.set_gauge(
                name="worker_tasks_active",
                value=active_tasks,
                labels={
                    "hostname": hostname,
                    "worker": worker_name,
                    "queue_name": queue_name,
                },
            )
        except Exception as e:
            logging.exception(f"error setting worker_tasks_active metric: {e}")
        
        
        # todo: add metrics handling for processed tasks.

    def _handle_worker_status(self, event):
        logging.info(f"worker status event: {event}")
        
        try:
            worker_status_event = WorkerStatusCeleryEvent(**event)
            logging.info(f"worker status event parsed: {worker_status_event}")
        except Exception as e:
            logging.warning(f"failed to parse worker status event: {e}")

        logging.debug(
            f"received event {worker_status_event.event_type} for worker {worker_status_event.worker_name}")

        # Update last event timestamp
        self._last_event_timestamp = time.time()

        if (
            worker_status_event.event_type == "worker-online" and 
            worker_status_event.hostname not in self._workers_metadata
        ):
            self._load_worker_metadata(worker_status_event.hostname)

        if not self._should_monitor_queue(worker_status_event.get_queue_name(self._workers_metadata, self._default_queue_name)):
            return

        try:
            self._metrics.set_gauge(
                name="worker_active",
                value=worker_status_event.value,
                labels=worker_status_event.get_labels(self._workers_metadata, self._default_queue_name),
            )
        except Exception as e:
            logging.exception(f"error setting worker_active metric in worker status handler: {e}")

    def _get_task_from_event(self, event) -> Task:
        self._state.event(event)
        return self._state.tasks.get(event.get("uuid"))

    def _load_worker_metadata(self, hostname: str = None) -> None:
        if hostname and hostname in self._workers_metadata:
            return

        destination = [hostname] if hostname else None 

        try:
            queues = self._app.control.inspect(destination=destination).active_queues() or {}

            for worker_name, queue_info_list in queues.items():
                if worker_name not in self._workers_metadata:
                    self._workers_metadata[worker_name] = {"queues": [], "last_heartbeat": time.time()}

                for queue_info in queue_info_list:
                    queue_name = queue_info.get("name")

                    if not queue_name:
                        continue
                    
                    if queue_name not in self._queues:
                        self._queues.append(queue_name)

                    if queue_name not in self._workers_metadata[worker_name]["queues"]:
                        self._workers_metadata[worker_name]["queues"].append(queue_name)
        except Exception as e:
            logging.exception(f"error loading worker metadata for hostname {hostname}: {e}")
                    
    def _should_monitor_queue(self, queue_name: str) -> bool:
        return self._monitored_queues is None or queue_name in self._monitored_queues
