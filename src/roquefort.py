import asyncio
import logging
from pprint import pp, pformat, pprint
import time
import threading
from typing import Callable, Dict, Any
from queue import Queue
from celery import Celery, Task
from fastapi import FastAPI
from celery.events import EventReceiver as Receiver

from .helpers import (
    format_queue_names,
    get_exception_name,
    get_queue_length,
    get_queue_name_from_worker_metadata,
    get_worker_names,
    is_valid_transport,
)

from .metrics.metrics import MetricService
from .server.server import FastAPIServer, HttpServer


class ThreadManager:
    """Manages threads for event handlers and background tasks."""
    
    def __init__(self):
        self.threads: Dict[str, threading.Thread] = {}
        self.shutdown_event = threading.Event()
        self.event_queues: Dict[str, Queue] = {}
        self.running = False
        self.initialization_complete = threading.Event()
        
    def start_event_handler_thread(self, event_type: str, handler_func: Callable, roquefort_instance):
        """Start a thread for a specific event handler."""
        # Create a dedicated queue for this event type
        self.event_queues[event_type] = Queue()
        
        def event_handler_worker():
            logging.debug(f"Starting event handler thread for {event_type}")
            while not self.shutdown_event.is_set():
                try:
                    # Get event from this handler's queue with timeout
                    try:
                        event = self.event_queues[event_type].get(timeout=1.0)
                    except:
                        continue
                        
                    try:
                        handler_func(event)
                    except Exception as e:
                        logging.error(f"Error in {event_type} handler: {e}")
                        
                    self.event_queues[event_type].task_done()
                except Exception as e:
                    logging.error(f"Error in event handler thread for {event_type}: {e}")
                    
        thread = threading.Thread(target=event_handler_worker, name=f"handler-{event_type}")
        thread.daemon = True
        self.threads[event_type] = thread
        thread.start()
        logging.info(f"Started event handler thread for {event_type}")
        
    def start_background_thread(self, name: str, target_func: Callable, *args, **kwargs):
        """Start a background thread for tasks like purging or queue length calculation."""
        def background_worker():
            logging.debug(f"Starting background thread: {name}")
            while not self.shutdown_event.is_set():
                try:
                    target_func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Error in background thread {name}: {e}")
                    
        thread = threading.Thread(target=background_worker, name=name)
        thread.daemon = True
        self.threads[name] = thread
        thread.start()
        logging.info(f"Started background thread: {name}")
        
    def start_event_consumer_thread(self, roquefort_instance):
        """Start the event consumer thread."""
        def event_consumer_worker():
            logging.debug("Starting event consumer thread")
            
            def distribute_event(event):
                """Distribute event to the appropriate handler queue."""
                event_type = event.get("type")
                if event_type in self.event_queues:
                    self.event_queues[event_type].put(event)
                else:
                    logging.warning(f"No handler queue for event type: {event_type}")
            
            handlers = {
                "task-sent": distribute_event,
                "task-received": distribute_event,
                "task-started": distribute_event,
                "task-succeeded": distribute_event,
                "task-failed": distribute_event,
                "task-retried": distribute_event,
                "task-rejected": distribute_event,
                "task-revoked": distribute_event,
                "worker-heartbeat": distribute_event,
                "worker-online": distribute_event,
                "worker-offline": distribute_event,
            }
            
            while not self.shutdown_event.is_set():
                try:
                    with roquefort_instance._app.connection() as connection:
                        recv = roquefort_instance._app.events.Receiver(connection, handlers=handlers)
                        recv.capture(limit=None, timeout=1.0, wakeup=True)
                except Exception as e:
                    logging.error(f"Error in event consumer: {e}")
                    time.sleep(1)
                    
        thread = threading.Thread(target=event_consumer_worker, name="event-consumer")
        thread.daemon = True
        self.threads["event-consumer"] = thread
        thread.start()
        logging.info("Started event consumer thread")
        
    def mark_initialization_complete(self):
        """Mark that all threads have been initialized."""
        self.running = True
        self.initialization_complete.set()
        logging.info("Thread manager initialization complete")
        
    def wait_for_initialization(self, timeout=10):
        """Wait for initialization to complete."""
        return self.initialization_complete.wait(timeout)
        
    def shutdown(self):
        """Shutdown all threads gracefully."""
        logging.info("Shutting down thread manager")
        self.shutdown_event.set()
        self.running = False
        
        # Wait for all threads to finish
        for name, thread in self.threads.items():
            if thread.is_alive():
                logging.debug(f"Waiting for thread {name} to finish")
                thread.join(timeout=5.0)
                if thread.is_alive():
                    logging.warning(f"Thread {name} did not finish gracefully")
                    
        self.threads.clear()
        self.event_queues.clear()
        self.initialization_complete.clear()
        logging.info("Thread manager shutdown complete")
        
    def is_running(self) -> bool:
        """Check if thread manager is running and threads are alive."""
        return self.running and any(thread.is_alive() for thread in self.threads.values())


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
            labels=["name", "hostname", "queue_name"],
        )

    def _purger(self):
        """Purge metrics for inactive workers."""
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
        """Start metrics collection with threading."""
        logging.info("starting metrics collection")
        
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

        # Start event consumer thread
        self._thread_manager.start_event_consumer_thread(self)

        # Start event handler threads
        handler_mapping = {
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

        for event_type, handler_func in handler_mapping.items():
            self._thread_manager.start_event_handler_thread(event_type, handler_func, self)

        # Start background threads
        self._thread_manager.start_background_thread("purger", self._purger_loop)
        self._thread_manager.start_background_thread("queue-length", self._calculate_queue_length_loop)

        # Mark initialization as complete
        self._thread_manager.mark_initialization_complete()

        logging.info("metrics collection started")

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

        if not self._should_monitor_queue(queue_name):
            return
            
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
        task: Task = self._get_task_from_event(event)
        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(
                event.get("hostname"), self._workers_metadata
            )
            or self._default_queue_name
        )
        
        if not self._should_monitor_queue(queue_name):
            return

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
        task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
    
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
        task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )
        
        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
        task_name = getattr(task, "name")
        runtime = event.get("runtime")

        self._handle_task_generic(
            event=event,
            task=task,
            metric_name="task_succeeded",
            labels={
                "name": task_name,
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
                    "name": task_name,
                    "hostname": hostname,
                    "queue_name": queue_name,
                },
            )
        except Exception as e:
            logging.error(f"error setting task_runtime metric: {e}")
                

    def _handle_task_failed(self, event):
        task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
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
        task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
            
        worker_name, _ = get_worker_names(hostname)
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
        task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
    
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
        task: Task = self._get_task_from_event(event)

        hostname = event.get("hostname")
        queue_name = (
            getattr(task, "queue", None)
            or get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )

        if not self._should_monitor_queue(queue_name):
            return
        
        worker_name, _ = get_worker_names(hostname)
        
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
            logging.error(f"error setting worker_active metric: {e}")

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
            logging.error(f"error setting worker_tasks_active metric: {e}")
        
        
        # todo: add metrics handling for processed tasks.

    def _handle_worker_status(self, event):
        event_type = event.get("type")
        hostname = event.get("hostname")
        worker_name, _ = get_worker_names(hostname)

        logging.debug(f"received event {event_type} for worker {hostname}")

        if event_type == "worker-online" and hostname not in self._workers_metadata:
            self._load_worker_metadata(hostname)

        queue_name = (
            get_queue_name_from_worker_metadata(hostname, self._workers_metadata)
            or self._default_queue_name
        )
        
        if not self._should_monitor_queue(queue_name):
            return
            
        value = 0

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
                
                if queue_name not in self._queues:
                    self._queues.append(queue_name)

                if queue_name not in self._workers_metadata[worker_name]["queues"]:
                    self._workers_metadata[worker_name]["queues"].append(queue_name)
                    
    def _should_monitor_queue(self, queue_name: str) -> bool:
        return self._monitored_queues is None or queue_name in self._monitored_queues
