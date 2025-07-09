import logging
import threading
import time
from typing import Callable, Dict, Any
from queue import Queue


class ThreadManager:
    """Manages threads for event handlers and background tasks."""
    
    def __init__(self):
        self.threads: Dict[str, threading.Thread] = {}
        self.shutdown_event = threading.Event()
        self.event_queue = Queue()  # Single queue for all events
        self.event_handlers: Dict[str, Callable] = {}  # Map event types to handlers
        self.running = False
        self.initialization_complete = threading.Event()
        
    def register_event_handler(self, event_type: str, handler_func: Callable):
        """Register an event handler for a specific event type."""
        self.event_handlers[event_type] = handler_func
        logging.debug(f"Registered handler for event type: {event_type}")
        
    def start_unified_event_handler_thread(self):
        """Start a single thread to handle all event types."""
        def unified_event_handler_worker():
            logging.debug("Starting unified event handler thread")
            while not self.shutdown_event.is_set():
                try:
                    # Get event from the shared queue with timeout
                    try:
                        event = self.event_queue.get(timeout=1.0)
                    except:
                        continue
                        
                    # Get event type and find appropriate handler
                    event_type = event.get("type")
                    if event_type in self.event_handlers:
                        try:
                            handler_func = self.event_handlers[event_type]
                            handler_func(event)
                        except Exception as e:
                            logging.exception(f"Error in {event_type} handler: {e}")
                    else:
                        logging.warning(f"No handler registered for event type: {event_type}")
                        
                    self.event_queue.task_done()
                except Exception as e:
                    logging.exception(f"Error in unified event handler thread: {e}")
                    
        thread = threading.Thread(target=unified_event_handler_worker, name="unified-event-handler")
        thread.daemon = True
        self.threads["unified-event-handler"] = thread
        thread.start()
        logging.info("Started unified event handler thread")
        
    def start_background_thread(self, name: str, target_func: Callable, *args, **kwargs):
        """Start a background thread for tasks like purging or queue length calculation."""
        def background_worker():
            logging.debug(f"Starting background thread: {name}")
            while not self.shutdown_event.is_set():
                try:
                    target_func(*args, **kwargs)
                except Exception as e:
                    logging.exception(f"Error in background thread {name}: {e}")
                    
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
                """Distribute event to the unified event queue."""
                try:
                    self.event_queue.put(event)
                except Exception as e:
                    logging.exception(f"Error distributing event {event}: {e}")
            
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
                    # Simple approach: recreate the whole receiver when it fails
                    with roquefort_instance._app.connection() as connection:
                        recv = roquefort_instance._app.events.Receiver(connection, handlers=handlers)
                        recv.capture(limit=None, timeout=1.0, wakeup=True)
                except Exception as e:
                    # Any error including kombu warnings will cause recreation
                    logging.warning(f"event receiver disconnected, recreating: {e} - {type(e)}")
                    time.sleep(1)
                    
            logging.info("Event consumer thread stopped")
                    
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
                try:
                    thread.join(timeout=5.0)
                    if thread.is_alive():
                        logging.warning(f"Thread {name} did not finish gracefully")
                except Exception as e:
                    logging.exception(f"Error while shutting down thread {name}: {e}")
                    
        self.threads.clear()
        self.event_handlers.clear()
        self.initialization_complete.clear()
        logging.info("Thread manager shutdown complete")
        
    def is_running(self) -> bool:
        """Check if thread manager is running and threads are alive."""
        return self.running and any(thread.is_alive() for thread in self.threads.values()) 