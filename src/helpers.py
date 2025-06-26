import logging
from celery.utils import nodesplit  # type: ignore

def format_queue_names(queue_names: list[str]) -> str:
    """Format a list of queue names into a string.

    Args:
        queue_names (list[str]): A list of queue names.

    Returns:
        str: _description_
    """
    return ",".join(queue_names)


def get_worker_names(name: str) -> tuple[str, str]:
    try:
        workername, hostname = nodesplit(name)
    except Exception as e:
        logging.error(f"error getting worker names from {name}: {e}")
        return "unknown", "unknown"
    return workername, hostname

def get_queue_name_from_worker_metadata(worker: str, metadata: dict) -> str:
    """Get the queue name from the worker metadata.

    Args:
        worker (str): The worker name.
        metadata (dict): The worker metadata.
    """
    if worker not in metadata:
        logging.warning(f"worker {worker} not found in metadata")
        return None
    
    worker_metadata = metadata.get(worker, {})
    
    if not "queues" in worker_metadata:
        logging.warning(f"queues not found in worker metadata for {worker}")
        return None
    
    queues = worker_metadata.get("queues", [])
    
    if len(queues) != 1:
        logging.warning(f"multiple queues found for {worker}: {queues}")
        return None

    return queues[0]
