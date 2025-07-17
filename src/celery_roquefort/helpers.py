import logging
from celery.utils import nodesplit  # type: ignore

UNKNOWN_WORKER_NAME = "unknown"
UNKNOWN_HOSTNAME = "unknown"


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
        return UNKNOWN_WORKER_NAME, UNKNOWN_HOSTNAME
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

    if not queues:
        logging.warning(f"no queues found for {worker}")
        return None

    if len(queues) != 1:
        logging.warning(f"multiple queues found for {worker}: {queues}")
        return None

    return queues[0]


def is_valid_transport(transport: str) -> bool:
    """Check if the transport is valid.

    Args:
        transport (str): The transport name.
    """
    
    # TODO: add more transports
    return transport in ["redis", "rediss", "sentinel"]

def get_queue_length(connection, transport: str, queue_name: str) -> int:
    
    if transport in ["redis", "rediss", "sentinel"]:
        return get_redis_queue_length(connection, queue_name)
    
    return 0

def get_redis_queue_length(connection, queue_name: str) -> int:
    """Get the length of a Redis queue.

    Args:
        connection (Connection): The Redis connection.
        queue_name (str): The name of the queue.
    """
    return connection.default_channel.client.llen(queue_name)