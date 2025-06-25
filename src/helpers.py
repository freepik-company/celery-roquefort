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
    workername, hostname = nodesplit(name)
    return workername, hostname

def get_queue_name_from_worker_metadata(worker: str, metadata: dict) -> str:
    """Get the queue name from the worker metadata.

    Args:
        worker (str): The worker name.
        metadata (dict): The worker metadata.
    """
    return get_worker_names(metadata.get(worker, {}).get("queues", []))
