import re
import logging

from typing import Optional, List, ClassVar, Pattern, Union
from celery import Celery, Task
from fastapi import FastAPI
from celery.events import EventReceiver as Receiver
from pydantic import BaseModel, ConfigDict

from celery.utils import nodesplit  # type: ignore
from celery_roquefort.helpers import (
    get_queue_name_from_worker_metadata,
    get_worker_names,
)

UNKNOWN_EXCEPTION_NAME = "UnknownException"
UNKNOWN_EVENT_TYPE = "unknown"


class BaseCeleryEvent(BaseModel):
    """Base Celery event."""
    hostname: str
    utcoffset: Optional[int] = None
    pid: Optional[int] = None
    clock: Optional[int] = None
    timestamp: Optional[float] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_queue_name(self, workers_metadata: dict, default_queue_name: str) -> str:
        return get_queue_name_from_worker_metadata(
            self.hostname, workers_metadata
        ) or default_queue_name
    
    @property
    def worker_name(self) -> str:
        workername, _ = get_worker_names(self.hostname)
        return workername

    @property
    def hostname(self) -> str:
        _, hostname = get_worker_names(self.hostname)
        return hostname


class TaskRetriedCeleryEvent(BaseCeleryEvent):
    """Task retried event."""
    hostname: str
    name: Optional[str] = None
    utcoffset: Optional[int] = None
    pid: Optional[int] = None
    clock: Optional[int] = None
    uuid: Optional[str] = None
    exception: Optional[str] = None
    traceback: Optional[str] = None
    timestamp: Optional[float] = None
    type: Optional[str] = None
    local_received: Optional[float] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    exception_re: ClassVar[Pattern[str]] = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\(")

    def __get_exception_name(self, message: str) -> str:
        match = self.exception_re.match(message)
        if match:
            return match.group(1)
        return UNKNOWN_EXCEPTION_NAME
    
    def get_exception_name(self) -> str:
        return self.__get_exception_name(self.exception)


class WorkerHeartbeatCeleryEvent(BaseCeleryEvent):
    """Worker heartbeat event."""

    hostname: str
    utcoffset: Optional[int] = None
    pid: Optional[int] = None
    clock: Optional[int] = None
    freq: Optional[float] = None
    active: Optional[int] = None
    processed: Optional[int] = None
    loadavg: Optional[List[float]] = None
    sw_ident: Optional[str] = None
    sw_ver: Optional[str] = None
    sw_sys: Optional[str] = None
    timestamp: Optional[float] = None
    type: Optional[str] = None
    local_received: Optional[float] = None

    def extract_queue_from_worker(self, workers_metadata: dict, default_queue_name: str) -> str:
        
        return get_queue_name_from_worker_metadata(self.hostname, workers_metadata) or default_queue_name


    def get_labels(self, workers_metadata: dict, default_queue_name: str) -> dict:
        return {
            "hostname": self.hostname,
            "worker": self.worker_name,
            "queue_name": self.get_queue_name(workers_metadata, default_queue_name),
        }


class TaskSentCeleryEvent(BaseCeleryEvent):
    """Task sent event."""
    hostname: str
    utcoffset: Optional[int] = None
    pid: Optional[int] = None
    clock: Optional[int] = None
    uuid: Optional[str] = None
    root_id: Optional[str] = None
    parent_id: Optional[str] = None
    name: Optional[str] = None
    args: Optional[str] = None
    kwargs: Optional[str] = None
    retries: Optional[int] = None
    eta: Optional[Union[str, float]] = None
    expires: Optional[Union[str, float]] = None
    queue: Optional[str] = None
    exchange: Optional[str] = None
    routing_key: Optional[str] = None
    timestamp: Optional[float] = None
    type: Optional[str] = None
    local_received: Optional[float] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_labels(self, workers_metadata: dict, default_queue_name: str) -> dict:
        return {
            "name": self.name,
            "queue_name": self.get_queue_name(workers_metadata, default_queue_name),
        }


class TaskReceivedCeleryEvent(BaseCeleryEvent):
    """Task received event."""

    hostname: str
    utcoffset: Optional[int] = None
    pid: Optional[int] = None
    clock: Optional[int] = None
    uuid: Optional[str] = None
    name: Optional[str] = None
    args: Optional[str] = None
    kwargs: Optional[str] = None
    retries: Optional[int] = None
    eta: Optional[Union[str, float]] = None
    expires: Optional[Union[str, float]] = None
    timestamp: Optional[float] = None
    type: Optional[str] = None
    local_received: Optional[float] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_labels(self, workers_metadata: dict, default_queue_name: str) -> dict:
        return {
            "name": self.name,
            "worker": self.worker_name,
            "hostname": self.hostname,
            "queue_name": self.get_queue_name(workers_metadata, default_queue_name),
        }


class WorkerStatusCeleryEvent(BaseCeleryEvent):
    """Worker status event (online/offline)."""
    hostname: str
    utcoffset: Optional[int] = None
    pid: Optional[int] = None
    clock: Optional[int] = None
    freq: Optional[float] = None
    active: Optional[int] = None
    processed: Optional[int] = None
    loadavg: Optional[List[float]] = None
    sw_ident: Optional[str] = None
    sw_ver: Optional[str] = None
    sw_sys: Optional[str] = None
    timestamp: Optional[float] = None
    type: Optional[str] = None
    local_received: Optional[float] = None
    
    @property
    def event_type(self) -> str:
        return self.type or UNKNOWN_EVENT_TYPE

    def get_labels(self, workers_metadata: dict, default_queue_name: str) -> dict:
        return {
            "worker": self.worker_name,
            "hostname": self.hostname,
            "queue_name": self.get_queue_name(workers_metadata, default_queue_name),
        }

    @property
    def value(self) -> int:
        if self.event_type == "worker-online":
            return 1
        else:
            logging.warning(f"worker {self.hostname} went offline")
            return 0
