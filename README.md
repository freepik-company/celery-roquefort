# celery-roquefort

'Cause celery always taste better with roquefort.

celery-roquefort is a utility that helps projects to fetch and register Celery task metrics using Prometheus. It monitors your Celery workers and queues, providing detailed metrics about task execution, worker health, and queue performance.

## Features

- **Task Metrics**: Monitor task lifecycle (sent, received, started, succeeded, failed, retried, rejected, revoked)
- **Worker Metrics**: Track active workers and their task processing status
- **Queue Metrics**: Monitor queue length and consumer/worker counts
- **Performance Metrics**: Histogram of task runtime measurements
- **HTTP Health Check**: Built-in health check endpoint
- **Prometheus Integration**: Exposes metrics in Prometheus format
- **Custom Labels**: Support for custom metric labels

## Installation

```bash
pip install celery-roquefort
```

## Quick Start

```python
from roquefort import Roquefort

# Initialize with Redis broker
roquefort = Roquefort(
    broker_url="redis://localhost:6379/0",
    host="0.0.0.0",
    port=8001,
    prefix="my_app_",
    custom_labels={"environment": "production", "service": "my-service"}
)

# Start the metrics server
roquefort.run()
```

## Configuration

### Parameters

- `broker_url`: Celery broker URL (required)
- `host`: HTTP server host (default: "0.0.0.0")
- `port`: HTTP server port (default: 8000)
- `prefix`: Metric name prefix (default: "roquefort_")
- `custom_labels`: Dictionary of custom labels to add to all metrics

## Metrics

### Counters
- `task_sent`: Tasks published to the broker
- `task_received`: Tasks received by workers
- `task_started`: Tasks started by workers
- `task_succeeded`: Successfully completed tasks
- `task_failed`: Failed tasks
- `task_retried`: Retried tasks after failure
- `task_rejected`: Rejected tasks
- `task_revoked`: Revoked tasks

### Gauges
- `worker_active`: Number of active workers
- `worker_tasks_active`: Tasks currently being processed
- `queue_length`: Number of tasks in queue
- `active_consumer_count`: Active consumers in broker queue
- `active_worker_count`: Active workers in broker queue
- `active_process_count`: Active processes in broker queue

### Histograms
- `task_runtime`: Task execution time distribution

## Endpoints

- `/metrics`: Prometheus metrics endpoint
- `/health-check`: Service health status

## Usage with Docker

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "celery_roquefort.cli"]
```

## Development

### Requirements

- Python ≥ 3.9
- Celery ≥ 5.5.3
- FastAPI ≥ 0.115.13
- Redis ≥ 6.2.0

### Dependencies

The project uses:
- **Celery**: Task queue integration
- **FastAPI**: HTTP server for metrics endpoint
- **Prometheus Client**: Metrics collection and exposition
- **Redis**: Default broker support
- **Click**: Command-line interface
- **Uvicorn**: ASGI server

## License

This project is licensed under the MIT License.
