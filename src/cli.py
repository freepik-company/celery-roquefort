import asyncio
import click
import json
import sys
import os
from typing import Dict, Any
from .roquefort import Roquefort


def parse_custom_labels(ctx, param, value):
    """Parse custom labels from string format key1=value1,key2=value2 or JSON"""
    if not value:
        return {}

    # Try to parse as JSON first
    if value.startswith("{") and value.endswith("}"):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            raise click.BadParameter(f"Invalid JSON format: {value}")

    # Parse as key=value pairs
    labels = {}
    try:
        for pair in value.split(","):
            if "=" not in pair:
                raise click.BadParameter(
                    f"Invalid label format: {pair}. Use key=value format"
                )
            key, val = pair.split("=", 1)
            labels[key.strip()] = val.strip()
        return labels
    except Exception as e:
        raise click.BadParameter(f"Error parsing custom labels: {e}")


@click.command()
@click.option(
    "--env-prefix",
    default="CR_",
    envvar="CR_ENV_PREFIX",
    help="Environment variable prefix (default: CR_). Can be overridden with CR_ENV_PREFIX env var",
)
@click.option(
    "--broker-url",
    "-b",
    required=True,
    envvar="CR_BROKER_URL",
    help="Celery broker URL (e.g., redis://localhost:6379/0, amqp://guest@localhost//). Env: CR_BROKER_URL",
)
@click.option(
    "--host",
    "-h",
    default="0.0.0.0",
    envvar="CR_HOST",
    help="HTTP server host address (default: 0.0.0.0). Env: CR_HOST",
)
@click.option(
    "--port",
    "-p",
    default=8000,
    type=int,
    envvar="CR_PORT",
    help="HTTP server port (default: 8000). Env: CR_PORT",
)
@click.option(
    "--prefix",
    default="roquefort_",
    envvar="CR_PREFIX",
    help="Metric name prefix (default: roquefort_). Env: CR_PREFIX",
)
@click.option(
    "--custom-labels",
    "-l",
    callback=parse_custom_labels,
    envvar="CR_CUSTOM_LABELS",
    help='Custom labels in format key1=value1,key2=value2 or JSON format {"key": "value"}. Env: CR_CUSTOM_LABELS',
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    envvar="CR_VERBOSE",
    help="Enable verbose logging. Env: CR_VERBOSE=1",
)
@click.option(
    "--default-queue-name",
    "-q",
    default="unknown-queue",
    envvar="CR_DEFAULT_QUEUE_NAME",
    help="Default queue name. Env: CR_DEFAULT_QUEUE_NAME",
)
@click.version_option(version="0.1.0", prog_name="roquefort")
def main(
    env_prefix: str,
    broker_url: str,
    host: str,
    port: int,
    prefix: str,
    custom_labels: Dict[str, Any],
    verbose: bool,
    default_queue_name: str = "unknown-queue",
):
    """
    Celery Roquefort - Prometheus metrics collector for Celery tasks and workers.

    'Cause celery always taste better with roquefort.

    This tool monitors your Celery workers and queues, providing detailed metrics
    about task execution, worker health, and queue performance via Prometheus.

    Environment Variables:

    All CLI options can be set via environment variables with the prefix CR_ (configurable):

    \b
    CR_BROKER_URL     - Celery broker URL
    CR_HOST           - HTTP server host
    CR_PORT           - HTTP server port
    CR_PREFIX         - Metric name prefix
    CR_CUSTOM_LABELS  - Custom labels (key=value,key2=value2 or JSON)
    CR_VERBOSE        - Enable verbose logging (set to 1)
    CR_ENV_PREFIX     - Change the environment variable prefix

    Examples:

    \b
    # Basic usage with Redis
    roquefort --broker-url redis://localhost:6379/0

    \b
    # Using environment variables
    export CR_BROKER_URL=redis://localhost:6379/0
    export CR_HOST=127.0.0.1
    export CR_PORT=9090
    roquefort

    \b
    # With custom host and port
    roquefort -b redis://localhost:6379/0 -h 127.0.0.1 -p 9090

    \b
    # With custom labels
    roquefort -b redis://localhost:6379/0 -l environment=prod,service=api

    \b
    # With JSON custom labels
    roquefort -b redis://localhost:6379/0 -l '{"env": "production", "team": "backend"}'

    \b
    # Using custom environment prefix
    export API_BROKER_URL=redis://localhost:6379/0
    roquefort --env-prefix API_ --broker-url will-be-overridden
    """

    # Show environment variable information if verbose
    if verbose:
        import logging

        logging.basicConfig(level=logging.DEBUG)
        click.echo(f"Environment prefix: {env_prefix}")
        click.echo(f"Starting Roquefort with broker: {broker_url}")
        click.echo(f"Server will run on {host}:{port}")
        if custom_labels:
            click.echo(f"Custom labels: {custom_labels}")

        # Show which values came from environment
        env_vars_used = []
        if os.getenv(f"{env_prefix}BROKER_URL"):
            env_vars_used.append(f"{env_prefix}BROKER_URL")
        if os.getenv(f"{env_prefix}HOST"):
            env_vars_used.append(f"{env_prefix}HOST")
        if os.getenv(f"{env_prefix}PORT"):
            env_vars_used.append(f"{env_prefix}PORT")
        if os.getenv(f"{env_prefix}PREFIX"):
            env_vars_used.append(f"{env_prefix}PREFIX")
        if os.getenv(f"{env_prefix}CUSTOM_LABELS"):
            env_vars_used.append(f"{env_prefix}CUSTOM_LABELS")
        if os.getenv(f"{env_prefix}VERBOSE"):
            env_vars_used.append(f"{env_prefix}VERBOSE")
        if os.getenv(f"{env_prefix}DEFAULT_QUEUE_NAME"):
            env_vars_used.append(f"{env_prefix}DEFAULT_QUEUE_NAME")

        if env_vars_used:
            click.echo(f"Using environment variables: {', '.join(env_vars_used)}")

    try:
        roquefort = Roquefort(
            broker_url=broker_url,
            host=host,
            port=port,
            prefix=prefix,
            custom_labels=custom_labels,
            default_queue_name=default_queue_name,
        )

        click.echo("🧀 Starting Roquefort metrics collector...")
        click.echo(f"📊 Metrics available at: http://{host}:{port}/metrics")
        click.echo(f"🏥 Health check at: http://{host}:{port}/health-check")
        click.echo("Press Ctrl+C to stop")

        asyncio.run(roquefort.run())

    except (KeyboardInterrupt, SystemExit):
        click.echo("\n👋 Roquefort stopped gracefully")
        sys.exit(0)
    except Exception as e:
        click.echo(f"❌ Error starting Roquefort: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
