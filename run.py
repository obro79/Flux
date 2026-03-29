import asyncio
import logging
import signal
import sys
from pythonjsonlogger import jsonlogger

signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter())
logging.root.setLevel(logging.INFO)
logging.root.addHandler(handler)

logger = logging.getLogger(__name__)


async def run_all():
    producer = await asyncio.create_subprocess_exec(
        "uv",
        "run",
        "services/ingestion/main.py",
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    consumer = await asyncio.create_subprocess_exec(
        "uv",
        "run",
        "services/consumer/main.py",
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    api = await asyncio.create_subprocess_exec(
        "uv",
        "run",
        "uvicorn",
        "services.api.main:app",
        "--reload",
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    await asyncio.gather(producer.wait(), consumer.wait(), api.wait())


if __name__ == "__main__":
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
