import asyncio
import sys


async def run_all():
    producer = await asyncio.create_subprocess_exec(
        "uv", "run", "services/ingestion/main.py",
        stdout=sys.stdout, stderr=sys.stderr,
    )
    consumer = await asyncio.create_subprocess_exec(
        "uv", "run", "services/consumer/main.py",
        stdout=sys.stdout, stderr=sys.stderr,
    )
    api = await asyncio.create_subprocess_exec(
        "uv", "run", "uvicorn", "services.api.main:app", "--reload",
        stdout=sys.stdout, stderr=sys.stderr,
    )
    await asyncio.gather(producer.wait(), consumer.wait(), api.wait())


if __name__ == "__main__":
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        print("\nShutting down...")
