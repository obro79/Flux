import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from aiokafka import AIOKafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.kafka_smoke import build_trade_payload


ROOT = Path(__file__).resolve().parent.parent
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/postgres"
REDIS_URL = "redis://localhost:6379"
PRODUCT_ID = "BTC-USD"
EXCHANGE = "coinbase"


class SmokeError(RuntimeError):
    pass


def request_json(url: str, timeout: float = 5) -> tuple[int, object]:
    with urlopen(url, timeout=timeout) as response:
        return response.status, json.loads(response.read().decode())


def request_text(url: str, timeout: float = 5) -> tuple[int, str]:
    with urlopen(url, timeout=timeout) as response:
        return response.status, response.read().decode()


def wait_for(description: str, check, timeout: float = 30, interval: float = 1):
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (HTTPError, URLError, OSError, SmokeError) as exc:
            last_error = exc
        time.sleep(interval)
    if last_error:
        raise SmokeError(f"Timed out waiting for {description}: {last_error}")
    raise SmokeError(f"Timed out waiting for {description}")


def require_compose_infra() -> None:
    result = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "--format", "json"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    running = set()
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        service = json.loads(line).get("Service")
        if service:
            running.add(service)

    required = {"postgres", "redis", "kafka", "prometheus"}
    missing = sorted(required - running)
    if missing:
        raise SmokeError(
            "Docker compose infra is not running. Missing: "
            + ", ".join(missing)
            + ". Run `docker compose up -d` first."
        )


def start_process(args: list[str]) -> subprocess.Popen:
    env = {
        **os.environ,
        "DATABASE_URL": DATABASE_URL,
        "REDIS_URL": REDIS_URL,
    }
    return subprocess.Popen(
        args,
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


async def emit_payloads(payloads: list[bytes]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()
    try:
        for payload in payloads:
            await producer.send_and_wait("market_trades", payload)
    finally:
        await producer.stop()


def prometheus_targets_up(*jobs: str) -> bool:
    _, payload = request_json("http://127.0.0.1:9090/api/v1/targets")
    active_targets = payload["data"]["activeTargets"]
    health_by_job = {
        target["labels"].get("job"): target.get("health")
        for target in active_targets
    }
    return all(health_by_job.get(job) == "up" for job in jobs)


def dlq_total() -> float:
    _, metrics = request_text("http://127.0.0.1:8002/metrics")
    total = 0.0
    for line in metrics.splitlines():
        if line.startswith("dlq_messages_total{"):
            total += float(line.rsplit(" ", 1)[1])
    return total


def candles_available() -> list[dict]:
    status, payload = request_json(
        f"http://127.0.0.1:8000/candles/{PRODUCT_ID}/1m?exchange={EXCHANGE}&limit=3"
    )
    if status != 200 or not isinstance(payload, list):
        return []
    return [row for row in payload if row.get("exchange") == EXCHANGE]


def unsupported_resolution_is_400() -> bool:
    try:
        request_json(
            f"http://127.0.0.1:8000/candles/{PRODUCT_ID}/5m?exchange={EXCHANGE}",
            timeout=5,
        )
    except HTTPError as exc:
        return exc.code == 400
    return False


async def run_smoke() -> None:
    require_compose_infra()

    consumer = start_process(["uv", "run", "services/consumer/main.py"])
    api = start_process(["uv", "run", "uvicorn", "services.api.main:app", "--port", "8000"])
    try:
        wait_for("consumer metrics", lambda: request_text("http://127.0.0.1:8002/metrics"))
        wait_for("API metrics", lambda: request_text("http://127.0.0.1:8000/metrics"))

        baseline_dlq = dlq_total()
        now = datetime.now(timezone.utc)
        payloads = [
            build_trade_payload(
                exchange=EXCHANGE,
                product_id=PRODUCT_ID,
                index=index,
                age_seconds=70 + (index * 60),
                now=now,
            )
            for index in range(3)
        ]
        await emit_payloads(payloads)

        candles = wait_for("persisted smoke candles", candles_available, timeout=45)
        if len(candles) < 1:
            raise SmokeError("Expected at least one persisted smoke candle row")

        if not unsupported_resolution_is_400():
            raise SmokeError("Unsupported candle resolution did not return HTTP 400")

        await emit_payloads([b'{"exchange":"unknown","broken":true'])
        wait_for(
            "DLQ metric increment",
            lambda: dlq_total() > baseline_dlq,
            timeout=30,
        )

        wait_for(
            "Prometheus API and consumer targets up",
            lambda: prometheus_targets_up("api", "consumer"),
            timeout=45,
            interval=2,
        )

        print(
            "local integration smoke passed: "
            f"candles={len(candles)}, dlq_before={baseline_dlq}, dlq_after={dlq_total()}"
        )
    finally:
        stop_process(api)
        stop_process(consumer)


if __name__ == "__main__":
    try:
        asyncio.run(run_smoke())
    except SmokeError as exc:
        print(f"local integration smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
