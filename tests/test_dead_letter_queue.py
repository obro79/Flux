import asyncio
import json
from types import SimpleNamespace

from dead_letter_queue import publish_to_dlq


class RecordingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []

    async def send(self, topic: str, value: bytes) -> None:
        self.sent.append((topic, value))


def test_publish_to_dlq_wraps_original_message_with_error_context() -> None:
    producer = RecordingProducer()
    message = SimpleNamespace(topic="market_trades", offset=42, value=b'{"bad": true}')

    asyncio.run(
        publish_to_dlq(producer, message, ValueError("missing field"), exchange="kraken")
    )

    assert len(producer.sent) == 1
    topic, raw = producer.sent[0]
    payload = json.loads(raw)
    assert topic == "market_trades.dlq"
    assert payload["exchange"] == "kraken"
    assert payload["original_topic"] == "market_trades"
    assert payload["original_offset"] == 42
    assert payload["original_value"] == '{"bad": true}'
    assert payload["error"] == "missing field"
    assert payload["error_type"] == "ValueError"
    assert payload["failed_at"]


def test_publish_to_dlq_keeps_undecodable_bytes() -> None:
    producer = RecordingProducer()
    message = SimpleNamespace(topic="market_trades", offset=0, value=b"\xff\xfe")

    asyncio.run(publish_to_dlq(producer, message, ValueError("bad"), exchange="unknown"))

    payload = json.loads(producer.sent[0][1])
    assert payload["original_value"] == "��"
