import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace

from base_consumer import BaseConsumer
from metrics import dlq_messages_total, messages_consumed_total
from models import MarketTradeMessage, Trade


def make_trade(trade_id: str, price: float) -> Trade:
    return Trade(
        exchange="coinbase",
        trade_id=trade_id,
        product_id="BTC-USD",
        price=price,
        size=0.1,
        side="BUY",
        time=datetime(2026, 4, 13, 22, 40, 1, tzinfo=UTC),
    )


def kafka_message(value: bytes | None, offset: int) -> SimpleNamespace:
    return SimpleNamespace(topic="market_trades", offset=offset, value=value)


class FakeKafkaConsumer:
    def __init__(self, messages: list[SimpleNamespace]) -> None:
        self._messages = messages
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def assignment(self) -> set:
        return set()

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self._messages:
            yield message


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self.stopped = False

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.stopped = True

    async def send(self, topic: str, value: bytes) -> None:
        self.sent.append((topic, value))


class RecordingConsumer(BaseConsumer):
    def __init__(self, messages: list[SimpleNamespace], group_id: str) -> None:
        # Skip BaseConsumer.__init__ so no real Kafka clients are created.
        self.group_id = group_id
        self.topic = "market_trades"
        self.consumer = FakeKafkaConsumer(messages)
        self.dlq_producer = FakeProducer()
        self.redis = None
        self._lag_task = None
        self.trades: list[Trade] = []
        self.started_hook = False
        self.stopped_hook = False

    async def process_trade(self, trade: Trade) -> None:
        self.trades.append(trade)

    async def on_start(self) -> None:
        self.started_hook = True

    async def on_stop(self) -> None:
        self.stopped_hook = True


def counter_value(counter, **labels) -> float:
    return counter.labels(**labels)._value.get()


def test_run_fans_out_every_trade_in_a_message() -> None:
    message = MarketTradeMessage.from_trades(
        exchange="coinbase",
        trades=[make_trade("1", 100.0), make_trade("2", 101.0)],
    )
    consumer = RecordingConsumer(
        [kafka_message(message.model_dump_json().encode(), 0)],
        group_id="run_fanout",
    )

    asyncio.run(consumer.run())

    assert [trade.trade_id for trade in consumer.trades] == ["1", "2"]
    assert consumer.dlq_producer.sent == []
    assert counter_value(
        messages_consumed_total,
        consumer_group="run_fanout",
        topic="market_trades",
        exchange="coinbase",
    ) == 1


def test_run_routes_malformed_messages_to_dlq_and_keeps_consuming() -> None:
    good = MarketTradeMessage.from_trades(
        exchange="kraken",
        trades=[make_trade("ok", 100.0)],
    )
    consumer = RecordingConsumer(
        [
            kafka_message(b"not json", 0),
            kafka_message(json.dumps({"exchange": "kraken"}).encode(), 1),
            kafka_message(good.model_dump_json().encode(), 2),
        ],
        group_id="run_dlq",
    )

    asyncio.run(consumer.run())

    assert [trade.trade_id for trade in consumer.trades] == ["ok"]
    dlq_payloads = [json.loads(value) for _, value in consumer.dlq_producer.sent]
    assert [p["original_offset"] for p in dlq_payloads] == [0, 1]
    assert dlq_payloads[0]["exchange"] == "unknown"
    assert dlq_payloads[1]["exchange"] == "kraken"
    assert counter_value(
        dlq_messages_total,
        consumer_group="run_dlq",
        topic="market_trades",
        error_type="ValidationError",
        exchange="kraken",
    ) == 1


def test_run_skips_tombstones_and_runs_lifecycle_hooks() -> None:
    consumer = RecordingConsumer([kafka_message(None, 0)], group_id="run_tombstone")

    asyncio.run(consumer.run())

    assert consumer.trades == []
    assert consumer.dlq_producer.sent == []
    assert consumer.started_hook and consumer.stopped_hook
    assert consumer.consumer.started and consumer.consumer.stopped
    assert consumer.dlq_producer.stopped


class FailingOnceConsumer(RecordingConsumer):
    async def process_trade(self, trade: Trade) -> None:
        if trade.trade_id == "boom":
            raise RuntimeError("redis unavailable")
        await super().process_trade(trade)


def test_run_routes_process_trade_errors_to_dlq_and_keeps_consuming() -> None:
    failing = MarketTradeMessage.from_trades(exchange="coinbase", trades=[make_trade("boom", 1.0)])
    good = MarketTradeMessage.from_trades(exchange="coinbase", trades=[make_trade("after", 2.0)])
    consumer = FailingOnceConsumer(
        [
            kafka_message(failing.model_dump_json().encode(), 0),
            kafka_message(good.model_dump_json().encode(), 1),
        ],
        group_id="run_process_error",
    )

    asyncio.run(consumer.run())

    assert [trade.trade_id for trade in consumer.trades] == ["after"]
    [(topic, raw)] = consumer.dlq_producer.sent
    payload = json.loads(raw)
    assert topic == "market_trades.dlq"
    assert payload["original_offset"] == 0
    assert payload["error_type"] == "RuntimeError"
    assert payload["error"] == "redis unavailable"
    assert payload["exchange"] == "coinbase"
    assert counter_value(
        dlq_messages_total,
        consumer_group="run_process_error",
        topic="market_trades",
        error_type="RuntimeError",
        exchange="coinbase",
    ) == 1
