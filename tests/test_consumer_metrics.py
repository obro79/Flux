import asyncio

from aiokafka import TopicPartition

from base_consumer import BaseConsumer
from metrics import consumer_lag
from models import Trade


class DummyConsumer(BaseConsumer):
    async def process_trade(self, trade: Trade) -> None:
        return None


class FakeKafkaConsumer:
    def __init__(self) -> None:
        self._assignments = {
            TopicPartition("market_trades", 0),
            TopicPartition("market_trades", 1),
        }
        self._end_offsets = {
            TopicPartition("market_trades", 0): 10,
            TopicPartition("market_trades", 1): 3,
        }
        self._positions = {
            TopicPartition("market_trades", 0): 7,
            TopicPartition("market_trades", 1): 3,
        }

    def assignment(self) -> set[TopicPartition]:
        return self._assignments

    async def end_offsets(
        self,
        assignments: set[TopicPartition],
    ) -> dict[TopicPartition, int]:
        return {assignment: self._end_offsets[assignment] for assignment in assignments}

    async def position(self, partition: TopicPartition) -> int:
        return self._positions[partition]


def test_update_lag_metrics_uses_end_offsets_and_positions() -> None:
    consumer = DummyConsumer.__new__(DummyConsumer)
    consumer.group_id = "test_group"
    consumer.consumer = FakeKafkaConsumer()

    asyncio.run(consumer.update_lag_metrics())

    assert (
        consumer_lag.labels(
            consumer_group="test_group",
            topic="market_trades",
            partition="0",
        )._value.get()
        == 3
    )
    assert (
        consumer_lag.labels(
            consumer_group="test_group",
            topic="market_trades",
            partition="1",
        )._value.get()
        == 0
    )
