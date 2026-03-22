import json
import asyncio
from typing import Protocol
import aiokafka
from Indicators import RunningSMA, RunningRSI


class Indicator(Protocol):
    def add(self, price: float) -> float | None: ...


class IndicatorEngineConsumer:
    def __init__(self):
        self.indicators: dict[str, Indicator] = {}

        self.consumer = aiokafka.AIOKafkaConsumer(
            "market_trades",
            bootstrap_servers="localhost:9092",
            group_id="market_trades",
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )

    def add_indicator(self, name: str, indicator):
        self.indicators[name] = indicator

    def add_price(self, price: float) -> dict:
        return {name: ind.add(price) for name, ind in self.indicators.items()}

    async def run(self):
        await self.consumer.start()
        try:
            async for message in self.consumer:
                price = message.value["price"]
                indicators = self.add_price(price)
                print(f"Price: {price} | {indicators}")
        finally:
            await self.consumer.stop()


if __name__ == "__main__":
    engine = IndicatorEngineConsumer()
    asyncio.run(engine.run())
