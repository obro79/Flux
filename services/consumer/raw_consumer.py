import json
import asyncio
import aiokafka


class RawConsumer:
    def __init__(self):
        self.consumer = aiokafka.AIOKafkaConsumer(
            "market_trades",
            bootstrap_servers="localhost:9092",
            group_id="market_trades",
            value_deserializer=lambda x: json.loads(x) if x else None,
            auto_offset_reset="earliest",
        )

    async def run(self):
        await self.consumer.start()
        try:
            async for message in self.consumer:
                print(f"Received: {message.value} at offset {message.offset}")
        finally:
            await self.consumer.stop()


if __name__ == "__main__":
    consumer = RawConsumer()
    asyncio.run(consumer.run())
