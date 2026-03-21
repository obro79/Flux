import json
import asyncio
import aiokafka


async def consumer():
    consumer = aiokafka.AIOKafkaConsumer(
        "market_trades",
        bootstrap_servers="localhost:9092",
        group_id="market_trades",
        value_deserializer=lambda x: json.loads(x) if x else None,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for message in consumer:
            print(f"Received: {message.value} at offset {message.offset}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consumer())
