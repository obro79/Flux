import json
import asyncio
import aiokafka


def calculate_simple_moving_average(
    prices: list[float], window_size: int
) -> list[float]:
    """Calculate the simple moving average for a list of prices."""
    if len(prices) < window_size:
        return []
    sma: list[float] = []
    for i in range(window_size - 1, len(prices)):
        window: list[float] = prices[i - window_size + 1 : i + 1]
        sma.append(sum(window) / window_size)
    return sma


def calculate_exponential_moving_average(
    prices: list[float], window_size: int
) -> list[float]:
    """Calculate the exponential moving average for a list of prices."""
    if len(prices) < window_size:
        return []
    ema: list[float] = []
    multiplier: float = 2 / (window_size + 1)
    ema.append(sum(prices[:window_size]) / window_size)  # Start with SMA
    for price in prices[window_size:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    return ema


def calculate_rsi(prices: list[float], window_size: int) -> list[float]:
    """Calculate the Relative Strength Index (RSI) for a list of prices."""
    if len(prices) < window_size + 1:
        return []
    rsi: list[float] = []
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, len(prices)):
        change: float = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
        if i >= window_size:
            avg_gain: float = sum(gains[-window_size:]) / window_size
            avg_loss: float = sum(losses[-window_size:]) / window_size
            rs: float = avg_gain / avg_loss if avg_loss != 0 else float("inf")
            rsi.append(100 - (100 / (1 + rs)))
    return rsi


async def indicator_engine_consumer():
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
    asyncio.run(indicator_engine_consumer())
