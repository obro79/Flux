from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from models import Trade
from ticker_consumer import CandleAggregator, TickerConsumer, minute_bucket


def trade_at(
    timestamp: str,
    *,
    product_id: str = "BTC-USD",
    trade_id: str = "1",
    price: float = 100.0,
    size: float = 1.0,
    side: str = "BUY",
) -> Trade:
    return Trade(
        trade_id=trade_id,
        product_id=product_id,
        price=price,
        size=size,
        side=side,
        time=datetime.fromisoformat(timestamp.replace("Z", "+00:00")),
    )


class FakeDatabase:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, datetime], dict[str, object]] = {}

    def insert_candle(self, candle: dict[str, object]) -> None:
        key = (str(candle["product_id"]), candle["timestamp"])
        self._rows.setdefault(key, candle.copy())

    def disconnect(self) -> None:
        return None

    @property
    def rows(self) -> list[dict[str, object]]:
        return list(self._rows.values())


def test_minute_bucket_normalizes_to_utc() -> None:
    timestamp = datetime.fromisoformat("2026-04-13T15:39:42-07:00")
    assert minute_bucket(timestamp) == datetime(2026, 4, 13, 22, 39, tzinfo=timezone.utc)


def test_candle_aggregator_separates_products_and_minutes() -> None:
    aggregator = CandleAggregator()
    aggregator.add_trade(trade_at("2026-04-13T22:39:10Z", product_id="BTC-USD", trade_id="1", price=10, size=1))
    aggregator.add_trade(trade_at("2026-04-13T22:39:40Z", product_id="ETH-USD", trade_id="2", price=20, size=2))
    aggregator.add_trade(trade_at("2026-04-13T22:40:01Z", product_id="BTC-USD", trade_id="3", price=11, size=3))

    candles = aggregator.flush_all()

    assert [(c["product_id"], c["timestamp"]) for c in candles] == [
        ("BTC-USD", datetime(2026, 4, 13, 22, 39, tzinfo=timezone.utc)),
        ("BTC-USD", datetime(2026, 4, 13, 22, 40, tzinfo=timezone.utc)),
        ("ETH-USD", datetime(2026, 4, 13, 22, 39, tzinfo=timezone.utc)),
    ]


def test_out_of_order_trades_use_trade_times_for_open_and_close() -> None:
    aggregator = CandleAggregator()
    aggregator.add_trade(trade_at("2026-04-13T22:39:20Z", trade_id="2", price=105, size=2))
    aggregator.add_trade(trade_at("2026-04-13T22:39:05Z", trade_id="1", price=100, size=1))
    aggregator.add_trade(trade_at("2026-04-13T22:39:50Z", trade_id="3", price=103, size=3))

    candle = aggregator.flush_all()[0]

    assert candle["open"] == 100
    assert candle["high"] == 105
    assert candle["low"] == 100
    assert candle["close"] == 103
    assert candle["volume"] == 6


def test_flush_ready_waits_for_grace_period() -> None:
    aggregator = CandleAggregator(grace_period=timedelta(seconds=5))
    aggregator.add_trade(trade_at("2026-04-13T22:39:59Z", trade_id="1", price=100, size=1))

    assert aggregator.flush_ready(now=datetime(2026, 4, 13, 22, 40, 4, tzinfo=timezone.utc)) == []

    candles = aggregator.flush_ready(
        now=datetime(2026, 4, 13, 22, 40, 5, tzinfo=timezone.utc)
    )
    assert len(candles) == 1
    assert candles[0]["timestamp"] == datetime(2026, 4, 13, 22, 39, tzinfo=timezone.utc)


def test_late_trade_after_flush_is_effectively_dropped_by_persistence() -> None:
    db = FakeDatabase()
    with patch("ticker_consumer.BaseConsumer.__init__", return_value=None):
        consumer = TickerConsumer(db=db, flush_grace_period=timedelta(seconds=5))

    consumer.aggregator.add_trade(
        trade_at("2026-04-13T22:39:10Z", trade_id="1", price=100, size=1)
    )
    consumer.flush_ready_candles(
        now=datetime(2026, 4, 13, 22, 40, 5, tzinfo=timezone.utc)
    )

    consumer.aggregator.add_trade(
        trade_at("2026-04-13T22:39:20Z", trade_id="2", price=101, size=2)
    )
    consumer.flush_ready_candles(
        now=datetime(2026, 4, 13, 22, 41, 5, tzinfo=timezone.utc)
    )

    assert len(db.rows) == 1
    candle = db.rows[0]
    assert candle["open"] == 100
    assert candle["close"] == 100
    assert candle["volume"] == 1
