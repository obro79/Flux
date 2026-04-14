from services.ingestion.exchanges.normalizers import (
    normalize_coinbase_message,
    normalize_kraken_message,
)


def test_normalize_coinbase_message_builds_shared_schema() -> None:
    payload = {
        "channel": "market_trades",
        "timestamp": "2026-04-13T22:37:22.952905057Z",
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "product_id": "BTC-USD",
                        "trade_id": "1000273158",
                        "price": "74214",
                        "size": "0.0173",
                        "time": "2026-04-13T22:37:22.888019Z",
                        "side": "buy",
                    }
                ],
            }
        ],
    }

    message = normalize_coinbase_message(payload)

    assert message is not None
    assert message.exchange == "coinbase"
    trade = message.events[0].trades[0]
    assert trade.exchange == "coinbase"
    assert trade.side == "BUY"
    assert trade.product_id == "BTC-USD"


def test_normalize_kraken_message_builds_shared_schema() -> None:
    payload = {
        "channel": "trade",
        "type": "update",
        "timestamp": "2026-04-13T22:37:22.952905057Z",
        "symbol": "BTC/USD",
        "data": [
            {
                "symbol": "BTC/USD",
                "side": "sell",
                "trade_id": 42,
                "price": 74214.1,
                "qty": 0.5,
                "timestamp": "2026-04-13T22:37:22.888019Z",
            }
        ],
    }

    message = normalize_kraken_message(
        payload,
        symbol_map={"BTC/USD": "BTC-USD"},
    )

    assert message is not None
    assert message.exchange == "kraken"
    trade = message.events[0].trades[0]
    assert trade.exchange == "kraken"
    assert trade.product_id == "BTC-USD"
    assert trade.trade_id == "42"
    assert trade.side == "SELL"
