from services.consumer.models import MarketTradeMessage


def test_market_trade_message_parses_coinbase_market_trades_payload() -> None:
    payload = {
        "channel": "market_trades",
        "exchange": "coinbase",
        "timestamp": "2026-04-13T22:37:22.952905057Z",
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "exchange": "coinbase",
                        "product_id": "BTC-USD",
                        "trade_id": "1000273158",
                        "price": "74214",
                        "size": "0.0173",
                        "time": "2026-04-13T22:37:22.888019Z",
                        "side": "BUY",
                    }
                ],
            }
        ],
    }

    message = MarketTradeMessage(**payload)

    assert message.channel == "market_trades"
    assert message.exchange == "coinbase"
    assert len(message.events) == 1
    trade = message.events[0].trades[0]
    assert trade.exchange == "coinbase"
    assert trade.product_id == "BTC-USD"
    assert trade.trade_id == "1000273158"
    assert trade.price == 74214.0
    assert trade.size == 0.0173
    assert trade.side == "BUY"
    assert trade.time.isoformat() == "2026-04-13T22:37:22.888019+00:00"
