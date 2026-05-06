import json

from scripts.demo_fallback_publisher import build_demo_trade


def test_build_demo_trade_uses_demo_exchange() -> None:
    payload = json.loads(build_demo_trade("BTC-USD", 3))
    trade = payload["events"][0]["trades"][0]

    assert payload["exchange"] == "demo"
    assert trade["exchange"] == "demo"
    assert trade["product_id"] == "BTC-USD"
    assert trade["trade_id"].startswith("demo-BTC-USD-")
