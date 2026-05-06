import json
from datetime import datetime, timezone

from scripts.kafka_smoke import build_trade_payload


def test_build_trade_payload_applies_age_seconds_offset() -> None:
    now = datetime(2026, 4, 13, 22, 40, 10, tzinfo=timezone.utc)

    payload = build_trade_payload(
        exchange="coinbase",
        product_id="BTC-USD",
        index=2,
        age_seconds=70,
        now=now,
    )
    message = json.loads(payload)
    trade = message["events"][0]["trades"][0]

    assert message["timestamp"] == "2026-04-13T22:39:00Z"
    assert trade["time"] == "2026-04-13T22:39:00Z"
    assert trade["trade_id"] == "smoke-2"
