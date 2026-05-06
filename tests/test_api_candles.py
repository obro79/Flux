import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from services.api.routes.candles import get_candles


class FakeDatabase:
    def get_candles(
        self,
        product_id: str,
        limit: int = 100,
        exchange: str = "coinbase",
    ) -> list[dict[str, object]]:
        return [
            {
                "exchange": exchange,
                "product_id": product_id,
                "timestamp": datetime(2026, 4, 13, 22, 39, tzinfo=timezone.utc),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 0.5,
            }
        ][:limit]


def fake_request() -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(db=FakeDatabase())))


def test_get_candles_returns_1m_rows() -> None:
    rows = asyncio.run(
        get_candles(
            fake_request(),
            product_id="BTC-USD",
            resolution="1m",
            exchange="coinbase",
            limit=3,
        )
    )

    assert rows[0]["exchange"] == "coinbase"
    assert rows[0]["product_id"] == "BTC-USD"


def test_get_candles_rejects_unsupported_resolution() -> None:
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            get_candles(
                fake_request(),
                product_id="BTC-USD",
                resolution="5m",
                exchange="coinbase",
                limit=3,
            )
        )

    assert exc.value.status_code == 400

