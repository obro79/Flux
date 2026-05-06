import asyncio
from types import SimpleNamespace

from services.api.routes.health import health
from services.api.routes.markets import markets


def test_health_returns_ok() -> None:
    assert asyncio.run(health()) == {"status": "ok"}


def test_markets_returns_supported_public_surface(monkeypatch) -> None:
    monkeypatch.setenv("SUPPORTED_EXCHANGES", "coinbase,kraken,demo")
    monkeypatch.setenv("SUPPORTED_PRODUCTS", "BTC-USD,ETH-USD")
    request = SimpleNamespace(base_url="https://api.example.com/")

    response = asyncio.run(markets(request))

    assert response["exchanges"] == ["coinbase", "kraken", "demo"]
    assert response["products"] == ["BTC-USD", "ETH-USD"]
    assert response["resolutions"] == ["1m"]
    assert response["examples"]["candles"] == (
        "https://api.example.com/candles/BTC-USD/1m?exchange=demo&limit=3"
    )
    assert response["examples"]["crypto_websocket"] == (
        "wss://api.example.com/crypto/BTC-USD?exchange=demo"
    )
