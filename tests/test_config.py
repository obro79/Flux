from services.config import csv_env, kafka_bootstrap_servers


def test_csv_env_ignores_empty_items(monkeypatch) -> None:
    monkeypatch.setenv("SUPPORTED_PRODUCTS", "BTC-USD, ETH-USD,,")

    assert csv_env("SUPPORTED_PRODUCTS", "SOL-USD") == ["BTC-USD", "ETH-USD"]


def test_kafka_bootstrap_servers_uses_env(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    assert kafka_bootstrap_servers() == "kafka:29092"
