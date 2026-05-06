import os


def csv_env(name: str, default: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, default).split(",") if item.strip()]


def kafka_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def supported_exchanges() -> list[str]:
    return csv_env("SUPPORTED_EXCHANGES", "coinbase,kraken,demo")


def supported_products() -> list[str]:
    return csv_env("SUPPORTED_PRODUCTS", "BTC-USD,ETH-USD,SOL-USD")
