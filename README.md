# Flux

> A real-time crypto market data pipeline: live Coinbase and Kraken trades stream through Kafka into 1-minute candles, streaming indicators and a public REST and WebSocket API, with Prometheus and Grafana watching every stage.

[![CI](https://github.com/obro79/Flux/actions/workflows/ci.yml/badge.svg)](https://github.com/obro79/Flux/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white)

<!-- TODO(owen): record a short GIF of the Grafana pipeline dashboard next to a websocat stream, save it as docs/demo.gif, and replace this block with:
![Flux demo](docs/demo.gif)
-->
> **Demo:** _GIF coming soon (Grafana pipeline dashboard and a live indicator stream)._

## What it does

- **Ingests live trades from several exchanges.** Coinbase and Kraken WebSocket feeds are normalized into one schema, so adding an exchange means writing one adapter and nothing downstream changes.
- **Fans out through Kafka.** Three independent consumer groups read the same topic: raw prices, candles and indicators. A slow database never stalls the live stream.
- **Builds correct 1-minute OHLCV candles.** Buckets use event time, handle out-of-order trades, wait out a 5-second grace window for late data, and write idempotently to Postgres.
- **Streams indicators in O(1) per trade.** SMA, EMA and Wilder RSI are kept per exchange and product in Redis and pushed over WebSockets.
- **Isolates bad data.** Malformed messages go to a dead letter queue with full error context instead of crashing a consumer.
- **Is observable end to end.** Per-exchange and per-consumer-group metrics, a provisioned Grafana dashboard, and alerts for API down, DLQ traffic and consumer lag.

## Architecture

Ingestion, consumers and the API are separate processes that share state only through Kafka, Postgres and Redis. Each consumer group keeps its own offsets, so candles, indicators and raw prices can fall behind, replay or scale on their own.

```mermaid
flowchart LR
    subgraph Sources
        CB[Coinbase WS]
        KR[Kraken WS]
        DM[Demo publisher]
    end
    subgraph Consumers["Kafka consumer groups"]
        RC[raw_consumer]
        TC[candle_builder]
        IC[indicator_engine]
    end
    CB & KR --> ING[Ingestion<br/>adapters + normalizers]
    DM --> K
    ING --> K[[Kafka<br/>market_trades]]
    K --> Consumers
    Consumers -. bad messages .-> DLQ[[market_trades.dlq]]
    TC --> PG[(Postgres<br/>candles_1m)]
    RC & IC --> RD[(Redis)]
    PG & RD --> API[FastAPI<br/>REST + WebSocket]
```

Every service exports Prometheus metrics, which feed a provisioned Grafana dashboard and alert rules.

The full design, covering the message schema, candle grace window, DLQ envelope, indicator math, delivery guarantees and known limitations, is in **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.12, `asyncio`, `websockets`, `aiokafka` |
| Messaging | Apache Kafka (KRaft mode) with a dead letter topic |
| Processing | Custom async consumers, Pydantic models, `tenacity` retries |
| Storage | PostgreSQL (candles), Redis (live prices and indicators) |
| API | FastAPI, Uvicorn, REST and WebSockets |
| Observability | Prometheus, Grafana, alert rules, structured JSON logs |
| Infra | Docker Compose, Caddy (TLS), GitHub Actions, `uv`, Ruff |

## Run locally

**Prerequisites:** Docker Desktop (or Docker Engine with Compose), Python 3.12, and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/obro79/Flux.git && cd Flux
cp .env.example .env        # local defaults work out of the box
docker compose up -d        # Kafka, Postgres, Redis, Prometheus, Grafana, Kafka UI
uv sync
uv run run.py               # ingestion + consumers + API
```

Then try it:

```bash
curl "http://localhost:8000/markets"
curl "http://localhost:8000/candles/BTC-USD/1m?exchange=coinbase&limit=3"
websocat "ws://localhost:8000/indicators/BTC-USD?exchange=coinbase"
```

| Service | URL |
|---|---|
| API docs (Swagger) | http://localhost:8000/docs |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Kafka UI | http://localhost:8888 |

**Environment variables** (see [`.env.example`](.env.example)):

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/postgres` | Candle storage |
| `REDIS_URL` | `redis://localhost:6379` | Live prices and indicators |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `SUPPORTED_EXCHANGES` / `SUPPORTED_PRODUCTS` | `coinbase,kraken,demo` / `BTC-USD,ETH-USD,SOL-USD` | Surface advertised by `/markets` |
| `RATE_LIMIT_PER_MINUTE` | `120` | Per-client limit on public routes (`0` disables it) |
| `DEMO_FALLBACK_ENABLED` | `true` | Deterministic `demo` exchange for when live feeds are quiet |

### API

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check |
| `GET` | `/markets` | Supported exchanges, products, resolutions and example URLs |
| `GET` | `/candles/{product_id}/1m?exchange=&limit=` | Historical 1-minute OHLCV candles |
| `WS` | `/crypto/{product_id}?exchange=` | Live price, size and indicators |
| `WS` | `/indicators/{product_id}?exchange=` | Live SMA, EMA and RSI |

### Deploy

Production runs on a single VPS with Docker Compose. Only Caddy (HTTPS) is public, and Kafka, Postgres, Redis, Prometheus and Grafana stay on the private network.

```bash
cp .env.production.example .env.production   # set API_DOMAIN, ACME_EMAIL and passwords
make prod-up
make prod-smoke
```

## Tests

```bash
uv run pytest          # unit tests: no Kafka, Postgres or Redis needed
uv run ruff check .    # lint
make smoke             # end-to-end against the docker compose stack
```

The unit suite covers the logic that decides correctness: exchange normalizers, candle bucketing with late and out-of-order trades, indicator math against hand-computed values, the consumer loop's DLQ routing, and the API routes. `make smoke` publishes synthetic backdated trades through real Kafka, then checks the persisted candles, the API responses, a forced DLQ increment and the Prometheus targets. CI runs lint and tests on every push and pull request.

## Project structure

```text
services/
├── ingestion/       # Exchange adapters and normalizers -> Kafka
├── consumer/        # BaseConsumer, candle builder, indicator engine, DLQ, metrics
├── market_data/     # Shared Pydantic trade schema
├── database/        # Postgres access and idempotent schema setup
└── api/             # FastAPI REST and WebSocket routes
monitoring/          # Prometheus config, alert rules, Grafana provisioning
scripts/             # Demo publisher, Kafka smoke test, end-to-end smoke test
tests/               # Pytest unit suite
docs/                # Architecture
```

## Author

Built by [Owen Fisher](https://github.com/obro79). Released under the [MIT License](LICENSE).
