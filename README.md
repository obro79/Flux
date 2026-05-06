# Real-Time Crypto Data Pipeline

> Exchange-aware crypto market data ingestion, Kafka fan-out, persistence, and observability.

![Status](https://img.shields.io/badge/status-in%20progress-yellow)
![Python](https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![WebSocket](https://img.shields.io/badge/WebSocket-010101?logo=socketdotio&logoColor=white)

---

## Architecture

```mermaid
flowchart LR
    subgraph Exchanges
        CB[Coinbase]
        KR[Kraken]
    end

    subgraph Ingestion
        ING[Exchange Adapters]
    end

    subgraph Broker
        K[[Kafka: market_trades]]
    end

    subgraph Consumers
        RC[Raw Consumer]
        TC[Ticker Consumer]
        IC[Indicator Engine]
    end

    subgraph Storage
        PG[(PostgreSQL)]
        RD[(Redis)]
    end

    subgraph API
        REST[REST: /candles]
        WS[WebSocket: /crypto and /indicators]
    end

    subgraph Observability
        PR[Prometheus]
        GF[Grafana]
        AL[Alert Rules]
    end

    CB --> ING
    KR --> ING
    ING --> K
    K --> RC
    K --> TC
    K --> IC
    TC --> PG
    IC --> RD
    PG --> REST
    RD --> WS
    ING --> PR
    RC --> PR
    TC --> PR
    IC --> PR
    REST --> PR
    PR --> GF
    PR --> AL
```

Current rollout includes Coinbase and Kraken adapters in the same trade schema. The public API stays stable while storage and metrics carry exchange labels end to end.

## Features

- Exchange-adapter model with Coinbase and Kraken in the same `market_trades` path
- Kafka-backed fan-out for raw events, candle aggregation, and indicator computation
- OHLCV candle aggregation with a short grace window for late trades
- Streaming indicators computed per product and persisted in Redis
- REST and WebSocket API for historical candles and live indicator reads
- Observability built around Prometheus scrape targets, Grafana panels, and alert rules for API down, DLQ non-zero, and consumer lag high

## Tech Stack

| Layer | Technology | Role |
|-------|-------------|------|
| Ingestion | `websockets`, `aiokafka` | Connect to exchange feeds and publish normalized trades |
| Messaging | Apache Kafka (KRaft) | Decouple ingestion from processing |
| Processing | `asyncio`, custom consumers | Candle aggregation, indicator computation, DLQ handling |
| Storage | PostgreSQL | Persistent candle storage |
| Cache | Redis | Real-time indicator values |
| API | FastAPI, Uvicorn | REST endpoints and WebSocket streaming |
| Observability | Prometheus, Grafana | Scrape targets, dashboard panels, and alert evaluation |
| Infra | Docker Compose | Kafka, Redis, Grafana, Prometheus, and Kafka UI |

## Quick Start

```bash
# 1. Start infrastructure with Docker Desktop running
docker compose up -d

# 2. Install dependencies
uv sync

# 3. Configure local runtime values
cp .env.example .env

# 4. Run the full pipeline
uv run run.py
```

Kafka UI is available at `localhost:8888`.
Grafana is available at `localhost:3000`.
Prometheus is available at `localhost:9090`.

Docker Desktop must be running for local Kafka, Redis, Postgres, Prometheus, and Grafana.

## Public Deployment

The production deployment is designed for a single VPS running Docker Compose. Only the API is public; Kafka, Redis, Postgres, Prometheus, and Grafana remain on the private Docker network.

```bash
# On the VPS
git clone https://github.com/obro79/Flux.git
cd Flux
cp .env.production.example .env.production

# Edit API_DOMAIN, ACME_EMAIL, POSTGRES_PASSWORD, DATABASE_URL, and GRAFANA_ADMIN_PASSWORD.
make prod-up
```

Caddy terminates HTTPS for `API_DOMAIN` and proxies to the FastAPI service. The same app image runs the API, live ingestion, consumer, and deterministic `demo` fallback publisher.

Public demo endpoints:

```bash
curl "https://$API_DOMAIN/health"
curl "https://$API_DOMAIN/markets"
curl "https://$API_DOMAIN/candles/BTC-USD/1m?exchange=demo&limit=3"
```

WebSocket examples:

```text
wss://$API_DOMAIN/crypto/BTC-USD?exchange=demo
wss://$API_DOMAIN/indicators/BTC-USD?exchange=demo
```

Live Coinbase and Kraken exchange data is best-effort. The `demo` exchange keeps the public API usable if exchange WebSocket traffic is quiet or unavailable.

## Observability

Prometheus scrapes the ingestion, consumer, and API services on their local metrics ports. Grafana uses those scrape targets to show the pipeline overview dashboard:

- Pipeline throughput by published and consumed message rate
- DLQ volume
- Redis write rate
- API request rate, error rate, and latency
- Consumer lag

Alerting is based on the same Prometheus signals:

- API down
- DLQ non-zero
- Consumer lag high

The metric names are exchange-aware and group-aware so the dashboard can separate Coinbase, Kraken, and consumer group behavior without duplicating panels.

## Local Validation Flow

```bash
# infrastructure
docker compose up -d

# services (in separate terminals)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres REDIS_URL=redis://localhost:6379 uv run services/ingestion/main.py
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres REDIS_URL=redis://localhost:6379 uv run services/consumer/main.py
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres REDIS_URL=redis://localhost:6379 uv run uvicorn services.api.main:app --reload

# deterministic local integration smoke
make smoke
```

`make smoke` expects Docker compose infrastructure to already be running. It starts only the API and consumer subprocesses, emits synthetic backdated Kafka trades, verifies persisted candle rows, checks unsupported resolutions return HTTP 400, confirms malformed traffic increments the DLQ metric, and waits for Prometheus to mark the API and consumer targets as `up`.

Live ingestion remains a separate best-effort validation because exchange WebSocket availability and message volume vary:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres REDIS_URL=redis://localhost:6379 uv run services/ingestion/main.py
curl http://127.0.0.1:8001/metrics
curl http://127.0.0.1:9090/api/v1/targets
```

Validation should cover three things: Prometheus sees ingestion, consumer, and API as `up`; Grafana panels show non-empty series for the active services; and alert rules are loaded and can be forced locally by stopping the API, creating DLQ traffic with `uv run scripts/kafka_smoke.py --malformed`, or building consumer lag.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Runtime health check |
| `GET` | `/markets` | Supported exchanges, products, resolutions, and example URLs |
| `GET` | `/candles/{product_id}/{resolution}` | Historical OHLCV candles; `1m` is currently the only supported resolution |
| `WS` | `/crypto/{product_id}` | Live raw price and indicator stream |
| `WS` | `/indicators/{product_id}` | Live indicator stream (SMA, RSI, EMA) |

## Project Structure

```text
services/
├── ingestion/          # Exchange adapters -> Kafka
│   └── exchanges/      # Coinbase and Kraken adapters
├── consumer/           # Kafka -> Postgres + Redis
│   ├── ticker_consumer.py
│   ├── indicator_consumer.py
│   └── indicators.py   # RunningSMA, RunningRSI, RunningEMA
├── database/           # Postgres wrapper
└── api/                # FastAPI REST + WebSocket
    └── routes/
monitoring/             # Prometheus and Grafana provisioning
tests/                  # Pytest coverage for the runtime path
```
