# Real-Time Crypto Data Pipeline

> Live cryptocurrency market data ingestion, streaming aggregation, and technical indicator computation.

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
        CB[Coinbase WebSocket]
    end

    subgraph Ingestion
        ING[Ingestion Service]
    end

    subgraph Broker
        K[[Kafka — market_trades]]
    end

    subgraph Consumers
        TC[Ticker Consumer]
        IC[Indicator Engine]
    end

    subgraph Storage
        PG[(PostgreSQL)]
        RD[(Redis)]
    end

    subgraph API
        REST[GET /candles]
        WS[WS /indicators]
    end

    CB -- trades --> ING
    ING -- publish --> K
    K -- consume --> TC
    K -- consume --> IC
    TC -- OHLCV candles --> PG
    IC -- SMA / RSI / EMA --> RD
    PG --> REST
    RD --> WS
```

## Features

- **Real-time ingestion** — WebSocket connection to Coinbase for BTC-USD, ETH-USD, SOL-USD
- **Stream processing** — Kafka-backed pipeline with independent consumer groups
- **OHLCV candle aggregation** — 1-minute candles built from raw trades with a 5-second grace window for slightly late trades
- **Streaming indicators** — Running SMA, RSI, and EMA computed per-product and published to Redis
- **REST + WebSocket API** — Query historical candles or subscribe to live indicator updates
- **Pluggable exchange adapters** — Abstract base class for adding new exchanges

## Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| Ingestion | `websockets`, `aiokafka` | Connect to exchange feeds, publish to Kafka |
| Messaging | Apache Kafka (KRaft) | Decouple ingestion from processing |
| Processing | `asyncio`, custom consumers | Candle aggregation, indicator computation |
| Storage | PostgreSQL | Persistent candle storage |
| Cache | Redis | Real-time indicator values |
| API | FastAPI, Uvicorn | REST endpoints + WebSocket streaming |
| Infra | Docker Compose | Kafka, Redis, Kafka UI |

## Quick Start

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Install dependencies
uv sync

# 3. Set environment variables
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
export REDIS_URL="redis://localhost:6379"

# 4. Run all services
uv run run.py
```

Kafka UI is available at `localhost:8888`.
Grafana is available at `localhost:3000`.
Prometheus is available at `localhost:9090`.

## Local Validation Flow

```bash
# infrastructure
docker compose up -d

# services (in separate terminals)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres uv run services/ingestion/main.py
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres uv run services/consumer/main.py
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres uv run uvicorn services.api.main:app --reload

# checks
curl http://127.0.0.1:8000/candles/BTC-USD/1m?limit=3
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/candles/{product_id}/{resolution}` | Historical OHLCV candles |
| `WS` | `/indicators/{product_id}` | Live indicator stream (SMA, RSI, EMA) |

## Project Structure

```
services/
├── ingestion/          # Exchange WebSocket → Kafka
│   └── exchanges/      # Coinbase, Binance, Kraken adapters
├── consumer/           # Kafka → Postgres + Redis
│   ├── ticker_consumer.py
│   ├── indicator_consumer.py
│   └── Indicators.py   # RunningSMA, RunningRSI, RunningEMA
├── database/           # psycopg2 Postgres wrapper
└── api/                # FastAPI REST + WebSocket
    └── routes/
```
