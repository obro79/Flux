.PHONY: run infra down test lint clean smoke smoke-kafka

run:
	docker compose up -d
	uv run run.py

infra:
	docker compose up -d

down:
	docker compose down

test:
	uv run pytest

lint:
	uv run ruff check .

clean:
	docker compose down -v

test-ws:
	websocat ws://localhost:8000/indicators/BTC-USD

test-api:
	curl "http://localhost:8000/candles/BTC-USD/1m?exchange=coinbase&limit=3"

smoke:
	uv run scripts/local_integration_smoke.py

smoke-kafka:
	uv run scripts/kafka_smoke.py --exchange coinbase --product-id BTC-USD --count 3 --age-seconds 70
