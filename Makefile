.PHONY: run infra down test lint clean

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
	curl http://localhost:8000/indicators/BTC-USD