.PHONY: run infra down test lint clean smoke smoke-kafka prod-up prod-down prod-logs prod-smoke

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

prod-up:
	docker compose -f docker-compose.prod.yaml --env-file .env.production up -d --build

prod-down:
	docker compose -f docker-compose.prod.yaml --env-file .env.production down

prod-logs:
	docker compose -f docker-compose.prod.yaml --env-file .env.production logs -f --tail=100

prod-smoke:
	. ./.env.production; curl -fsS "https://$${API_DOMAIN}/health"; curl -fsS "https://$${API_DOMAIN}/markets"
