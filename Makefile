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
