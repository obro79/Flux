from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass
class KafkaProducerWrapper:
    bootstrap_servers: str = "localhost:9092"
    topic: str = "market-data"

    def __post_init__(self) -> None:
        self._producer: Any = None

    def send(self, value: dict[str, Any], topic: str | None = None) -> None:
        payload = json.dumps(value)
        if self._producer is not None:
            self._producer.send(topic or self.topic, value)
            self._producer.flush()
            return

        # Placeholder for local development until the Kafka client is wired in.
        _ = (topic or self.topic, payload, self.bootstrap_servers)
