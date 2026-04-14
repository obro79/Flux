from prometheus_client import Counter, Gauge

messages_consumed_total = Counter(
    "messages_consumed_total",
    "Total messages consumed from Kafka",
    labelnames=("consumer_group", "topic", "exchange"),
)
redis_writes_total = Counter(
    "redis_writes_total",
    "Total writes to Redis",
    labelnames=("consumer_group", "exchange", "keyspace"),
)
dlq_messages_total = Counter(
    "dlq_messages_total",
    "Total messages sent to dead letter queue",
    labelnames=("consumer_group", "topic", "error_type", "exchange"),
)
consumer_lag = Gauge(
    "consumer_lag",
    "Current consumer lag in Kafka",
    labelnames=("consumer_group", "topic", "partition"),
)
