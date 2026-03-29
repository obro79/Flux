from prometheus_client import Counter, Gauge

messages_consumed_total = Counter(
    "messages_consumed_total", "Total messages consumed from Kafka"
)
redis_writes_total = Counter(
    "redis_writes_total", "Total writes to Redis"
)
dlq_messages_total = Counter(
    "dlq_messages_total", "Total messages sent to dead letter queue"
)
consumer_lag = Gauge(
    "consumer_lag", "Current consumer lag in Kafka"
)
