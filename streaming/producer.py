"""
Kafka producer for streaming synthetic multi-cloud cost events.
"""
import json
import logging
import sys
import time
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

from streaming.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    EVENT_INTERVAL_SECONDS,
    ANOMALY_PROBABILITY,
    MAX_EVENTS,
)
from streaming.generator import generate_event
from streaming.models import CloudCostEvent

# Configure standard logger
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger("streamcost.producer")


def create_producer(bootstrap_servers: Optional[str] = None) -> KafkaProducer:
    """
    Initializes and returns a KafkaProducer instance configured for JSON serialization.

    Args:
        bootstrap_servers: Kafka bootstrap servers address string.

    Returns:
        KafkaProducer: Connected producer instance.
    """
    servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
    try:
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=5,
            request_timeout_ms=10000,
            acks="all"
        )
        return producer
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka at {servers}: {e}")
        raise


def publish_event(producer: KafkaProducer, topic: str, event: CloudCostEvent) -> None:
    """
    Publishes a validated CloudCostEvent to the specified Kafka topic.

    Args:
        producer: Active KafkaProducer.
        topic: Kafka topic name.
        event: CloudCostEvent instance.
    """
    # Key by account_id for partition affinity
    event_dict = json.loads(event.model_dump_json())
    future = producer.send(
        topic,
        key=event.account_id,
        value=event_dict
    )
    future.get(timeout=10)


def format_log_line(event: CloudCostEvent) -> str:
    """Formats event for clean console logging."""
    ts_str = event.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    cost_str = f"${event.cost:.2f}" if event.cost >= 0.01 else f"${event.cost:.4f}"

    if event.is_synthetic_anomaly:
        return f"[ANOMALY GENERATED] {event.provider} | {event.service} | {event.region} | {cost_str} (Usage: {event.usage} {event.usage_unit})"
    return f"[{ts_str}] {event.provider} | {event.service} | {event.region} | {cost_str}"


def run_producer(
    max_events: Optional[int] = None,
    interval: Optional[float] = None,
    anomaly_prob: Optional[float] = None,
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None
) -> int:
    """
    Continuously generates and publishes multi-cloud cost events to Kafka.

    Args:
        max_events: Max events to produce (0 or None means infinite).
        interval: Delay in seconds between events.
        anomaly_prob: Probability of generating an anomalous event.
        bootstrap_servers: Override Kafka servers.
        topic: Override Kafka topic.

    Returns:
        int: Number of events produced.
    """
    target_topic = topic or KAFKA_TOPIC
    wait_sec = interval if interval is not None else EVENT_INTERVAL_SECONDS
    prob = anomaly_prob if anomaly_prob is not None else ANOMALY_PROBABILITY
    limit = max_events if max_events is not None else MAX_EVENTS

    logger.info(f"Connecting to Kafka at {bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS}...")
    producer = create_producer(bootstrap_servers)
    logger.info(f"Starting StreamCost Event Producer on topic '{target_topic}' (interval: {wait_sec}s, anomaly_rate: {prob*100:.1f}%)")
    logger.info("Press Ctrl+C to stop generation.\n")

    produced_count = 0
    try:
        while True:
            # 1. Generate event
            event = generate_event(anomaly_prob=prob)

            # 2. Publish to Kafka
            publish_event(producer, target_topic, event)
            produced_count += 1

            # 3. Print concise log
            logger.info(format_log_line(event))

            # 4. Check if max events reached
            if limit > 0 and produced_count >= limit:
                logger.info(f"\nReached target limit of {limit} events. Stopping.")
                break

            time.sleep(wait_sec)

    except KeyboardInterrupt:
        logger.info("\nShutdown signal (Ctrl+C) received. Stopping producer...")
    finally:
        producer.flush(timeout=5)
        producer.close(timeout=5)
        logger.info(f"Producer closed. Total events produced: {produced_count}")

    return produced_count


if __name__ == "__main__":
    run_producer()
