"""
CLI consumer script to read and display cost events from Kafka.
Usage:
    python scripts/run_consumer.py
    python scripts/run_consumer.py --max-events 20
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from streaming.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO, format="%(message)s")
# Suppress low-level kafka-python internal network logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger("streamcost.consumer")


def run_consumer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    max_events: int = 0,
    from_beginning: bool = False
) -> int:
    """
    Connects to Kafka and continuously prints consumed events.

    Args:
        bootstrap_servers: Kafka bootstrap servers address.
        topic: Kafka topic to consume.
        max_events: Stop after consuming this many events (0 for infinite).
        from_beginning: If True, reads from earliest offset.

    Returns:
        int: Number of events consumed.
    """
    offset_reset = "earliest" if from_beginning else "latest"
    group_id = f"streamcost-consumer-{int(time.time())}"

    logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
    logger.info(f"Listening on topic '{topic}' (offset: {offset_reset})...")
    logger.info("Press Ctrl+C to stop consumer.\n")

    timeout_ms = 10000 if max_events > 0 else float("inf")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset=offset_reset,
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=timeout_ms
        )
    except KafkaError as e:
        logger.error(f"Failed to connect consumer to Kafka: {e}")
        return 0

    count = 0
    try:
        for message in consumer:
            event = message.value
            count += 1

            provider = event.get("provider", "UNKNOWN")
            service = event.get("service", "UNKNOWN")
            region = event.get("region", "UNKNOWN")
            cost = event.get("cost", 0.0)
            usage = event.get("usage", 0.0)
            unit = event.get("usage_unit", "")
            is_anomaly = event.get("is_synthetic_anomaly", False)
            timestamp = event.get("timestamp", "")

            cost_str = f"${cost:.2f}" if cost >= 0.01 else f"${cost:.4f}"

            if is_anomaly:
                logger.info(f"[ANOMALY DETECTED] #{count:03d} | {provider} | {service} | {region} | {cost_str} ({usage} {unit}) | ID: {event.get('event_id', '')[:8]}")
            else:
                logger.info(f"[EVENT #{count:03d}] {timestamp} | {provider} | {service} | {region} | {cost_str} ({usage} {unit})")

            if max_events > 0 and count >= max_events:
                logger.info(f"\nReached max_events limit of {max_events}. Stopping.")
                break

    except KeyboardInterrupt:
        logger.info("\nShutdown signal (Ctrl+C) received. Stopping consumer...")
    finally:
        consumer.close()
        logger.info(f"Consumer closed. Total events received: {count}")

    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamCost Kafka Event Consumer")
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after consuming N events (0 for infinite)"
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from beginning of topic instead of latest"
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers override"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC,
        help="Kafka topic override"
    )

    args = parser.parse_args()

    run_consumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        max_events=args.max_events,
        from_beginning=args.from_beginning
    )


if __name__ == "__main__":
    main()
