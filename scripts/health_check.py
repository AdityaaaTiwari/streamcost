"""
StreamCost Infrastructure Health Check Script.

Verifies:
1. PostgreSQL database connectivity.
2. Apache Kafka broker health.
3. Existence/creation of the 'cloud-cost-events' topic.
4. End-to-end publishing of a test JSON event to Kafka.
5. Consuming and validating the event from Kafka.
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Ensure stdout handles UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Load environment variables from .env if present
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "streamcost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "streamcost")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "streamcost")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cloud-cost-events")

TEST_EVENT = {
    "timestamp": "2026-09-24T14:00:00Z",
    "provider": "AWS",
    "account_id": "aws-prod-001",
    "service": "EC2",
    "region": "ap-south-1",
    "resource_id": "i-demo001",
    "usage": 1.0,
    "usage_unit": "hours",
    "cost": 0.12,
    "currency": "USD"
}


def check_postgres(max_retries: int = 5, retry_interval: int = 2) -> bool:
    """Checks PostgreSQL connection and executes a validation query."""
    print(f"\n[1/4] Checking PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT} (DB: {POSTGRES_DB})...")
    import psycopg2

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=5
            )
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print(f"  [OK] Connected to PostgreSQL successfully!")
                print(f"    Version: {version}")
            conn.close()
            return True
        except Exception as e:
            print(f"  [Attempt {attempt}/{max_retries}] PostgreSQL connection failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
    return False


def check_and_create_kafka_topic(max_retries: int = 5, retry_interval: int = 2) -> bool:
    """Verifies Kafka broker connectivity and ensures target topic exists."""
    print(f"\n[2/4] Checking Kafka broker at {KAFKA_BOOTSTRAP_SERVERS}...")
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    for attempt in range(1, max_retries + 1):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                client_id="streamcost-healthcheck-admin",
                request_timeout_ms=10000
            )
            cluster_topics = admin_client.list_topics()
            print(f"  [OK] Connected to Kafka broker!")
            print(f"    Existing topics: {cluster_topics}")

            if KAFKA_TOPIC not in cluster_topics:
                print(f"  Creating topic '{KAFKA_TOPIC}'...")
                try:
                    new_topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
                    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
                    print(f"  [OK] Topic '{KAFKA_TOPIC}' created successfully!")
                except TopicAlreadyExistsError:
                    print(f"  [OK] Topic '{KAFKA_TOPIC}' already exists.")
            else:
                print(f"  [OK] Topic '{KAFKA_TOPIC}' already exists.")

            admin_client.close()
            return True
        except Exception as e:
            print(f"  [Attempt {attempt}/{max_retries}] Kafka check failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
    return False


def produce_and_consume_kafka_event() -> bool:
    """Produces a test event to Kafka and consumes it to verify round-trip."""
    print(f"\n[3/4] Producing test cost event to topic '{KAFKA_TOPIC}'...")
    from kafka import KafkaProducer, KafkaConsumer

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=10000
        )
        future = producer.send(KAFKA_TOPIC, value=TEST_EVENT)
        record_metadata = future.get(timeout=10)
        producer.flush()
        producer.close()
        print(f"  [OK] Test event produced successfully!")
        print(f"    Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
    except Exception as e:
        print(f"  [FAIL] Failed to produce event to Kafka: {e}")
        return False

    print(f"\n[4/4] Consuming and verifying test event from topic '{KAFKA_TOPIC}'...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"streamcost-healthcheck-{int(time.time())}",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=10000
        )

        consumed_event = None
        for message in consumer:
            if message.value.get("resource_id") == TEST_EVENT["resource_id"]:
                consumed_event = message.value
                break
        consumer.close()

        if consumed_event:
            print(f"  ✓ Test event consumed and verified successfully!")
            print(f"    Received event: {json.dumps(consumed_event, indent=2)}")
            return True
        else:
            print(f"  [FAIL] Timed out waiting for matching event.")
            return False
    except Exception as e:
        print(f"  ✗ Failed to consume event from Kafka: {e}")
        return False


def run_all_checks() -> dict:
    """Runs all health checks and returns a summary dict."""
    results = {}
    print("=" * 60)
    print("StreamCost Day 1 Infrastructure Health Check")
    print("=" * 60)

    results["PostgreSQL"] = check_postgres()
    results["Kafka Broker"] = check_and_create_kafka_topic()

    if results["Kafka Broker"]:
        results["Kafka Topic"] = True
        results["Kafka E2E Message"] = produce_and_consume_kafka_event()
    else:
        results["Kafka Topic"] = False
        results["Kafka E2E Message"] = False

    print("\n" + "=" * 60)
    print("HEALTH CHECK RESULTS SUMMARY")
    print("=" * 60)
    for service, status in results.items():
        symbol = "PASS" if status else "FAIL"
        print(f"  {service:<25}: {symbol}")
    print("=" * 60)

    all_passed = all(results.values())
    return results, all_passed


if __name__ == "__main__":
    _, success = run_all_checks()
    sys.exit(0 if success else 1)
