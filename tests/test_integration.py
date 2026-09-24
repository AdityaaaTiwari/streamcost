"""
Integration tests for PostgreSQL and Kafka services.
"""
import os
import psycopg2
from kafka.admin import KafkaAdminClient
from dotenv import load_dotenv

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "streamcost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "streamcost")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "streamcost")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cloud-cost-events")


def test_postgres_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=5
    )
    with conn.cursor() as cur:
        cur.execute("SELECT 1;")
        res = cur.fetchone()[0]
        assert res == 1
    conn.close()


def test_kafka_topic_exists():
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="streamcost-pytest-admin",
        request_timeout_ms=10000
    )
    topics = admin.list_topics()
    admin.close()
    assert KAFKA_TOPIC in topics
