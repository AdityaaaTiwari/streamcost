"""
Configuration settings for StreamCost streaming components.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root if present
_project_root = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=_project_root / ".env")

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "cloud-cost-events")

EVENT_INTERVAL_SECONDS: float = float(os.getenv("EVENT_INTERVAL_SECONDS", "1.0"))
ANOMALY_PROBABILITY: float = float(os.getenv("ANOMALY_PROBABILITY", "0.03"))
MAX_EVENTS: int = int(os.getenv("MAX_EVENTS", "0"))
