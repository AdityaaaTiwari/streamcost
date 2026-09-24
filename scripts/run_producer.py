"""
CLI entrypoint to run the StreamCost multi-cloud event producer.
Usage:
    python scripts/run_producer.py
    python scripts/run_producer.py --max-events 50 --interval 0.5 --anomaly-prob 0.1
"""
import argparse
import sys
from pathlib import Path

# Add project root to sys.path so scripts can import from streaming module
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from streaming.config import EVENT_INTERVAL_SECONDS, ANOMALY_PROBABILITY, MAX_EVENTS
from streaming.producer import run_producer


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamCost Multi-Cloud Cost Event Generator")
    parser.add_argument(
        "--max-events",
        type=int,
        default=MAX_EVENTS,
        help="Maximum events to generate (0 for infinite, default from .env or 0)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=EVENT_INTERVAL_SECONDS,
        help="Interval in seconds between events (default: 1.0)"
    )
    parser.add_argument(
        "--anomaly-prob",
        type=float,
        default=ANOMALY_PROBABILITY,
        help="Probability of generating an anomalous event (default: 0.03)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers override (default from config/env)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Kafka topic override (default: cloud-cost-events)"
    )

    args = parser.parse_args()

    run_producer(
        max_events=args.max_events,
        interval=args.interval,
        anomaly_prob=args.anomaly_prob,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )


if __name__ == "__main__":
    main()
