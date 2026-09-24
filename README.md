# StreamCost

## Real-Time Multi-Cloud Cost Intelligence & Anomaly Detection Platform

StreamCost is a near-real-time cloud cost intelligence and anomaly detection platform designed to ingest, process, and analyze billing and resource usage streams across **AWS**, **Azure**, and **GCP**. It empowers engineering and FinOps teams to detect cost spikes immediately, forecast expenditures, and eliminate cloud waste before the monthly bill arrives.

---

### Planned Architecture

```
Cloud Cost Sources (AWS / Azure / GCP)
        ↓
Kafka (Message Broker / KRaft)
        ↓
PySpark Structured Streaming (ETL & Window Aggregations)
        ↓
AWS S3 (Data Lake) + PostgreSQL (Data Warehouse / App DB)
        ↓
dbt (Transformations & Data Modeling)
        ↓
Cost Intelligence (Aggregations & Unit Economics)
        ↓
Anomaly Detection + Forecasting (Z-score / Isolation Forest / Prophet)
        ↓
Streamlit Dashboard (Interactive Visualizations & Alerts)
```

> **Current Phase:** Day 2 — Synthetic Multi-Cloud Event Generator (Pydantic schema, realistic rate models, controlled anomaly generation, Kafka producer & consumer CLI tools).

---

### Day 2 — Synthetic Multi-Cloud Event Generator

During this development phase, StreamCost features a high-fidelity synthetic multi-cloud event generator:

```
[AWS / Azure / GCP Synthetic Event Generator]
                     ↓
       [Kafka Topic: cloud-cost-events]
                     ↓
[Future PySpark Streaming Pipeline & App DB]
```

> **Note on Synthetic Data:**
> Synthetic data is intentionally generated during development and testing to simulate multi-cloud usage without requiring live cloud provider billing API credentials (AWS Cost Explorer, Azure Cost Management, GCP Cloud Billing) or incurring real cloud costs. This data simulates realistic usage patterns, resource naming, and approximate service pricing, but does not represent actual production cloud bills.

#### Features
- **Multi-Cloud Support:** Generates events across **AWS** (`EC2`, `S3`, `RDS`, `Lambda`, `DataTransfer`), **Azure** (`VirtualMachines`, `BlobStorage`, `SQLDatabase`, `Functions`, `Bandwidth`), and **GCP** (`Compute Engine`, `Cloud Storage`, `Cloud SQL`, `Cloud Functions`, `Network`).
- **Realistic Pricing Models:** Calculates baseline costs using service-specific unit rates and usage metrics (`streaming/rates.py`).
- **Controlled Anomaly Injection:** Periodically injects synthetic cost anomalies (spikes of 3x to 10x normal cost, flagged via `is_synthetic_anomaly`) with configurable probability (`ANOMALY_PROBABILITY=0.03`) to benchmark and validate future anomaly detection models.
- **Graceful Execution:** Full CLI tools with clean shutdown (`Ctrl+C`), Kafka reconnection handling, and configurable throughput.

---

### Repository Structure

```
streamcost/
├── ingestion/                  # Cloud cost producers and API scrapers (AWS CUR, Azure Cost Mgmt, GCP Billing)
├── streaming/                  # Stream processing consumers, generators, models, and config
│   ├── config.py               # Streaming environment configuration
│   ├── generator.py            # Multi-cloud synthetic event generator
│   ├── models.py               # Pydantic schemas (CloudCostEvent)
│   ├── producer.py             # Kafka event producer
│   └── rates.py                # Multi-cloud service rate catalogs and regions
├── spark/                      # PySpark Structured Streaming jobs
├── storage/                    # Storage adapters, schema definitions, and migrations
├── dbt/                        # dbt project for warehouse modeling
├── anomaly_detection/          # Statistical and ML anomaly detection algorithms
├── forecasting/                # Time-series forecasting models
├── dashboard/                  # Streamlit FinOps visualization dashboard
├── dags/                       # Apache Airflow orchestration DAGs
├── tests/                      # Automated test suite (unit and integration tests)
├── data/                       # Local data directories (ignored by git except .gitkeep)
│   ├── raw/                    # Raw event dumps
│   ├── processed/              # Processed parquet/csv outputs
│   └── sample/                 # Synthetic cost event fixtures
├── docs/                       # Architectural diagrams and technical specifications
├── scripts/                    # Operational automation and CLI scripts
│   ├── health_check.py         # Infrastructure health verification
│   ├── run_producer.py         # Producer CLI entrypoint
│   └── run_consumer.py         # Consumer CLI entrypoint
├── .github/workflows/          # CI/CD automation pipelines
├── .env.example                # Template for environment variables
├── .gitignore                  # Git ignore rules
├── docker-compose.yml          # Container configuration for PostgreSQL and KRaft Kafka
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

---

### Tech Stack

- **Language:** Python 3.11+
- **Message Broker:** Apache Kafka (KRaft mode, no Zookeeper dependency)
- **Database:** PostgreSQL 16
- **Containerization:** Docker & Docker Compose
- **Data Validation:** Pydantic
- **Testing:** Pytest

---

### Quickstart & Run Instructions

#### 1. Prerequisites
- [Docker & Docker Compose](https://docs.docker.com/get-docker/) installed and running
- [Python 3.11+](https://www.python.org/) installed

#### 2. Environment Configuration
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

Configuration parameters:
```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=streamcost
POSTGRES_USER=streamcost
POSTGRES_PASSWORD=streamcost

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=cloud-cost-events
EVENT_INTERVAL_SECONDS=1
ANOMALY_PROBABILITY=0.03
MAX_EVENTS=0
```

#### 3. Start Infrastructure
Start PostgreSQL and Kafka in the background:
```bash
docker compose up -d
```

Verify services are healthy:
```bash
docker compose ps
```

#### 4. Setup Python Environment
```bash
python -m venv .venv
source .venv/bin/activate       # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

#### 5. Run the Event Producer
In your terminal, start the multi-cloud cost event generator:
```bash
python scripts/run_producer.py
```

Optional CLI flags:
```bash
python scripts/run_producer.py --max-events 100 --interval 0.5 --anomaly-prob 0.05
```

> **To stop the producer:** Press `Ctrl+C`. The producer will gracefully flush remaining records and close the connection.

#### 6. Run the Event Consumer
In another terminal, consume and inspect live cost events from Kafka:
```bash
python scripts/run_consumer.py
```

Optional flags to read from the beginning or limit events:
```bash
python scripts/run_consumer.py --from-beginning --max-events 50
```

#### 7. Run Automated Tests
Execute the complete test suite:
```bash
pytest tests/ -v
```

---

### Event Schema

Events published to `cloud-cost-events` adhere to the Pydantic schema defined in [`streaming/models.py`](file:///d:/streamcost/streaming/models.py):

```json
{
  "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "timestamp": "2026-09-24T10:44:15.355701Z",
  "provider": "AWS",
  "account_id": "aws-prod-001",
  "service": "EC2",
  "region": "ap-south-1",
  "resource_id": "i-429-a1b2c3",
  "usage": 2.5,
  "usage_unit": "hours",
  "cost": 0.24,
  "currency": "USD",
  "is_synthetic_anomaly": false
}
```
