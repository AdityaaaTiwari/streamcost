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

> **Current Phase:** Day 1 Infrastructure Setup (Repository Structure, Docker Services with PostgreSQL 16 & Apache Kafka in KRaft mode, Health Checks, and Base Event Schema).

---

### Repository Structure

```
streamcost/
├── ingestion/                  # Cloud cost producers and API scrapers (AWS CUR, Azure Cost Mgmt, GCP Billing)
├── streaming/                  # Stream processing consumers and event definitions
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
├── scripts/                    # Operational automation and health check scripts
├── .github/workflows/          # CI/CD automation pipelines
├── .env.example                # Template for environment variables
├── .gitignore                  # Git ignore rules
├── docker-compose.yml          # Container configuration for PostgreSQL and KRaft Kafka
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

---

### Tech Stack (Day 1)

- **Language:** Python 3.11+
- **Message Broker:** Apache Kafka (KRaft mode, no Zookeeper dependency)
- **Database:** PostgreSQL 16
- **Containerization:** Docker & Docker Compose
- **Data Validation:** Pydantic
- **Testing:** Pytest

---

### Getting Started

#### 1. Prerequisites
- [Docker & Docker Compose](https://docs.docker.com/get-docker/) installed and running
- [Python 3.11+](https://www.python.org/) installed

#### 2. Environment Configuration
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

Default settings in `.env`:
```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=streamcost
POSTGRES_USER=streamcost
POSTGRES_PASSWORD=streamcost

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=cloud-cost-events
```

#### 3. Start Infrastructure Services
Start PostgreSQL and Kafka in the background:
```bash
docker compose up -d
```

Verify running containers:
```bash
docker compose ps
```

#### 4. Install Python Dependencies
Create a virtual environment and install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

#### 5. Run Health Check & Verification
StreamCost includes an automated verification script that tests PostgreSQL connectivity, Kafka broker health, topic existence, and event publish/consume cycles:
```bash
python scripts/health_check.py
```

#### 6. Run Unit & Integration Tests
Execute the test suite using pytest:
```bash
pytest tests/ -v
```

---

### Cloud Cost Event Schema (Day 1)

Events streamed through the Kafka topic `cloud-cost-events` adhere to the following schema:

```json
{
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
```
