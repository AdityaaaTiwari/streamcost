"""
Unit tests for CloudCostEvent schema and validation.
"""
import json
from pathlib import Path
import pytest
from pydantic import ValidationError
from streaming.models import CloudCostEvent


def test_valid_cloud_cost_event():
    raw_event = {
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
    event = CloudCostEvent(**raw_event)
    assert event.provider == "AWS"
    assert event.account_id == "aws-prod-001"
    assert event.service == "EC2"
    assert event.region == "ap-south-1"
    assert event.resource_id == "i-demo001"
    assert event.usage == 1.0
    assert event.cost == 0.12
    assert event.currency == "USD"


def test_sample_json_file():
    sample_file = Path(__file__).resolve().parent.parent / "data" / "sample" / "test_event.json"
    assert sample_file.exists(), "Sample test_event.json fixture must exist"

    with open(sample_file, "r") as f:
        data = json.load(f)

    event = CloudCostEvent(**data)
    assert event.resource_id == "i-demo001"


def test_missing_required_fields():
    with pytest.raises(ValidationError):
        CloudCostEvent(
            provider="AWS",
            service="EC2"
        )


def test_default_currency():
    event = CloudCostEvent(
        timestamp="2026-09-24T14:00:00Z",
        provider="GCP",
        account_id="gcp-prod-002",
        service="Compute Engine",
        region="us-central1",
        resource_id="instance-gcp-01",
        usage=2.5,
        usage_unit="hours",
        cost=0.35
    )
    assert event.currency == "USD"
