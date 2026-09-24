"""
Unit tests for StreamCost event models, generator, and serialization.
These tests do not require Kafka or PostgreSQL to be running.
"""
import json
from unittest.mock import MagicMock
import pytest
from pydantic import ValidationError

from streaming.models import CloudCostEvent
from streaming.generator import generate_event
from streaming.rates import SERVICE_CATALOG, CLOUD_REGIONS
from streaming.producer import publish_event


# 1. Valid CloudCostEvent
def test_valid_cloud_cost_event():
    event = CloudCostEvent(
        provider="AWS",
        account_id="aws-prod-001",
        service="EC2",
        region="ap-south-1",
        resource_id="i-test001",
        usage=2.5,
        usage_unit="hours",
        cost=0.24,
        currency="USD"
    )
    assert event.provider == "AWS"
    assert event.service == "EC2"
    assert event.usage == 2.5
    assert event.cost == 0.24
    assert event.currency == "USD"
    assert event.event_id is not None
    assert event.timestamp is not None
    assert event.is_synthetic_anomaly is False


# 2. Invalid provider rejected
def test_invalid_provider_rejected():
    with pytest.raises(ValidationError):
        CloudCostEvent(
            provider="OracleCloud",  # Not in Literal["AWS", "Azure", "GCP"]
            account_id="ora-001",
            service="Compute",
            region="us-phoenix-1",
            resource_id="ocid-1",
            usage=1.0,
            usage_unit="hours",
            cost=0.10
        )


# 3. Negative cost rejected
def test_negative_cost_rejected():
    with pytest.raises(ValidationError):
        CloudCostEvent(
            provider="AWS",
            account_id="aws-prod-001",
            service="EC2",
            region="ap-south-1",
            resource_id="i-test001",
            usage=1.0,
            usage_unit="hours",
            cost=-0.05
        )


# 4. Negative usage rejected
def test_negative_usage_rejected():
    with pytest.raises(ValidationError):
        CloudCostEvent(
            provider="GCP",
            account_id="gcp-prod-001",
            service="Compute Engine",
            region="us-central1",
            resource_id="gce-001",
            usage=-10.0,
            usage_unit="hours",
            cost=0.50
        )


# 5. Event ID uniqueness
def test_event_id_uniqueness():
    num_events = 100
    events = [generate_event() for _ in range(num_events)]
    unique_ids = {e.event_id for e in events}
    assert len(unique_ids) == num_events, "Every generated event must have a unique event_id"


# 6. Event generation for all providers
def test_event_generation_providers_and_services():
    for provider in ["AWS", "Azure", "GCP"]:
        event = generate_event(provider=provider)
        assert event.provider == provider
        assert event.service in SERVICE_CATALOG[provider]
        assert event.region in CLOUD_REGIONS[provider]
        assert event.usage >= 0.0
        assert event.cost >= 0.0
        assert event.currency == "USD"


def test_event_generation_diversity():
    events = [generate_event() for _ in range(60)]
    providers = {e.provider for e in events}
    services = {e.service for e in events}

    # All 3 cloud providers should appear across 60 random events
    assert "AWS" in providers
    assert "Azure" in providers
    assert "GCP" in providers
    assert len(services) >= 5, "Diverse services should be generated"


# 7. Anomaly generation
def test_anomaly_generation_behavior():
    # Forced normal event
    normal_event = generate_event(anomaly_prob=0.0, force_anomaly=False)
    assert normal_event.is_synthetic_anomaly is False

    # Forced anomaly event
    anomaly_event = generate_event(force_anomaly=True)
    assert anomaly_event.is_synthetic_anomaly is True
    assert anomaly_event.cost > 0.0


# 8. Serialization and Deserialization
def test_serialization_and_deserialization():
    original = generate_event(force_anomaly=True)
    json_data = original.model_dump_json()

    # Verify JSON string parses
    parsed_dict = json.loads(json_data)
    assert parsed_dict["event_id"] == original.event_id
    assert parsed_dict["provider"] == original.provider
    assert parsed_dict["cost"] == original.cost
    assert parsed_dict["is_synthetic_anomaly"] is True

    # Reconstruct Pydantic model
    restored = CloudCostEvent.model_validate_json(json_data)
    assert restored.event_id == original.event_id
    assert restored.provider == original.provider
    assert restored.service == original.service
    assert restored.cost == original.cost
    assert restored.is_synthetic_anomaly is True


# 9. Kafka producer publishing with mock
def test_publish_event_with_mock():
    mock_producer = MagicMock()
    mock_future = MagicMock()
    mock_producer.send.return_value = mock_future

    event = generate_event()
    publish_event(mock_producer, "cloud-cost-events", event)

    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args
    assert call_args[0][0] == "cloud-cost-events"
    assert call_args[1]["key"] == event.account_id
    assert call_args[1]["value"]["event_id"] == event.event_id
    mock_future.get.assert_called_once()
