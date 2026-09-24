"""
Synthetic multi-cloud cost event generator.
Simulates realistic usage and expenditure streams across AWS, Azure, and GCP.
"""
import random
import uuid
from datetime import datetime, timezone
from typing import Optional, Literal
from streaming.models import CloudCostEvent
from streaming.rates import SERVICE_CATALOG, CLOUD_REGIONS, CLOUD_ACCOUNTS


def generate_event(
    anomaly_prob: float = 0.03,
    provider: Optional[Literal["AWS", "Azure", "GCP"]] = None,
    force_anomaly: bool = False
) -> CloudCostEvent:
    """
    Generates a single realistic synthetic CloudCostEvent.

    Args:
        anomaly_prob: Probability (0.0 to 1.0) of generating an anomalous cost spike.
        provider: Optional override to force an event for a specific cloud provider.
        force_anomaly: If True, forces this event to be an anomaly regardless of probability.

    Returns:
        CloudCostEvent: Validated Pydantic model instance.
    """
    # 1. Select Cloud Provider
    if not provider:
        provider = random.choice(["AWS", "Azure", "GCP"])

    # 2. Select Service from Catalog
    services = SERVICE_CATALOG[provider]
    service_name = random.choice(list(services.keys()))
    service_meta = services[service_name]

    # 3. Select Region and Account
    region = random.choice(CLOUD_REGIONS[provider])
    account_id = random.choice(CLOUD_ACCOUNTS[provider])

    # 4. Generate Resource ID
    resource_suffix = f"{random.randint(100, 999)}-{uuid.uuid4().hex[:6]}"
    resource_id = f"{service_meta['resource_prefix']}{resource_suffix}"

    # 5. Generate Realistic Usage
    usage_min = service_meta["usage_min"]
    usage_max = service_meta["usage_max"]
    usage_unit = service_meta["usage_unit"]

    if usage_unit == "requests":
        # Integer requests rounded to nearest hundred
        usage = float(round(random.uniform(usage_min, usage_max) / 100) * 100)
    else:
        usage = round(random.uniform(usage_min, usage_max), 2)

    # 6. Calculate Baseline Cost
    normal_cost = round(usage * service_meta["rate_per_unit"], 4)
    if normal_cost == 0.0 and usage > 0:
        normal_cost = 0.01  # Minimum non-zero cost for demonstration

    # 7. Check for Synthetic Anomaly Spike (3% by default or forced)
    is_anomaly = force_anomaly or (random.random() < anomaly_prob)

    if is_anomaly:
        # Multiplier range between 3x and 10x
        anomaly_multiplier = round(random.uniform(3.0, 10.0), 2)
        cost = round(normal_cost * anomaly_multiplier, 4)
    else:
        cost = normal_cost

    return CloudCostEvent(
        event_id=str(uuid.uuid4()),
        timestamp=datetime.now(timezone.utc),
        provider=provider,
        account_id=account_id,
        service=service_name,
        region=region,
        resource_id=resource_id,
        usage=usage,
        usage_unit=usage_unit,
        cost=cost,
        currency="USD",
        is_synthetic_anomaly=is_anomaly
    )
