"""
StreamCost event data models.
"""
import uuid
from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict


class CloudCostEvent(BaseModel):
    """
    Schema for near-real-time multi-cloud cost events.
    Supports AWS, Azure, and GCP usage metrics.
    """
    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for the event"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of the usage event"
    )
    provider: Literal["AWS", "Azure", "GCP"] = Field(
        ...,
        description="Cloud provider (AWS, Azure, GCP)"
    )
    account_id: str = Field(..., description="Cloud account or subscription/project ID")
    service: str = Field(..., description="Cloud service name (e.g., EC2, VirtualMachines, Compute Engine)")
    region: str = Field(..., description="Cloud region name")
    resource_id: str = Field(..., description="Target resource identifier")
    usage: float = Field(..., ge=0.0, description="Usage amount (must be >= 0)")
    usage_unit: str = Field(..., description="Unit of measurement (hours, GB, requests, etc.)")
    cost: float = Field(..., ge=0.0, description="Incurred cost amount in currency (must be >= 0)")
    currency: str = Field(default="USD", description="Currency code (ISO 4217)")
    is_synthetic_anomaly: bool = Field(
        default=False,
        description="Internal marker for synthetic anomaly validation"
    )
