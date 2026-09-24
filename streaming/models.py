"""
StreamCost event data models.
"""
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict


class CloudCostEvent(BaseModel):
    """
    Schema for near-real-time cloud cost events.
    """
    model_config = ConfigDict(extra="ignore")

    timestamp: datetime = Field(..., description="ISO-8601 timestamp of usage event")
    provider: str = Field(..., description="Cloud provider, e.g., AWS, Azure, GCP")
    account_id: str = Field(..., description="Cloud account or project identifier")
    service: str = Field(..., description="Cloud service name, e.g., EC2, S3, BigQuery")
    region: str = Field(..., description="Geographic or cloud region, e.g., ap-south-1")
    resource_id: str = Field(..., description="Resource identifier or ARN")
    usage: float = Field(..., description="Usage amount")
    usage_unit: str = Field(..., description="Unit of measurement, e.g., hours, GB, requests")
    cost: float = Field(..., description="Incurred cost amount")
    currency: str = Field(default="USD", description="Currency code (ISO 4217)")
