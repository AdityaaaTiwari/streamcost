"""
Service definitions, regions, and approximate pricing rates for synthetic cost generation.
These rates are realistic approximations designed for data engineering demos and anomaly detection validation.
"""
from typing import Dict, Any, List

# Cloud Providers and their supported regions
CLOUD_REGIONS: Dict[str, List[str]] = {
    "AWS": ["ap-south-1", "us-east-1", "eu-west-1"],
    "Azure": ["Central India", "East US", "West Europe"],
    "GCP": ["asia-south1", "us-central1", "europe-west1"]
}

# Example accounts / subscriptions / projects per cloud
CLOUD_ACCOUNTS: Dict[str, List[str]] = {
    "AWS": ["aws-prod-001", "aws-stage-002", "aws-data-analytics-003"],
    "Azure": ["sub-corp-prod-01", "sub-corp-staging-02", "sub-data-lake-03"],
    "GCP": ["gcp-prod-infra-01", "gcp-analytics-prod-02", "gcp-ml-sandbox-03"]
}

# Service catalog with usage units, usage sampling bounds, and approximate unit rates (in USD)
SERVICE_CATALOG: Dict[str, Dict[str, Dict[str, Any]]] = {
    "AWS": {
        "EC2": {
            "usage_unit": "hours",
            "rate_per_unit": 0.096,         # ~$0.096 / hr (e.g., t3.large)
            "usage_min": 0.5,
            "usage_max": 24.0,
            "resource_prefix": "i-"
        },
        "S3": {
            "usage_unit": "GB",
            "rate_per_unit": 0.023,         # ~$0.023 / GB-month
            "usage_min": 10.0,
            "usage_max": 500.0,
            "resource_prefix": "bucket-s3-"
        },
        "RDS": {
            "usage_unit": "hours",
            "rate_per_unit": 0.24,          # ~$0.24 / hr (e.g., db.m5.large)
            "usage_min": 1.0,
            "usage_max": 24.0,
            "resource_prefix": "rds-db-"
        },
        "Lambda": {
            "usage_unit": "requests",
            "rate_per_unit": 0.0000002,     # $0.20 per 1M requests
            "usage_min": 10000.0,
            "usage_max": 2000000.0,
            "resource_prefix": "fn-aws-"
        },
        "DataTransfer": {
            "usage_unit": "GB",
            "rate_per_unit": 0.09,          # ~$0.09 / GB outbound
            "usage_min": 5.0,
            "usage_max": 150.0,
            "resource_prefix": "dt-out-"
        }
    },
    "Azure": {
        "VirtualMachines": {
            "usage_unit": "hours",
            "rate_per_unit": 0.104,         # ~$0.104 / hr (e.g., D2s v5)
            "usage_min": 0.5,
            "usage_max": 24.0,
            "resource_prefix": "vm-az-"
        },
        "BlobStorage": {
            "usage_unit": "GB",
            "rate_per_unit": 0.020,         # ~$0.020 / GB Hot tier
            "usage_min": 15.0,
            "usage_max": 600.0,
            "resource_prefix": "blob-az-"
        },
        "SQLDatabase": {
            "usage_unit": "hours",
            "rate_per_unit": 0.28,          # ~$0.28 / hr
            "usage_min": 1.0,
            "usage_max": 24.0,
            "resource_prefix": "sqldb-az-"
        },
        "Functions": {
            "usage_unit": "requests",
            "rate_per_unit": 0.0000002,     # ~$0.20 per 1M executions
            "usage_min": 10000.0,
            "usage_max": 2000000.0,
            "resource_prefix": "func-az-"
        },
        "Bandwidth": {
            "usage_unit": "GB",
            "rate_per_unit": 0.087,         # ~$0.087 / GB Internet Egress
            "usage_min": 5.0,
            "usage_max": 150.0,
            "resource_prefix": "bw-az-"
        }
    },
    "GCP": {
        "Compute Engine": {
            "usage_unit": "hours",
            "rate_per_unit": 0.095,         # ~$0.095 / hr (e.g., e2-standard-2)
            "usage_min": 0.5,
            "usage_max": 24.0,
            "resource_prefix": "gce-inst-"
        },
        "Cloud Storage": {
            "usage_unit": "GB",
            "rate_per_unit": 0.020,         # ~$0.020 / GB Standard
            "usage_min": 10.0,
            "usage_max": 500.0,
            "resource_prefix": "gcs-bucket-"
        },
        "Cloud SQL": {
            "usage_unit": "hours",
            "rate_per_unit": 0.26,          # ~$0.26 / hr
            "usage_min": 1.0,
            "usage_max": 24.0,
            "resource_prefix": "csql-db-"
        },
        "Cloud Functions": {
            "usage_unit": "requests",
            "rate_per_unit": 0.0000004,     # ~$0.40 per 1M invocations
            "usage_min": 10000.0,
            "usage_max": 2000000.0,
            "resource_prefix": "gcf-fn-"
        },
        "Network": {
            "usage_unit": "GB",
            "rate_per_unit": 0.085,         # ~$0.085 / GB Worldwide Egress
            "usage_min": 5.0,
            "usage_max": 150.0,
            "resource_prefix": "net-egress-"
        }
    }
}
