import os
from . import autobricks_logging

_logger = autobricks_logging.get_logger(__name__)

"""
Loads the configuration from environment variables.

"""

_default_schema = {
    "type": "object",
    "properties": {
        "auth_type": {
            "type": "string",
            "enum": [
                "USER",
                "SERVICE_PRINCIPAL",
                "SERVICE_PRINCIPAL_MGMT_ENDPOINT",
                "SERVICE_PRINCIPAL_ADAL",
                "SERVICE_PRINCIPAL_MGMT_ENDPOINT_ADAL",
            ],
        },
        "databricks_api_host": {"type": "string"},
    },
    "required": ["auth_type", "databricks_api_host"],
}

_user_schema = {
    "type": "object",
    "properties": {
        "auth_type": {"type": "string"},
        "dbutilstoken": {"type": "string"},
        "databricks_api_host": {"type": "string"},
    },
    "required": ["auth_type", "dbutilstoken", "databricks_api_host"],
}

_sp_schema = {
    "type": "object",
    "properties": {
        "auth_type": {"type": "string"},
        "sp_client_id": {"type": "string"},
        "sp_client_secret": {"type": "string"},
        "ad_resource": {"type": "string"},
        "databricks_api_host": {"type": "string"},
    },
    "required": [
        "auth_type",
        "sp_client_id",
        "sp_client_secret",
        "ad_resource",
        "databricks_api_host",
    ],
}

_spme_schema = {
    "type": "object",
    "properties": {
        "auth_type": {"type": "string"},
        "sp_client_id": {"type": "string"},
        "sp_client_secret": {"type": "string"},
        "ad_resource": {"type": "string"},
        "tenant_id": {"type": "string"},
        "mgmt_resource_endpoint": {"type": "string"},
        "workspace_name": {"type": "string"},
        "resource_group": {"type": "string"},
        "subscription_id": {"type": "string"},
        "databricks_api_host": {"type": "string"},
    },
    "required": [
        "auth_type",
        "sp_client_id",
        "sp_client_secret",
        "ad_resource",
        "tenant_id",
        "mgmt_resource_endpoint",
        "workspace_name",
        "resource_group",
        "subscription_id",
        "databricks_api_host",
    ],
}

configuration = {
    "dbutilstoken": os.getenv("DBUTILSTOKEN"),
    "sp_client_id": os.getenv("SP_CLIENT_ID"),
    "sp_client_secret": os.getenv("SP_CLIENT_SECRET"),
    "ad_resource": os.getenv("AD_RESOURCE", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"),
    "tenant_id": os.getenv("TENANT_ID"),
    "mgmt_resource_endpoint": os.getenv(
        "MGMT_RESOURCE_ENDPOINT", "https://management.core.windows.net/"
    ),
    "workspace_name": os.getenv("WORKSPACE_NAME"),
    "resource_group": os.getenv("RESOURCE_GROUP"),
    "subscription_id": os.getenv("SUBSCRIPTION_ID"),
    "auth_type": os.getenv("AUTH_TYPE", "SERVICE_PRINCIPAL"),
    "databricks_api_host": os.getenv("DATABRICKS_API_HOST"),
}
