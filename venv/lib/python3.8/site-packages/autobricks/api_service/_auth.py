from abc import ABC, abstractmethod

import adal
from ._base_api import base_api_get as _api_get
from . import autobricks_logging

_logger = autobricks_logging.get_logger(__name__)


_AUTH_DNS = "login.microsoftonline.com"


class Auth(ABC):
    @abstractmethod
    def __init__(self, parameters: dict):
        pass

    @abstractmethod
    def get_headers(self) -> dict:
        pass


class UserAuth(Auth):
    def __init__(self, parameters: dict):

        try:
            self.bearer_token = parameters["dbutilstoken"]
        except KeyError as e:
            raise KeyError("dbutilstoken key not found in UserAuth parameters")

    def get_headers(self):
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        return headers


class SPAuth(Auth):
    def __init__(self, parameters: dict):

        self.sp_client_id = parameters["sp_client_id"]
        self.sp_client_secret = parameters["sp_client_secret"]
        self.ad_resource = parameters["ad_resource"]
        self.tenant_id = parameters["tenant_id"]
        self._authority_url = f"https://{_AUTH_DNS}/{self.tenant_id}/oauth2/token"
        self._authority_headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self._authority_data = {
            "grant_type": "client_credentials",
            "client_id": self.sp_client_id,
            "client_secret": self.sp_client_secret,
        }

        # get AD token
        self._authority_data["resource"] = self.ad_resource
        response = _api_get(
            url=self._authority_url,
            headers=self._authority_headers,
            data=self._authority_data,
        )
        self.bearer_token = response.json()["access_token"]

    def get_headers(self):
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        return headers


class SPMgmtEndpointAuth(SPAuth):
    def __init__(self, parameters: dict):

        # get AD token
        super().__init__(parameters)

        # get management endpoint token
        self.mgmt_resource_endpoint = parameters["mgmt_resource_endpoint"]
        self.workspace_name = parameters["workspace_name"]
        self.resource_group = parameters["resource_group"]
        self.subscription_id = parameters["subscription_id"]

        self._authority_data["resource"] = self.mgmt_resource_endpoint
        response = _api_get(
            url=self._authority_url,
            headers=self._authority_headers,
            data=self._authority_data,
        )
        self.mgmt_access_token = response.json()["access_token"]

    def get_headers(self):

        url = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Databricks/workspaces/{self.workspace_name}"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "X-Databricks-Azure-SP-Management-Token": self.mgmt_access_token,
            "X-Databricks-Azure-Workspace-Resource-Id": url,
        }
        return headers


class SPAdalAuth(Auth):
    def __init__(self, parameters: dict):

        self.sp_client_id = parameters["sp_client_id"]
        self.sp_client_secret = parameters["sp_client_secret"]
        self.ad_resource = parameters["ad_resource"]
        self.tenant_id = parameters["tenant_id"]

        self._authority_url = f"https://{_AUTH_DNS}/{self.tenant_id}"

        # get AD token
        context = adal.AuthenticationContext(self._authority_url)
        response = context.acquire_token_with_client_credentials(
            resource=self.ad_resource,
            client_id=self.sp_client_id,
            client_secret=self.sp_client_secret,
        )
        self.bearer_token = response["accessToken"]

    def get_headers(self):
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        return headers


class SPMgmtEndpointAdalAuth(SPAdalAuth):
    def __init__(self, parameters: dict):

        # get AD token
        super().__init__(parameters)
        self.mgmt_resource_endpoint = parameters["mgmt_resource_endpoint"]
        self.workspace_name = parameters["workspace_name"]
        self.resource_group = parameters["resource_group"]
        self.subscription_id = parameters["subscription_id"]

        # get management endpoint token
        context = adal.AuthenticationContext(self._authority_url)
        response = context.acquire_token_with_client_credentials(
            resource=self.mgmt_resource_endpoint,
            client_id=self.sp_client_id,
            client_secret=self.sp_client_secret,
        )
        self.mgmt_access_token = response["accessToken"]

    def get_headers(self):

        url = f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Databricks/workspaces/{self.workspace_name}"
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "X-Databricks-Azure-SP-Management-Token": self.mgmt_access_token,
            "X-Databricks-Azure-Workspace-Resource-Id": url,
        }
        return headers
