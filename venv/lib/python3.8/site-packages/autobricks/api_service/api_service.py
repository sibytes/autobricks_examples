from ._auth_factory import auth_factory, AuthenticationType
from ._auth import Auth
from ._base_api import base_api_get as _base_api_get, base_api_post as _base_api_post
from . import autobricks_logging
from ._exceptions import AutobricksConfigurationInvalid, AutobricksResponseJsonError
from ._configuration import configuration
import json

_logger = autobricks_logging.get_logger(__name__)


API_VERSION = "2.0"


class ApiService:
    def __init__(self, config: dict = None):

        _logger.info("Initialising ApiService")

        if config:
            _logger.debug("Configuring ApiService from constructor")
            _config = config
        else:
            _logger.debug("Configuring ApiService from default configuration")
            _config = configuration

        config_json = json.dumps(_config, indent=4)
        _logger.debug(config_json)

        try:
            auth_type_str = _config["auth_type"]
        except KeyError:
            e = AutobricksConfigurationInvalid(
                "auth_type", valid_values=AuthenticationType
            )
            _logger.error(e.message)
            raise e

        try:
            self.host = _config["databricks_api_host"]
            _logger.debug(f"Databricks REST endpoint set as {self.host}")
        except KeyError:
            e = AutobricksConfigurationInvalid("databricks_api_host")
            _logger.error(e.message)
            raise e

        try:
            _logger.debug(f"Setting AuthorisationType as {auth_type_str}")
            self.auth_type: AuthenticationType = AuthenticationType[auth_type_str]
        except:
            e = AutobricksConfigurationInvalid(
                "auth_type", value=auth_type_str, valid_values=AuthenticationType
            )
            _logger.error(e.message)
            raise e

        auth: Auth = auth_factory.get_auth(self.auth_type, _config)

        _logger.debug("Setting Authorisation Headers")
        self._headers = auth.get_headers()
        _header_json = json.dumps(self._headers, indent=4)
        _logger.debug(_header_json)

    def api_get(self, api: str, function: str, data: dict = None, query: str = None):

        url = f"{self.host}/api/{API_VERSION}/{api}/{function}"
        if query:
            url = f"{url}?{query}"
        response = _base_api_get(url=url, headers=self._headers, json=data)

        try:
            json = response.json()
        except Exception:
            ex = AutobricksResponseJsonError(url, "GET", data, response.text)
            _logger.error(ex.message)
            raise ex

        return json

    def api_post(self, api: str, function: str, data: dict):

        url = f"{self.host}/api/{API_VERSION}/{api}/{function}"
        response = _base_api_post(url=url, headers=self._headers, json=data)

        try:
            json = response.json()
        except Exception:
            ex = AutobricksResponseJsonError(url, "POST", data, response.text)
            _logger.error(ex.message)
            raise ex

        return json
