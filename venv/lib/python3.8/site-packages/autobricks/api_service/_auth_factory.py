from enum import Enum
from ._auth import (
    UserAuth,
    SPAuth,
    SPMgmtEndpointAuth,
    SPAdalAuth,
    SPMgmtEndpointAdalAuth,
)
from . import autobricks_logging
from ._exceptions import AutobricksAuthTypeNotRegistered

_logger = autobricks_logging.get_logger(__name__)


class AuthenticationType(Enum):

    USER = (1,)
    SERVICE_PRINCIPAL = (2,)
    SERVICE_PRINCIPAL_MGMT_ENDPOINT = (3,)
    SERVICE_PRINCIPAL_ADAL = (4,)
    SERVICE_PRINCIPAL_MGMT_ENDPOINT_ADAL = 5


class AuthFactory:
    def __init__(self):
        self._creators = {}

    def register_format(self, auth_type: AuthenticationType, creator: type):
        self._creators[auth_type] = creator

    def get_auth(self, auth_type: AuthenticationType, parameters: dict):

        creator = self._creators.get(auth_type)

        if not creator:
            e = AutobricksAuthTypeNotRegistered(auth_type)
            _logger.error(e.message)
            raise e

        return creator(parameters)


auth_factory = AuthFactory()
auth_factory.register_format(AuthenticationType.USER, UserAuth)
auth_factory.register_format(AuthenticationType.SERVICE_PRINCIPAL, SPAuth)
auth_factory.register_format(
    AuthenticationType.SERVICE_PRINCIPAL_MGMT_ENDPOINT, SPMgmtEndpointAuth
)
auth_factory.register_format(AuthenticationType.SERVICE_PRINCIPAL_ADAL, SPAdalAuth)
auth_factory.register_format(
    AuthenticationType.SERVICE_PRINCIPAL_MGMT_ENDPOINT_ADAL, SPMgmtEndpointAdalAuth
)
