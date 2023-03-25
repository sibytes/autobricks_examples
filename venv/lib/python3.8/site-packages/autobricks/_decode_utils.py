import base64
import sys
from enum import Enum
from .api_service import autobricks_logging

_logger = autobricks_logging.get_logger(__name__)


class OS(Enum):

    WINDOWS = 1
    LINUX = 2
    MAC = 3


def base64_decode(base64_string: str, encoding: str = "utf-8"):

    base64_bytes = base64_string.encode(encoding)
    content_bytes = base64.b64decode(base64_bytes)

    return content_bytes


def base64_encode(string_bytes: bytes, ecoding: str = "utf-8"):

    base64_bytes = base64.b64encode(string_bytes)

    return base64_bytes.decode(ecoding)


def is_windows():

    return sys.platform in ["win32", "cygwin", "msys"]


def format_path_for(path: str, os: OS):

    if os in (OS.LINUX, OS.MAC):
        return path.replace("\\", "/")

    elif os == OS.WINDOWS:
        return path.replace("/", "\\")


def format_path_for_os(path: str):

    if sys.platform in ["win32", "cygwin", "msys"]:
        return format_path_for(path, OS.WINDOWS)

    elif sys.platform in ["linux", "linux2"]:
        return format_path_for(path, OS.LINUX)

    elif sys.platform in ["darwin"]:
        return format_path_for(path, OS.MAC)

    else:
        raise Exception(
            f"Error formating path={path} for os. Operating system not supported {sys.platform}"
        )
