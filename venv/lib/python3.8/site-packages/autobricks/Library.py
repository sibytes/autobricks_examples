from .api_service import ApiService, autobricks_logging
from wheel_inspect import inspect_wheel
from dataclasses import dataclass
import os

_logger = autobricks_logging.get_logger(__name__)

_api_service = ApiService()


def library_all_cluster_statuses():

    return _api_service.api_get("libraries", "all-cluster-statuses")


@dataclass
class Wheel:
    path: str
    name: str
    major: int
    minor: int
    patch: int
    dev: int


def _is_wheel(entry):

    ret = entry.path.endswith(".whl")
    ret = ret and entry.is_file()
    ret = ret and not ".dirty" in entry.path

    return ret


def _get_wheel_version(wheel_path: str):

    version = inspect_wheel(wheel_path)["version"].split(".")

    if len(version) > 3:
        dev = int(version[3].replace("dev", ""))
    else:
        dev = 0

    name = inspect_wheel(wheel_path)["project"]
    wheel = Wheel(wheel_path, name, version[0], version[1], version[2], dev)

    return wheel


def get_latest_wheel(path: str, name: str):

    directory = os.path.abspath(path)
    wheels = [
        _get_wheel_version(entry.path)
        for entry in os.scandir(directory)
        if _is_wheel(entry)
    ]
    wheels = [w for w in wheels if name == w.name]
    if len(wheels) > 0:
        wheels.sort(key=lambda x: (x.major, x.minor, x.patch, x.dev), reverse=True)
        wheel = wheels[0]
    else:
        wheel = None

    return wheel
