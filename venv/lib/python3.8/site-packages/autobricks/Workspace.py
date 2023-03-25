from .api_service import ApiService, autobricks_logging

from ._decode_utils import (
    base64_encode,
    base64_decode,
    format_path_for_os,
    format_path_for,
    OS,
)
import os

from enum import Enum

_logger = autobricks_logging.get_logger(__name__)

endpoint = "workspace"


_api_service = ApiService()


class DeployMode(Enum):
    DEFAULT = 1
    MOVE = 2
    PARENT = 3
    CHILD = 4
    ROOT_CHILD = 5


class Extension(Enum):
    py = 1
    html = 2
    ipynb = 3
    dbc = 4


class Format(Enum):
    SOURCE = 1
    HTML = 2
    JUPYTER = 3
    DBC = 4


class Language(Enum):
    SCALA = 1
    PYTHON = 2
    SQL = 3
    R = 4


def get_format(extension: Extension):

    if extension == Extension.py:
        return Format.SOURCE
    elif extension == Extension.html:
        return Format.HTML
    elif extension == Extension.ipynb:
        return Format.JUPYTER
    elif extension == Extension.dbc:
        return Format.DBC


def workspace_import(
    form_path: str, to_path: str, format: Format, language: Language, overwrite: True
):

    with open(form_path, "rb") as file:
        content = base64_encode(file.read())

    data = {
        "path": to_path,
        "format": format.name,
        "language": language.name,
        "content": content,
        "overwrite": overwrite,
    }
    return _api_service.api_post(endpoint, "import", data)


def workspace_delete(path: str, recursive: True):

    data = {"path": path, "recursive": recursive}
    return _api_service.api_post(endpoint, "delete", data)


def workspace_get_status(path: str):

    data = {"path": path}
    return _api_service.api_get(endpoint, "get-status", data)


def workspace_list(path: str):

    data = {"path": path}
    return _api_service.api_get(endpoint, "list", data)


def workspace_mkdirs(path: str):

    data = {"path": path}
    return _api_service.api_post(endpoint, "mkdirs", data)


def workspace_export(from_path: str, format: Format, to_path: str):

    data = {"path": from_path, "format": format.name.upper(), "direct_download": False}

    response = _api_service.api_get(endpoint, "export", data)

    file_type = response["file_type"]
    filename = os.path.basename(from_path)
    file_path = f"{to_path}/{filename}.{file_type}"

    content = base64_decode(response["content"])
    with open(file_path, "wb") as file:
        file.write(content)

    if file_type == "py":
        response = {
            "from_path": from_path,
            "to_path": file_path,
            "file_type": Format.SOURCE.name,
        }

    elif file_type == "html":
        response = {
            "from_path": from_path,
            "to_path": file_path,
            "file_type": Format.HTML.name,
        }

    elif file_type == "ipynb":
        response = {
            "from_path": from_path,
            "to_path": file_path,
            "file_type": Format.JUPYTER.name,
        }

    elif file_type == "dbc":
        response = {
            "from_path": from_path,
            "to_path": file_path,
            "file_type": Format.DBC.name,
        }

    return response


def workspace_dir_exists(path: str):

    try:
        reponse = workspace_get_status(path)
    except Exception as e:
        return False

    return reponse.get("object_type") == "DIRECTORY" and reponse.get("path") == path


def workspace_notebook_exists(path: str):

    try:
        reponse = workspace_get_status(path)
    except Exception as e:
        return False

    return reponse.get("object_type") == "NOTEBOOK" and reponse.get("path") == path


def workspace_import_dir(
    from_notebook_root: str,
    source_dir: str = "/",
    target_dir: str = None,
    deploy_mode: DeployMode = DeployMode.DEFAULT,
):

    if deploy_mode == DeployMode.DEFAULT and target_dir:

        _logger.error(
            f"target_dir is not required for {deploy_mode.name} deployment mode"
        )
        raise Exception(
            f"target_dir is not required for {deploy_mode.name} deployment mode"
        )

    elif deploy_mode != DeployMode.DEFAULT and not target_dir:

        _logger.error(f"target_dir is required for {deploy_mode.name} deployment mode")
        raise Exception(
            f"target_dir is required for {deploy_mode.name} deployment mode"
        )

    # automaticall adjust for user entered paths using the wrong separators
    source_dir = format_path_for_os(source_dir)
    from_root_path = os.path.abspath(format_path_for_os(from_notebook_root))

    response = {
        "from_notebook_root": from_notebook_root,
        "source_dir": source_dir,
        "target_dir": target_dir,
        "actions": [],
    }

    deploy_this = False

    _logger.info(
        f"Start deploying from from_root_path={from_root_path} source_dir={source_dir} target_dir={target_dir} deploy_mode={deploy_mode.name}"
    )

    for root, subdirs, files in os.walk(from_root_path):

        deploy_dir = root.replace(from_root_path, "")

        _logger.debug(
            f"Checking if deploy_dir={deploy_dir} starts with source_dir={source_dir}"
        )

        if deploy_dir.startswith(source_dir) or not source_dir or source_dir == "/":

            deploy_this = True
            if deploy_dir == "":
                deploy_dir = "/"

        else:
            deploy_this = False
            _logger.info(
                f"Skipping dir source_dir={source_dir} deploy_dir={deploy_dir} target_dir={target_dir} deploy_mode={deploy_mode.name}"
            )

        if deploy_this:

            _logger.info(
                f"Deploying dir source_dir={source_dir} deploy_dir={deploy_dir} target_dir={target_dir} deploy_mode={deploy_mode.name}"
            )

            action = _deploy_dir(source_dir, deploy_dir, target_dir, deploy_mode)
            if action:
                response["actions"].append(action)

            # deploy the notebooks
            for filename in files:

                from_file_path = os.path.join(root, filename)
                to_file_path = from_file_path.replace(from_root_path, "")

                _logger.info(
                    f"Deploying from_file_path={from_file_path} source_dir={source_dir} deploy_dir={deploy_dir} target_dir={target_dir} deploy_mode={deploy_mode.name}"
                )

                action = _deploy_file(
                    from_file_path, source_dir, to_file_path, target_dir, deploy_mode
                )
                response["actions"].append(action)

    return response


def _deploy_file(
    from_file_path: str,
    source_dir: str,
    to_file_path: str,
    target_dir: str,
    deploy_mode: DeployMode,
):

    to_file_path = _modify_deploy_path(
        to_file_path, source_dir, target_dir, deploy_mode
    )

    to_file_path, extension = os.path.splitext(to_file_path)
    extension_type = Extension[extension.replace(".", "")]
    format = get_format(extension_type)
    language = Language.PYTHON
    overwrite = format != format.DBC

    action = {
        "action": "import",
        "from_file_path": from_file_path,
        "to_file_path": to_file_path,
        "format": format.name,
        "language": language.name,
        "overwrite": overwrite,
    }
    workspace_import(from_file_path, to_file_path, format, language, overwrite)

    return action


def _deploy_dir(
    source_dir: str, deploy_dir: str, target_dir: str, deploy_mode: DeployMode
):

    # insert the root sub-dir to support along side nested deployment if given.
    deploy_dir = _modify_deploy_path(deploy_dir, source_dir, target_dir, deploy_mode)

    # make the directory it's not the root
    action = None
    if deploy_dir != "/":
        action = {"action": "mkdirs", "path": deploy_dir}
        workspace_mkdirs(deploy_dir)

    return action


def _modify_deploy_path(
    deploy_dir: str, root: str, modifier: str, deploy_mode: DeployMode
):

    _logger.info(f"Preparing deploy path deploy_dir={deploy_dir}")
    _logger.debug(
        f"Preparing deploy path deploy_dir={deploy_dir} root={root} modifier={modifier} deploy_mode={deploy_mode.name}"
    )

    if deploy_mode == DeployMode.DEFAULT:
        new_path = deploy_dir

    elif deploy_mode == DeployMode.MOVE:
        new_path = deploy_dir.replace(root, modifier)

    elif deploy_mode == DeployMode.PARENT:
        modify_to = f"{modifier.replace('/.','')}{root}"
        new_path = deploy_dir.replace(root, modify_to)

    elif deploy_mode == DeployMode.CHILD:
        modify_to = f"{root}{modifier}"
        new_path = deploy_dir.replace(root, modify_to)

    elif deploy_mode == DeployMode.ROOT_CHILD:

        clean_modifier = modifier.replace("\\", "/")
        if clean_modifier.startswith("/"):
            clean_modifier = clean_modifier[1:]
        if clean_modifier.endswith("/"):
            clean_modifier = clean_modifier[:-1]

        folders = root.split("/")
        folders.insert(2, clean_modifier)
        modify_to = "/".join(folders)
        new_path = deploy_dir.replace(root, modify_to)

    # databricks runs on linux
    new_path = format_path_for(new_path, OS.LINUX)

    _logger.info(f"Prepared deploy path deploy_dir={new_path}")

    return new_path
