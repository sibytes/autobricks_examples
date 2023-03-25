from .api_service import ApiService, autobricks_logging
from ._decode_utils import base64_decode, format_path_for_os
import base64
import os, fnmatch

_logger = autobricks_logging.get_logger(__name__)

endpoint = "dbfs"


_api_service = ApiService()


def dbfs_upload(from_path: str, to_path: str, overwrite: bool = True):

    # Create a handle that will be used to add blocks
    data = {"path": to_path, "overwrite": "true"}

    handle = _api_service.api_post(endpoint, "create", data)["handle"]

    with open(from_path, "rb") as f:

        for block in _read_file_block(f):

            # data = block.encode("utf-8")
            data = base64.b64encode(block)
            data = {"handle": handle, "data": data.decode()}
            _api_service.api_post(endpoint, "add-block", data)

    # close the handle to finish uploading
    data = {"handle": handle}
    return _api_service.api_post(endpoint, "close", data)


def dbfs_upload_files(
    file_match: str, from_path: str, to_path: str, overwrite: bool = True
):

    _logger.info(
        f"Uploading files from {from_path} with filename matching {file_match} to dbfs {to_path}"
    )
    files = find_file(file_match, from_path)

    if not files:
        files = []

    _logger.info(f"{len(files)} files will be upload with overwrite={overwrite}")

    for f in files:

        filename = os.path.basename(f)

        _logger.info(f"{f} => {to_path}/{filename}")
        dbfs_upload(f, f"{to_path}/{filename}", overwrite)


def dbfs_delete_file(path: str, recursive: bool = True):

    data = {"path": path, "recursive": recursive}
    return _api_service.api_post(endpoint, "delete", data)


def dbfs_get_status(path: str):

    data = {"path": path}
    return _api_service.api_get(endpoint, "get-status", data)


def dbfs_list(path: str):

    data = {"path": path}
    return _api_service.api_get(endpoint, "list", data)


def dbfs_mkdirs(path: str):

    data = {"path": path}
    return _api_service.api_post(endpoint, "mkdirs", data)


def dbfs_move(source_path: str, destination_path: str):

    data = {"source_path": source_path, "destination_path": destination_path}
    return _api_service.api_post(endpoint, "move", data)


def dbfs_read(path: str, offset: int, length: int):

    data = {"path": path, "offset": offset, "length": length}
    return _api_service.api_get(endpoint, "read", data)


def dbfs_download(from_path: str, to_path: str):

    to_os_path = format_path_for_os(to_path)

    _logger.info(f"Starting to download file from dbfs: {from_path} => {to_os_path}")

    offset = 0
    # api limited to 1mb chunks
    read_chunk = 1024
    total_bytes = 0
    with open(to_os_path, "wb") as file:
        while True:

            response = dbfs_read(from_path, offset, read_chunk)
            offset += read_chunk
            bytes_read = response["bytes_read"]
            total_bytes += bytes_read
            data = response["data"]
            data = base64_decode(data)

            file.write(data)

            if bytes_read < read_chunk:
                break

    _logger.info(f"Finished downloading {str(total_bytes)} bytes")


def _read_file_block(file_object, chunk_size=1024):
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


def find_file(pattern: str, path: str):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result
