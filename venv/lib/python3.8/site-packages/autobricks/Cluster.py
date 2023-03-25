from .api_service import ApiService, autobricks_logging
import os
import yaml
from enum import Enum
import time
from .Dbfs import dbfs_upload

_logger = autobricks_logging.get_logger(__name__)

endpoint = "clusters"

_api_service = ApiService()


class ClusterState(Enum):
    PENDING = 1
    RUNNING = 2
    RESTARTING = 3
    RESIZING = 4
    TERMINATING = 5
    TERMINATED = 6
    ERROR = 7
    UNKNOWN = 8


class ClusterAction(Enum):
    PIN = 1
    UNPIN = 2
    START = 3
    RESTART = 4
    STOP = 5
    DELETE = 6


def cluster_name_exists(name: str):

    found = cluster_get_name_ids(name)

    return len(found) > 0


def cluster_get_by_name(name: str):

    response = cluster_list()
    found = []
    if response:
        found = [c for c in response["clusters"] if c["cluster_name"] == name]

    return found


def cluster_action(cluster_id: str, cluster_action: ClusterAction):

    action: str

    if cluster_action == ClusterAction.PIN:
        action = "pin"

    elif cluster_action == ClusterAction.UNPIN:
        action = "unpin"

    elif cluster_action == ClusterAction.START:
        action = "start"

    elif cluster_action == ClusterAction.RESTART:
        action = "restart"

    elif cluster_action == ClusterAction.STOP:
        action = "delete"

    elif cluster_action == ClusterAction.DELETE:
        action = "permanent-delete"

    cluster = dict()
    cluster["cluster_id"] = cluster_id

    logger.info(f"{cluster_action.name} cluster: {cluster_id}")

    response = _api_service.api_post(endpoint, action, cluster)

    return response


def clusters_create(
    cluster_defn_folder: str,
    pin: bool = True,
    stop: bool = True,
    delete_if_exists: bool = False,
    allow_duplicate_names: bool = False,
    init_script_path: str = None,
):

    directory = os.path.abspath(cluster_defn_folder)
    for entry in os.scandir(directory):
        if entry.path.endswith(".yaml") and entry.is_file():

            response = cluster_create(
                entry.path,
                pin,
                stop,
                delete_if_exists,
                allow_duplicate_names,
                init_script_path,
            )


def cluster_create(
    cluster_defn_path: str,
    pin: bool = True,
    stop: bool = True,
    delete_if_exists: bool = False,
    allow_duplicate_names: bool = False,
    init_script_path: str = None,
):

    with open(cluster_defn_path, "r") as f:
        cluster_defn: dict = yaml.safe_load(f)

    cluster_name: str = cluster_defn["cluster_name"]

    # upload init script
    if cluster_defn.get("init_scripts"):

        for i in cluster_defn["init_scripts"]:

            if i.get("dbfs"):

                to_path = i["dbfs"]["destination"]
                from_path = os.path.abspath(f"{init_script_path}/{cluster_name}.sh")
                dbfs_upload(from_path, to_path, overwrite=True)
                _logger.info(
                    f"Uploaded cluster {cluster_name} init script: {from_path} => {to_path}"
                )

    _logger.info(
        f"Attempting to create a cluster using the declaration at path {cluster_defn_path} with the name {cluster_name}"
    )

    clusters = cluster_get_by_name(cluster_name)
    _logger.info(f"Found {len(clusters)} clusters with the name {cluster_name}")

    if len(clusters) > 0 and delete_if_exists:

        _logger.info(
            f"Cluster(s) named {cluster_name} already exist. Deleting existing clusters."
        )
        cluster_delete_clusters(clusters)

    if len(clusters) == 0 or delete_if_exists or allow_duplicate_names:

        _logger.info(f"Creating cluster {cluster_name}")
        response = _api_service.api_post(endpoint, "create", cluster_defn)
        cluster_id = response["cluster_id"]
        _logger.info(f"Cluster {cluster_name} created with id: {cluster_id}")
        create_response = {"cluster_id": cluster_id}

        if pin:
            cluster_action(cluster_id, ClusterAction.PIN)
        if stop:
            cluster_action(cluster_id, ClusterAction.STOP)
        create_response = {"clusters": [create_response], "created": True}

    else:

        _logger.info(
            f"Cluster {cluster_name} already exists and delete_if_exists is disabled."
        )
        create_response = {"clusters": clusters, "created": False}

    return create_response


def cluster_delete_clusters(clusters: list):

    cluster_ids = [c["cluster_id"] for c in clusters]
    _logger.info(f"deleting clusters: {str(cluster_ids)}")

    for c in cluster_ids:
        try:
            cluster_action(c, ClusterAction.UNPIN)

        except Exception as e:
            _logger.info(f"Warning: Failed to unpin cluster_id={c}")

        cluster_action(c, ClusterAction.DELETE)


def cluster_list() -> dict:

    response = _api_service.api_get(endpoint, "list")
    if response == {}:
        return None
    else:
        return response


def get_cluster_state(cluster_id: str):

    cluster = cluster_get(cluster_id)
    state = ClusterState[cluster["state"]]
    return state


def cluster_is_running(cluster_id: str):

    return get_cluster_state(cluster_id) == ClusterState.RUNNING


def cluster_is_terminated(cluster_id: str):

    return get_cluster_state(cluster_id) == ClusterState.TERMINATED


def cluster_get(cluster_id: str):

    query = f"cluster_id={cluster_id}"
    return _api_service.api_get(endpoint, "get", query=query)


def cluster_wait_until_state(
    cluster_id: str, cluster_state: ClusterState, wait_seconds: int = 10
):

    _logger.info(
        f"Waiting for the cluster_id {cluster_id} to the reach the state: {cluster_state.name}"
    )

    if cluster_state not in [ClusterState.RUNNING, ClusterState.TERMINATED]:

        msg = f"Waiting for a cluster state {cluster_state.name} isn't final and isn't valid"
        _logger.error(msg)
        raise Exception(msg)

    previous_state = ClusterState.UNKNOWN
    _logger.info(f"The cluster_id {cluster_id} state: {previous_state.name}")

    while True:

        time.sleep(wait_seconds)

        cluster = cluster_get(cluster_id)

        state = ClusterState[cluster["state"]]
        # only log out if it's changed to reduce the log output
        if previous_state != state:
            _logger.info(f"The cluster_id {cluster_id} state: {state.name}")
            previous_state = state

        if state == ClusterState.ERROR:

            msg = f"The cluster_id {cluster_id} is in error state: {cluster['state_message']}"
            _logger.error(msg)
            raise Exception(msg)

        elif state == ClusterState.TERMINATED and cluster_state == ClusterState.RUNNING:

            msg = f"The cluster_id {cluster_id} {ClusterState.TERMINATED.name}: {cluster['state_message']}"
            _logger.error(msg)
            raise Exception(msg)

        elif state == cluster_state:

            break


def cluster_has_tag(cluster: dict, tag_key: str, tag_value: str):

    if "custom_tags" in cluster:
        value = cluster["custom_tags"].get(tag_key)
        return value == tag_value
    else:
        return False


def clusters_clear_down(
    tag_key: str = None, tag_value: str = None, show_only: bool = True
):

    clusters = cluster_list()

    if tag_key and tag_value:
        clusters["clusters"] = [
            c for c in clusters["clusters"] if cluster_has_tag(c, tag_key, tag_value)
        ]
        _logger.info(
            f"Deleting {len(clusters)} clusters found with tag {tag_key}={tag_value}"
        )
    else:
        _logger.info(f"Deleting {len(clusters)} clusters found")

    if show_only:
        for c in clusters["clusters"]:
            cluster_id = c["cluster_id"]
            cluster = cluster_get(cluster_id)
            cluster_name = cluster["cluster_name"]
            spark_version = cluster["spark_version"]
            _logger.info(
                f"name: {cluster_name} id: {cluster_id} spark_version: {spark_version} "
            )

    if not show_only:
        if clusters:
            for c in clusters["clusters"]:
                cluster_id = c["cluster_id"]
                cluster_action(cluster_id, ClusterAction.STOP)

            cluster_delete_clusters(clusters["clusters"])


def cluster_log_states():

    clusters = cluster_list()
    if clusters:
        for c in clusters["clusters"]:
            _logger.info(f'{c["cluster_id"]}: {c["state"]}')
    else:
        _logger.info("No clusters found")


def cluster_run(cluster_id: str):

    if cluster_is_terminated(cluster_id):
        _logger.info(f"starting cluster {cluster_id}")
        cluster_action(cluster_id, ClusterAction.START)
        cluster_wait_until_state(cluster_id, ClusterState.RUNNING)

    elif not cluster_is_running(cluster_id):
        _logger.info("cluster is starting")
        _logger.info(f"cluster {cluster_id} is starting")
        cluster_wait_until_state(cluster_id, ClusterState.RUNNING)

    else:
        _logger.info(f"cluster {cluster_id} is running")
