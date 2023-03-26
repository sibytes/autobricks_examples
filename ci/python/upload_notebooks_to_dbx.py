import os
from autobricks import Workspace

ROOT_DIR = os.getenv("ROOT_DIR")
WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT")
WORKSPACE_SUBDIRS:str = os.getenv("WORKSPACE_SUBDIRS")

from_notebook_root = f"{ROOT_DIR}/Databricks/Notebooks"
target_dir = f"/{WORKSPACE_ROOT}/"
sub_folders = [d.strip() for d in WORKSPACE_SUBDIRS.split(",")]

for f in sub_folders:

    Workspace.workspace_import_dir(
        from_notebook_root=from_notebook_root,
        source_dir=f"/{f}/",
        target_dir=target_dir,
        deploy_mode=Workspace.DeployMode.PARENT
    )
