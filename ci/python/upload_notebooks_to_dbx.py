import os
from autobricks import Workspace

ROOT_DIR = os.getenv("ROOT_DIR")
WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT")

source_dir = f"{ROOT_DIR}/Databricks/Notebooks"
target_dir = f"/{WORKSPACE_ROOT}"

print(f"importing workspace files from {source_dir} to {target_dir}")

Workspace.workspace_import_dir(
    from_notebook_root=source_dir,
    # source_dir=target_dir,
    target_dir="autobricks",
    deploy_mode=Workspace.DeployMode.PARENT
)
