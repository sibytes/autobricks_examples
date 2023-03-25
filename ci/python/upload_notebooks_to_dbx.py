# from autobricks import Workspace
import os
from autobricks import Workspace

Workspace.workspace_import_dir(
    from_notebook_root="",
    source_dir="",
    target_dir="autobricks",
    deploy_mode=Workspace.DeployMode.PARENT
)
