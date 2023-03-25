# from autobricks import Workspace
import os

print(os.getenv("TENANT_ID"))
print(os.getenv("SP_CLIENT_ID"))
print(os.getenv("SP_CLIENT_SECRET"))

# Workspace.workspace_import_dir(
#     from_notebook_root="",
#     source_dir="",
#     target_dir="autobricks",
#     deploy_mode=Workspace.DeployMode.PARENT
# )
