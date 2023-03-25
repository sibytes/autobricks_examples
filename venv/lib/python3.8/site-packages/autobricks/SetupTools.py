import distutils.cmd
import distutils.log
import os
from wheel_inspect import inspect_wheel
from dataclasses import dataclass
import shutil
import yaml
from .api_service import autobricks_logging

_logger = autobricks_logging.get_logger(__name__)


@dataclass
class Wheel:
    path: str
    name: str
    major: int
    minor: int
    patch: int
    dev: int


class DatabricksBuild(distutils.cmd.Command):

    description = "Build databricks project directory"
    user_options = [
        ("directory=", None, "path to databricks project directory"),
    ]

    def _copy_directory(self, from_path: str, to_path: str):

        try:
            # delete the destination dir if exists
            if os.path.exists(to_path):
                shutil.rmtree(to_path, ignore_errors=False, onerror=None)
            shutil.copytree(from_path, to_path)
        # Directories are the same
        except shutil.Error as e:
            print(f"Directory not copied. Error: {e}")
        # Any error saying that the directory doesn't exist
        except OSError as e:
            print(f"Directory not copied. Error: {e}")

    def _is_wheel(self, entry):

        ret = entry.path.endswith(".whl")
        ret = ret and entry.is_file()
        ret = ret and not ".dirty" in entry.path

        return ret

    def _get_wheel_version(self, wheel_path: str):

        version = inspect_wheel(wheel_path)["version"].split(".")

        if len(version) > 3:
            dev = int(version[3].replace("dev", ""))
        else:
            dev = 0

        name = inspect_wheel(wheel_path)["project"]
        wheel = Wheel(wheel_path, name, version[0], version[1], version[2], dev)

        return wheel

    def _get_latest_wheel(self, path: str, name: str = None):

        directory = os.path.abspath(path)
        wheels = [
            self._get_wheel_version(entry.path)
            for entry in os.scandir(directory)
            if self._is_wheel(entry)
        ]
        wheels = [w for w in wheels if name == w.name or not name]
        if len(wheels) > 0:
            wheels.sort(key=lambda x: (x.major, x.minor, x.patch, x.dev), reverse=True)
            wheel = wheels[0]
        else:
            wheel = None

        return wheel

    def initialize_options(self):

        """Set default values for options."""
        self.directory = "./databricks"
        self.dist = "./dist"
        self.databricks_dist_dir = f"{self.directory}/dist"

        if self.directory:
            assert os.path.exists(self.directory), (
                "Databricks project %s does not exist." % self.directory
            )

        if not os.path.exists(self.databricks_dist_dir):
            os.makedirs(self.databricks_dist_dir)

    def finalize_options(self):

        """Post-process options."""
        pass
        # if self.directory:
        #     assert os.path.exists(self.directory), (
        #         'Databricks project %s does not exist.' % self.directory)

    def _build_cluster_defns(
        self,
        cluster_defn_folder: str,
        semantic_version: str = "",
        init_script_path: str = None,
    ):

        directory = os.path.abspath(cluster_defn_folder)
        for entry in os.scandir(directory):
            if entry.path.endswith(".yaml") and entry.is_file():

                cluster_defn, init_scripts = self._build_cluster_defn(
                    entry.path, semantic_version, init_script_path
                )

                with open(entry.path, "w") as file:
                    data = yaml.dump(cluster_defn, file)

                if init_scripts:
                    self._build_init_scripts(init_scripts, entry.name)

    def _build_init_scripts(self, init_scripts: dict, cluster_defn_name: str):

        cluster_defn_name = cluster_defn_name.replace("yaml", "sh")
        from_path = f"{self.databricks_dist_dir}/init_scripts/{cluster_defn_name}"

        for i in init_scripts:

            if i.get("dbfs"):
                to_path = os.path.basename(i["dbfs"]["destination"])
                to_path = f"{self.databricks_dist_dir}/init_scripts/{to_path}"

                with open(from_path, "r") as from_file:
                    with open(to_path, "w") as to_file:
                        shell = from_file.readlines()

                        for l in shell:

                            if l[0:11] == "pip install":
                                path = l.split(" ")[-1]
                                filename = os.path.basename(path)
                                path = path.replace(filename, "")
                                filename, ext = os.path.splitext(filename)
                                wheel = self._get_latest_wheel(self.dist, filename)
                                if wheel:
                                    wheel_filename = os.path.basename(wheel.path)
                                    to_file.write(f"pip install {path}{wheel_filename}")
                                else:
                                    to_file.write(l)
                            else:
                                to_file.write(l)

                os.remove(from_path)

    def _build_cluster_defn(
        self,
        cluster_defn_path: str,
        semantic_version: str = "",
        init_script_path: str = None,
    ):

        with open(cluster_defn_path, "r") as f:
            cluster_defn: dict = yaml.safe_load(f)

        # cluster name variables replacements
        filename, _ = os.path.splitext(os.path.basename(cluster_defn_path))
        spark_version: str = cluster_defn["spark_version"]
        dbr = spark_version.split("-")[0].replace(".x", "")

        cluster_name: str = cluster_defn["cluster_name"]
        cluster_name = cluster_name.replace("{filename}", filename)
        cluster_name = cluster_name.replace("{dbr}", dbr)
        cluster_name = cluster_name.replace("{version}", semantic_version)
        cluster_defn["cluster_name"] = cluster_name

        # cluster log variable replacements
        if cluster_defn.get("cluster_log_conf"):

            log = cluster_defn["cluster_log_conf"].get("dbfs")

            if log:
                log["destination"] = log["destination"].replace(
                    "{cluster_name}", cluster_name
                )

        init_script = None
        # init script variable replacements
        if cluster_defn.get("init_scripts"):

            for i in cluster_defn["init_scripts"]:

                if i.get("dbfs"):

                    to_path = i["dbfs"]["destination"].replace(
                        "{cluster_name}", cluster_name
                    )
                    i["dbfs"]["destination"] = to_path

            init_script = cluster_defn["init_scripts"]

        return cluster_defn, init_script

    def run(self):

        """Run the databricks poject build."""
        self.announce("Building databrics project", level=distutils.log.INFO)

        # copy the directories in the source code home
        # to the dist folder. If the dist folder exists
        # then destroy and re-create it ensuring previous
        # builds are wiped out
        for entry in os.scandir(self.directory):
            if entry.is_dir() and entry.name != "dist":

                # set the destination dir
                to_path = f"{self.databricks_dist_dir}/{entry.name}"

                # copy and overwrite the directory
                self._copy_directory(entry.path, to_path)

        # build the cluster definitions off the copies created in dist
        self._build_cluster_defns(f"{self.databricks_dist_dir}/clusters")
