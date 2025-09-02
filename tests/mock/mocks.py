from __future__ import annotations

import getpass
from copy import deepcopy
from pathlib import Path
from typing import Sequence

from pipen.scheduler import XquteGbatchScheduler
from xqute.path import SpecPath
from xqute.schedulers.gbatch_scheduler import (
    DEFAULT_MOUNTED_ROOT,
    JOBNAME_PREFIX_RE,
    NAMED_MOUNT_RE,
    Scheduler,
)
from pipen_cli_gbatch import GSPath

from ..conftest import MOCK_MOUNTS_DIR


class MockAnyPath:
    def __init__(self, path):
        self.path = path

    def __str__(self):
        return self.path

    def __truediv__(self, other):
        return MockAnyPath(f"{self.path}/{other}")

    def joinpath(self, *args):
        return MockAnyPath(f"{self.path}/{'/'.join(args)}")

    def mkdir(self, exist_ok=False, parents=False):
        pass

    def exists(self):
        return True


def mock_isinstance(path, cls):
    if cls == GSPath:
        return str(path).startswith("gs://")
    return isinstance(path, cls)


class MockXquteGbatchScheduler(XquteGbatchScheduler):

    def __init__(
        self,
        *args,
        project: str,
        location: str,
        mount: str | Sequence[str] | None = None,
        service_account: str | None = None,
        network: str | None = None,
        subnetwork: str | None = None,
        no_external_ip_address: bool | None = None,
        machine_type: str | None = None,
        provisioning_model: str | None = None,
        image_uri: str | None = None,
        entrypoint: str = None,
        commands: str | Sequence[str] | None = None,
        runnables: Sequence[dict] | None = None,
        **kwargs,
    ):
        """Construct the gbatch scheduler"""
        self.gcloud = kwargs.pop("gcloud", "gcloud")
        self.project = project
        self.location = location
        kwargs.setdefault("mounted_workdir", f"{DEFAULT_MOUNTED_ROOT}/xqute_workdir")
        Scheduler.__init__(self, *args, **kwargs)

        if not isinstance(self.workdir, GSPath):
            raise ValueError(
                "'gbatch' scheduler requires google cloud storage 'workdir'."
            )

        if not JOBNAME_PREFIX_RE.match(self.jobname_prefix):
            raise ValueError(
                "'jobname_prefix' for gbatch scheduler doesn't follow pattern "
                "^[a-zA-Z][a-zA-Z0-9-]{0,47}$."
            )

        self._path_envs = {}
        task_groups = self.config.setdefault("taskGroups", [])
        if not task_groups:
            task_groups.append({})
        if not task_groups[0]:
            task_groups[0] = {}

        task_spec = task_groups[0].setdefault("taskSpec", {})
        task_runnables = task_spec.setdefault("runnables", [])

        # Process additional runnables with ordering
        additional_runnables = []
        if runnables:
            for runnable_dict in runnables:
                runnable_copy = deepcopy(runnable_dict)
                order = runnable_copy.pop("order", 0)
                additional_runnables.append((order, runnable_copy))

        # Sort by order
        additional_runnables.sort(key=lambda x: x[0])

        # Create main job runnable
        if not task_runnables:
            task_runnables.append({})
        if not task_runnables[0]:
            task_runnables[0] = {}

        job_runnable = task_runnables[0]
        if "container" in job_runnable or image_uri:
            job_runnable.setdefault("container", {})
            if not isinstance(job_runnable["container"], dict):  # pragma: no cover
                raise ValueError(
                    "'taskGroups[0].taskSpec.runnables[0].container' should be a "
                    "dictionary for gbatch configuration."
                )
            if image_uri:
                job_runnable["container"].setdefault("image_uri", image_uri)
            if entrypoint:
                job_runnable["container"].setdefault("entrypoint", entrypoint)

            job_runnable["container"].setdefault("commands", commands or [])
        else:
            job_runnable["script"] = {
                "text": None,  # placeholder for job command
                "_commands": commands,  # Store commands for later use
            }

        # Clear existing runnables and rebuild with proper ordering
        task_runnables.clear()

        # Add runnables with negative order (before job)
        for order, runnable_dict in additional_runnables:
            if order < 0:
                task_runnables.append(runnable_dict)

        # Add the main job runnable
        task_runnables.append(job_runnable)
        self.runnable_index = len(task_runnables) - 1

        # Add runnables with positive order (after job)
        for order, runnable_dict in additional_runnables:
            if order >= 0:
                task_runnables.append(runnable_dict)

        # Only logs the stdout/stderr of submission (when wrapped script doesn't run)
        # The logs of the wrapped script are logged to stdout/stderr files
        # in the workdir.
        logs_policy = self.config.setdefault("logsPolicy", {})
        logs_policy.setdefault("destination", "CLOUD_LOGGING")

        volumes = task_spec.setdefault("volumes", [])
        if not isinstance(volumes, list):
            raise ValueError(
                "'taskGroups[0].taskSpec.volumes' should be a list for "
                "gbatch configuration."
            )

        source_path = f"{MOCK_MOUNTS_DIR}/{str(self.workdir)[5:]}"
        link_path = f"{MOCK_MOUNTS_DIR}{self.workdir.mounted}"
        self.workdir = SpecPath(source_path, mounted=link_path)
        # make symbolic link for workdir
        link_path = Path(link_path)
        link_path.parent.mkdir(parents=True, exist_ok=True)
        if not link_path.exists():
            if link_path.is_symlink():
                link_path.unlink()
            link_path.symlink_to(source_path)

        volumes.insert(
            0,
            {
                "gcs": {"remotePath": str(self.workdir)},
                "mountPath": str(self.workdir.mounted),
            },
        )

        if mount and not isinstance(mount, (tuple, list)):
            mount = [mount]
        if mount:
            for m in mount:
                # Let's check if mount is provided as "OUTDIR=gs://bucket/dir"
                # If so, we mounted it to $DEFAULT_MOUNTED_ROOT/OUTDIR
                # and set OUTDIR env variable to the mounted path in self._path_envs
                if NAMED_MOUNT_RE.match(m):
                    name, gcs = m.split("=", 1)
                    if not gcs.startswith("gs://"):
                        raise ValueError(
                            "When using named mount, it should be in the format "
                            "'NAME=gs://bucket/dir', where NAME matches "
                            "^[A-Za-z][A-Za-z0-9_]*$"
                        )
                    gcs = f"{MOCK_MOUNTS_DIR}/{gcs[5:]}"
                    gcs_path = Path(gcs)
                    # Check if it is a file path
                    if gcs_path.is_file():
                        # Mount the parent directory
                        # gcs = str(gcs_path.parent._no_prefix)
                        mount_path = (
                            f"{DEFAULT_MOUNTED_ROOT}/{name}/{gcs_path.parent.name}"
                        )
                        self._path_envs[name] = f"{mount_path}/{gcs_path.name}"
                    else:
                        # gcs = gcs[5:]
                        mount_path = f"{DEFAULT_MOUNTED_ROOT}/{name}"
                        self._path_envs[name] = mount_path

                    volumes.append(
                        {
                            "gcs": {"remotePath": gcs},
                            "mountPath": mount_path,
                        }
                    )
                else:
                    # Or, we expect a literal mount "gs://bucket/dir:/mount/path"
                    gcs, mount_path = m.rsplit(":", 1)
                    if gcs.startswith("gs://"):
                        gcs = gcs[5:]
                    volumes.append(
                        {
                            "gcs": {"remotePath": gcs},
                            "mountPath": mount_path,
                        }
                    )

        # Add some labels for filtering by `gcloud batch jobs list`
        labels = self.config.setdefault("labels", {})

        labels.setdefault("xqute", "true")
        labels.setdefault("user", getpass.getuser())

        allocation_policy = self.config.setdefault("allocationPolicy", {})

        if service_account:
            allocation_policy.setdefault("serviceAccount", {}).setdefault(
                "email", service_account
            )

        if network or subnetwork or no_external_ip_address is not None:
            network_interface = allocation_policy.setdefault("network", {}).setdefault(
                "networkInterfaces", []
            )
            if not network_interface:
                network_interface.append({})
            network_interface = network_interface[0]
            if network:
                network_interface.setdefault("network", network)
            if subnetwork:
                network_interface.setdefault("subnetwork", subnetwork)
            if no_external_ip_address is not None:
                network_interface.setdefault(
                    "noExternalIpAddress", no_external_ip_address
                )

        if machine_type or provisioning_model:
            instances = allocation_policy.setdefault("instances", [])
            if not instances:
                instances.append({})
            policy = instances[0].setdefault("policy", {})
            if machine_type:
                policy.setdefault("machineType", machine_type)
            if provisioning_model:
                policy.setdefault("provisioningModel", provisioning_model)

        email = allocation_policy.get("serviceAccount", {}).get("email")
        if email:
            # 63 character limit, '@' is not allowed in labels
            # labels.setdefault("email", email[:63])
            labels.setdefault("sacct", email.split("@", 1)[0][:63])
