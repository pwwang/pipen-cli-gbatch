from __future__ import annotations

from pathlib import Path
from typing import Sequence

from panpath import PanPath, GSPath
from pipen.scheduler import XquteGbatchScheduler
from xqute.defaults import DEFAULT_WORKDIR_NAME
from xqute.path import SpecPath
from xqute.utils import sanitize_mounts

from ..conftest import MOCK_MOUNTS_DIR


# class MockAnyPath:
#     def __init__(self, path):
#         self.path = path

#     def __str__(self):
#         return self.path

#     def __truediv__(self, other):
#         return MockAnyPath(f"{self.path}/{other}")

#     def joinpath(self, *args):
#         return MockAnyPath(f"{self.path}/{'/'.join(args)}")

#     def mkdir(self, exist_ok=False, parents=False):
#         pass

#     def exists(self):
#         return True


def mock_isinstance(path, cls):
    if cls == GSPath:
        return str(path).startswith("gs://")
    return isinstance(path, cls)


class MockXquteGbatchScheduler(XquteGbatchScheduler):

    async def post_init(self):
        """Same as the base scheduler, but with the GCS paths mapped to
        the local mock mounts directory (MOCK_MOUNTS_DIR)."""
        mount: list[str] = self._kwargs["mount"] or []
        if not isinstance(mount, Sequence) or isinstance(mount, str):
            mount = [mount]
        else:
            mount = list(mount)

        mount_as_cwd = self._kwargs["mount_as_cwd"]
        if mount_as_cwd:
            mount.insert(0, f"{mount_as_cwd}:{self.DEFAULT_MOUNTED_ROOT}/.cwd")

        mounts, self._path_envs = await sanitize_mounts(
            mount,
            self.DEFAULT_MOUNTED_ROOT,
        )

        workdir_path = PanPath(self._kwargs["workdir"] or DEFAULT_WORKDIR_NAME)
        if mount_as_cwd:
            self.cwd = f"{self.DEFAULT_MOUNTED_ROOT}/.cwd"

            workdir_mount_needed = workdir_path.is_absolute()
            if not workdir_mount_needed:
                self._kwargs["workdir"] = f"{mount_as_cwd}/{workdir_path}"
                self._kwargs["mounted_workdir"] = (
                    self._kwargs["mounted_workdir"]
                    or f"{self.cwd}/{workdir_path}"
                )

                # If mounted_workdir is set, and it is not under any mounted
                # paths, we need to mount the workdir as well
                if not any(
                    Path(self._kwargs["mounted_workdir"]).is_relative_to(mounted)
                    for _, mounted in mounts
                ):
                    workdir_mount_needed = True
        elif self.cwd:
            cwd = Path(self.cwd)
            workdir_mount_needed = workdir_path.is_absolute()
            if not workdir_mount_needed:
                # get the cloud cwd
                cloud_cwd = None
                for host, mounted in mounts:
                    if cwd.is_relative_to(mounted):
                        cloud_cwd = (
                            host / cwd.relative_to(mounted),
                            mounted / cwd.relative_to(mounted),
                        )
                        break

                if cloud_cwd is None:
                    raise ValueError(
                        "'cwd' is not under any of the mounted paths. "
                        "Please specify 'mount_as_cwd' or ensure `cwd` is "
                        "under a mounted path."
                    )

                self._kwargs["workdir"] = f"{cloud_cwd[0]}/{workdir_path}"
                self._kwargs["mounted_workdir"] = (
                    self._kwargs["mounted_workdir"]
                    or f"{cloud_cwd[1]}/{workdir_path}"
                )

                if not any(
                    Path(self._kwargs["mounted_workdir"]).is_relative_to(mounted)
                    for _, mounted in mounts
                ):
                    workdir_mount_needed = True
        else:
            workdir_mount_needed = True

        if workdir_mount_needed:
            self._kwargs["mounted_workdir"] = (
                self._kwargs["mounted_workdir"]
                or f"{self.DEFAULT_MOUNTED_ROOT}/{DEFAULT_WORKDIR_NAME}"
            )

        self.workdir = SpecPath(
            self._kwargs["workdir"],
            mounted=self._kwargs["mounted_workdir"],
        )

        if not isinstance(self.workdir, GSPath):
            raise ValueError(
                "'gbatch' scheduler requires google cloud storage 'workdir'."
            )

        # Map the workdir to the local mock mounts directory
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

        volumes: list[dict] = self.config["taskGroups"][0]["taskSpec"]["volumes"]

        for host, mounted in mounts:
            if not isinstance(host, GSPath):
                raise ValueError(
                    f"Mount source '{host}' is not a GCS path. "
                    "Please specify a GCS path starting with 'gs://'."
                )

            volumes.append(
                {
                    "gcs": {
                        "remotePath": (
                            f"{MOCK_MOUNTS_DIR}/{'/'.join(host.parts[1:])}"
                        )
                    },
                    "mountPath": str(mounted),
                }
            )

        if workdir_mount_needed:
            volumes.insert(
                int(bool(mount_as_cwd)),
                {
                    "gcs": {"remotePath": str(self.workdir)},
                    "mountPath": str(self.workdir.mounted),
                },
            )
