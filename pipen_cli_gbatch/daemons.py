# pyright: reportOptionalOperand=false
from __future__ import annotations
from typing import Any

import re
import hashlib
import json
from argparse import Namespace
from slugify import slugify
from panpath import PanPath, GSPath, LocalPath
from pipen.scheduler import GbatchScheduler
from xqute import defaults as xqute_defaults
from xqute.utils import logger

from .mixin import CliGbatchDaemonMixin, error_and_exit


class CliGbatchDaemonPlain(CliGbatchDaemonMixin):
    """A daemon pipeline wrapper for running plain commands via Google Cloud Batch.

    This class wraps arbitrary commands as single-process pipen pipelines and executes
    them using the Google Cloud Batch scheduler. It handles configuration management,
    path mounting, and provides both synchronous and asynchronous execution modes.

    Attributes:
        config (Diot): Configuration dictionary containing all daemon settings.
        command (list[str]): The command to be executed as a list of arguments.

    Example:
        >>> daemon = CliGbatchDaemonPlain(
        ...     {"workdir": "gs://my-bucket/workdir", "project": "my-project"},
        ...     ["python", "script.py", "--input", "data.txt"]
        ... )
        >>> await daemon.run()
    """

    @property
    def daemon_name(self) -> str:
        """Infer the daemon name from configuration or command arguments."""
        if self.config.get("name"):
            return self.config["name"]

        self.config["name"] = f".gbatch-{slugify('-'.join(self.command[:2]))}"
        return self.config["name"]

    async def handle_workdir(self):
        """Handle workdir configuration and mounting.

        Validates that workdir is a Google Storage bucket path and sets up
        the appropriate mount configuration for the container.

        We only need to determine the mounted workdir and replace the --workdir value
        in the command with the mounted workdir. Since the workdir will be handled
        and mounted by Xqute.

        Raises:
            SystemExit: If workdir is not a valid Google Storage bucket path.
        """
        # no pipeline name yet
        workdir = PanPath(
            self.config.get("workdir")
            or xqute_defaults.DEFAULT_WORKDIR_NAME
        )

        if not workdir.is_absolute() and not self.mount_as_cwd:
            error_and_exit(
                "A Google Storage Bucket path is required for --workdir "
                "or 'workdir' in configuration file."
            )

        if workdir.is_absolute() and not isinstance(workdir, GSPath):
            error_and_exit(
                "An existing Google Storage Bucket path is "
                "required for --workdir (pipen gbatch --workdir <gs://bucket/path>)"
            )

        if self.mount_as_cwd:  # workdir is relative
            workdir = self.mount_as_cwd / workdir

        # xqute will handle the mounting
        self.config["workdir"] = workdir

    async def jobname_prefix(self) -> str:
        """Infer the job name prefix for the Google Cloud Batch scheduler.

        Priority order:
        1. config.jobname_prefix
        2. "pipen-gbatch-" prefix + --name from command (lowercased, sanitized,
            and truncated if necessary)
        """
        prefix = self.config.get("jobname_prefix", None)

        if not prefix:
            prefix = slugify(self.daemon_name).lstrip("-")
            if len(prefix) > 41:
                hsh = hashlib.sha1(prefix.encode("utf-8")).hexdigest()[:6]
                prefix = f"pipen-{prefix[:35]}-{hsh}"

        if not re.compile(r"^[a-z0-9-]+$").match(prefix) or len(prefix) > 48:
            error_and_exit(
                "Invalid jobname_prefix: must only contain lowercase letters, "
                "numbers, and hyphens; must be 48 characters or less."
            )

        return prefix


class CliGbatchDaemonPipeline(CliGbatchDaemonMixin):
    """A daemon pipeline wrapper for running plain commands via Google Cloud Batch.

    This class wraps arbitrary commands as single-process pipen pipelines and executes
    them using the Google Cloud Batch scheduler. It handles configuration management,
    path mounting, and provides both synchronous and asynchronous execution modes.

    Attributes:
        config (Diot): Configuration dictionary containing all daemon settings.
        command (list[str]): The command to be executed as a list of arguments.

    Example:
        >>> daemon = CliGbatchDaemonPipeline(
        ...     {"workdir": "gs://my-bucket/workdir", "project": "my-project"},
        ...     ["python", "script.py", "--input", "data.txt"]
        ... )
        >>> await daemon.run()
    """

    def __init__(self, config: dict | Namespace, command: list[str]):
        """Initialize the CliGbatchDaemonPipeline.

        Args:
            config: Configuration dictionary or Namespace containing daemon settings.
                Must include 'workdir' pointing to a Google Storage bucket path.
                Other options not recognized by the parser are stored in `_other_opts`.
            command: List of command arguments to execute.
        """
        other_opts: dict = {}
        if isinstance(config, Namespace) and hasattr(config, "_other_opts"):
            other_opts = config._other_opts or {}
            delattr(config, "_other_opts")

        super().__init__(config, command)
        self.cwd = self.config.get("cwd", None)
        if self.cwd:
            if self.mount_as_cwd:
                error_and_exit(
                    "The 'cwd' option cannot be used when 'mount_as_cwd' is set "
                    "for `pipen gbatch`."
                )

            self.cwd = PanPath(self.cwd)
            if not isinstance(self.cwd, LocalPath):
                error_and_exit(
                    "The 'cwd' option must be a local path from inside the VM, "
                    "not a Google Storage path for `pipen gbatch`."
                )

        # Convert other_opts to envs, so that the command can access them
        # as environment variables
        for key, val in other_opts.items():
            if key in ("scheduler", "workdir", "outdir"):
                # scheduler for command should be local in the Gbatch VM
                # workdir/outdir is handled by the daemon
                continue

            if isinstance(val, bool):
                val = f"@bool:{val}"
            elif isinstance(val, int):
                val = f"@int:{val}"
            elif isinstance(val, float):
                val = f"@float:{val}"
            elif val is None:
                val = "@none"
            elif isinstance(val, (list, tuple)):
                val = list(val)
                val = f"@json:{json.dumps(val)}"
            elif isinstance(val, dict):
                val = f"@json:{json.dumps(val)}"
            else:
                val = str(val)

            self.envs[f"PIPEN_{key}"] = val

    def _replace_arg_in_command(self, arg: str, value: Any) -> None:
        """Replace the value of the given argument in the command line.

        Args:
            arg: The argument name to replace (without '--' prefix).
            value: The new value to set for the argument.
        """
        cmd_equal = [cmd.startswith(f"--{arg}=") for cmd in self.command]
        cmd_space = [cmd == f"--{arg}" for cmd in self.command]
        value = str(value)

        if any(cmd_equal):
            index = cmd_equal.index(True)
            self.command[index] = f"--{arg}={value}"
        elif any(cmd_space) and len(cmd_space) > cmd_space.index(True) + 1:
            index = cmd_space.index(True)
            self.command[index + 1] = value
        else:
            self.command.extend([f"--{arg}", value])

    @property
    def daemon_name(self) -> str:
        """Infer the daemon name from configuration or command arguments."""
        if self.config.get("name"):
            return self.config["name"]

        self.config["name"] = ".GbatchDaemon"
        return self.config["name"]

    @property
    def command_workdir(self) -> PanPath:
        """Get the workdir for the command"""
        if not self.config["workdir"].is_absolute():
            if self.mount_as_cwd:
                return self.mount_as_cwd / self.config["workdir"]
            elif self.cwd:
                # We need to get the cloud path, instead of the path in VM
                # The only way is to parse the mounts
                mount: str | list[str] = self.config.get("mount", None) or []
                if not isinstance(mount, (list, tuple)):
                    mount = [mount]

                for mnt in mount:
                    # if not, let xqute handle it
                    if ":" in mnt:
                        src, tgt = mnt.rpartition(":")[::2]
                        if self.cwd.is_relative_to(tgt):
                            src = PanPath(src)
                            rel = self.cwd.relative_to(tgt)
                            return src / rel / self.config["workdir"]
                else:
                    error_and_exit(
                        "Cannot determine the cloud path for the relative workdir "
                        "from `cwd`. Please use `mount_as_cwd` or provide an absolute "
                        "Google Storage Bucket path for --workdir "
                        "or 'workdir' in configuration file for the pipeline."
                    )

        return self.config["workdir"]

    async def command_name(self) -> str:
        """Get the name of the command to be executed."""
        cname = await self._get_arg_from_command("name")
        if not cname:
            raise ValueError(
                "A name is required explicitly via --name or configuration file, "
                "for pipen pipeline to run via `pipen gbatch`."
            )

        return cname

    async def handle_workdir(self):
        """Handle workdir configuration and mounting.

        Validates that workdir is a Google Storage bucket path and sets up
        the appropriate mount configuration for the container.

        We only need to determine the mounted workdir and replace the --workdir value
        in the command with the mounted workdir. Since the workdir will be handled
        and mounted by Xqute.

        Raises:
            SystemExit: If workdir is not a valid Google Storage bucket path.
        """
        # no pipeline name yet
        workdir = PanPath(
            self.config.get("workdir", None)
            or await self._get_arg_from_command("workdir")
            or xqute_defaults.DEFAULT_WORKDIR_NAME
        )

        if not workdir.is_absolute() and not self.mount_as_cwd and not self.cwd:
            error_and_exit(
                "`mount_as_cwd` or `cwd` is required for relative workdir, "
                "or a Google Storage Bucket path is required for --workdir or "
                "'workdir' in configuration file for the pipeline."
            )

        if workdir.is_absolute() and not isinstance(workdir, GSPath):
            error_and_exit(
                "An existing Google Storage Bucket path is "
                "required for --workdir or 'workdir' in configuration file "
                "for the pipeline."
            )

        if self.cwd:
            mounted_workdir = f"{self.cwd}/{workdir}"
        elif self.mount_as_cwd:  # workdir is relative
            mounted_workdir = f"{GbatchScheduler.DEFAULT_MOUNTED_ROOT}/.cwd/{workdir}"
        else:
            mounted_workdir = (
                f"{GbatchScheduler.DEFAULT_MOUNTED_ROOT}/"
                f"{xqute_defaults.DEFAULT_WORKDIR_NAME}"
            )

        self.config["workdir"] = workdir / (await self.command_name())
        self._replace_arg_in_command("workdir", mounted_workdir)

        await self._handle_outdir()

    async def _handle_outdir(self):
        """Handle output directory configuration and mounting.

        We need to determine:
        1. If we need to mount the outdir
        2. Replace the --outdir value in the command with the mounted outdir
        """
        command_name = await self.command_name()
        command_outdir = await self._get_arg_from_command("outdir")
        if not command_outdir:
            command_outdir = f"{command_name}-output"

        command_outdir = PanPath(command_outdir)
        if not command_outdir.is_absolute() and not self.mount_as_cwd and not self.cwd:
            error_and_exit(
                "`mount_as_cwd` or `cwd` is required for relative outdir, "
                "or a Google Storage Bucket path is required for --outdir or "
                "'outdir' in configuration file for the pipeline."
            )

        if command_outdir.is_absolute() and not isinstance(command_outdir, GSPath):
            error_and_exit(
                "An existing Google Storage Bucket path is "
                "required for --outdir or 'outdir' in configuration file "
                "for the pipeline."
            )

        if self.cwd:
            mounted_outdir = f"{self.cwd}/{command_outdir}"
        elif not self.mount_as_cwd:
            mounted_outdir = (
                f"{GbatchScheduler.DEFAULT_MOUNTED_ROOT}/"
                f"{xqute_defaults.DEFAULT_WORKDIR_NAME}-{command_name}-output"
            )
            self._add_mount(str(command_outdir), mounted_outdir)
        else:
            mounted_outdir = (
                f"{GbatchScheduler.DEFAULT_MOUNTED_ROOT}/.cwd/{command_outdir}"
            )

        self._replace_arg_in_command("outdir", mounted_outdir)

    async def jobname_prefix(self) -> str:
        """Infer the job name prefix for the Google Cloud Batch scheduler.

        Priority order:
        1. config.jobname_prefix
        2. "pipen-gbatch-" prefix + --name from command (lowercased, sanitized,
            and truncated if necessary)
        """
        prefix = self.config.get("jobname_prefix", None)
        if not prefix:
            command_name = await self.command_name()
            # The max length of job name in gbatch is 48 characters
            command_name = slugify(command_name).lstrip("-")
            if len(command_name) > 34:
                hsh = hashlib.sha1(command_name.encode()).hexdigest()[:6]
                command_name = f"{command_name[:28]}-{hsh}"
            prefix = f"pipen-gbatch-{command_name}"

        if not re.compile(r"^[a-z0-9-]+$").match(prefix) or len(prefix) > 48:
            error_and_exit(
                "Invalid jobname_prefix: must only contain lowercase letters, "
                "numbers, and hyphens; must be 48 characters or less."
            )

        return prefix

    async def run(self):
        """Execute the daemon pipeline based on configuration.

        Determines the execution mode based on configuration flags:
        - version: Print version information
        - nowait: Run in detached mode
        - view_logs: Display logs from existing job
        - default: Run and wait for completion
        """
        if self.config.get("version"):
            self._run_version()
            return

        await self.setup()
        stdout_file = self.command_workdir / "run-latest.log"
        self._show_versions()
        self._show_scheduler_opts()
        logger.info(f"Daemon workdir: {self.config.get('workdir')}/{self.daemon_name}")
        if self.config.get("nowait"):
            await self._run_nowait(stdout_file=stdout_file)
        elif self.config.get("view_logs"):
            await self._run_view_logs()
        else:
            await self._run_wait(stdout_file=stdout_file)
