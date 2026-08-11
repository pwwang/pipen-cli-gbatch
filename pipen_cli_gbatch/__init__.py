"""A pipen cli plugin to run command via Google Cloud Batch.

The idea is to wrap the command as a single-process pipen (daemon) pipeline and use
the gbatch scheduler to run it on Google Cloud Batch.

For example, to run a command like:
    python myscript.py --input input.txt --output output.txt

You can run it with:
    pipen gbatch -- python myscript.py --input input.txt --output output.txt

In order to provide configurations like we do for a normal pipen pipeline, you
can also provide a config file (the [cli-gbatch] section will be used):
    pipen gbatch @config.toml -- \\
        python myscript.py --input input.txt --output output.txt

We can also use the --nowait option to run the command in a detached mode:
    pipen gbatch --nowait -- \\
        python myscript.py --input input.txt --output output.txt

Or by default, it will wait for the command to complete:
    pipen gbatch -- \\
        python myscript.py --input input.txt --output output.txt

while waiting the running logs will be pulled and shown in the terminal.

Because teh demon pipeline is running on Google Cloud Batch, so a Google Storage
Bucket path is required for the workdir. For example: gs://my-bucket/workdir

A unique job id will be generated per the name (--name) and workdir, so that if
the same command is run again with the same name and workdir, it will not start a
new job, but just attach to the existing job and pull the logs.

if `--name` is not provided in the command line or `cli-gbatch.name` is not
provided from the configuration file, it will try to grab the name (`--name`) from
the command line arguments after `--`, or else use "name" from the root section
of the configuration file, with a "CliGbatchDaemon" suffix. If nothing can be found, a
default name "PipenCliGbatchDaemon" will be used.

When running in the detached mode, one can also pull the logs later by:
    pipen gbatch --view-logs -- \\
        python myscript.py --input input.txt --output output.txt

Then a workdir `{workdir}/<daemon pipeline name>/` will be created to store the
meta information.

One can have some default configuration file for the daemon pipeline in either/both
the user home directory `~/.pipen.toml` or the current working directory
`./.pipen.toml`. The configurations in these files will be overridden by
the command line arguments.

The API can also be used to run commands programmatically:

    >>> from pipen_cli_gbatch import CliGbatchDaemon
    >>> pipe = CliGbatchDaemon(config_for_daemon, command)
    >>> await pipe.run()

Note that the daemon pipeline will always be running without caching, so that the
command will always be executed when the pipeline is run.
"""
# pyright: reportOptionalOperand=false
from __future__ import annotations
from abc import abstractmethod
from typing import Sequence

import re
import sys
import json
import asyncio
import hashlib
from contextlib import suppress
from pathlib import Path
from typing import Any
from diot import Diot
from argx import Namespace
from panpath import LocalPath, PanPath, GSPath
from simpleconf import Config, ProfileConfig
from slugify import slugify
from rich.logging import RichHandler
from xqute import Xqute, plugin, defaults as xqute_defaults
from xqute.utils import logger
from pipen import __version__ as pipen_version
from pipen.defaults import CONFIG_FILES
from pipen.cli import AsyncCLIPlugin
from pipen.scheduler import GbatchScheduler
from pipen_poplog import LogsPopulator
from pipen_args.parser_ import _pre_parse

__version__ = "1.2.0a9"
__all__ = ("CliGbatchPlugin", "CliGbatchDaemonPlain", "CliGbatchDaemonPipeline")


def _error_and_exit(msg: str) -> None:
    """Print error message and exit."""
    from rich import traceback, console

    cons = console.Console()
    traceback.install(
        width=cons.width,
        code_width=cons.width - 10,
        extra_lines=1,
        suppress=[asyncio],
    )
    raise ValueError(f"{msg}\n")


class CliGbatchDaemonMixin:
    """A mixin class for the CliGbatchDaemon to provide common functionality.

    This mixin provides methods for handling workdir and outdir configurations,
    inferring job name prefixes, and managing command arguments. It is intended
    to be used in conjunction with the CliGbatchDaemon class.
    """

    def __init__(self, config: dict | Namespace, command: list[str]):
        self.config = (
            Diot(vars(config))
            if isinstance(config, Namespace)
            else Diot(config)
        )

        self.mount_as_cwd = (
            self.config.get("mount_as_cwd") or self.config.get("volume_as_cwd")
        )
        if self.mount_as_cwd:
            self.mount_as_cwd = PanPath(self.mount_as_cwd)

        self.config.prescript = self.config.get("prescript", None) or ""
        self.config.postscript = self.config.get("postscript", None) or ""
        if "labels" in self.config and isinstance(self.config.labels, list):
            self.config.labels = {
                key: val
                for key, val in (item.split("=", 1) for item in self.config.labels)
            }
        self.command = command
        # cache for command arguments
        self._command_args: dict = {}
        # envs sent to the command, can be used in the future to pass some information
        # to the command without using command line arguments
        self.envs: dict = {}

    @property
    @abstractmethod
    def daemon_name(self) -> str:
        """Infer the daemon name from configuration or command arguments."""

    @abstractmethod
    async def handle_workdir(self):
        """Handle workdir configuration and mounting.

        Validates that workdir is a Google Storage bucket path and sets up
        the appropriate mount configuration for the container.

        We only need to determine the mounted workdir and replace the --workdir value
        in the command with the mounted workdir. Since the workdir will be handled
        and mounted by Xqute.
        """

    @abstractmethod
    async def jobname_prefix(self) -> str:
        """Infer the job name prefix for the Google Cloud Batch scheduler.

        Priority order:
        1. config.jobname_prefix
        2. "pipen-gbatch-" prefix + --name from command (lowercased, sanitized,
            and truncated if necessary)
        """

    async def _get_arg_from_command(self, arg: str) -> str | None:
        """Get the value of the given argument from the command line.

        Args:
            arg: The argument name to search for (without '--' prefix).

        Returns:
            The value of the argument if found, None otherwise.

        Raises:
            FileNotFoundError: If a config file is specified but doesn't exist.
        """
        if arg in self._command_args:
            return self._command_args[arg]

        cmd_equal = [cmd.startswith(f"--{arg}=") for cmd in self.command]
        cmd_space = [cmd == f"--{arg}" for cmd in self.command]
        cmd_at = [cmd.startswith("@") for cmd in self.command]

        if any(cmd_equal):
            index = cmd_equal.index(True)
            value = self.command[index].split("=", 1)[1]
        elif any(cmd_space) and len(cmd_space) > cmd_space.index(True) + 1:
            index = cmd_space.index(True)
            value = self.command[index + 1]
        elif any(cmd_at):
            index = cmd_at.index(True)
            config_file = PanPath(self.command[index][1:])
            if not await config_file.a_exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")
            # content = await config_file.a_read_text()
            conf = await Config.a_load_one(config_file)
            value = conf.get(arg, None)
        else:
            value = None

        self._command_args[arg] = value
        return value

    def _add_mount(self, source: str | GSPath, target: str) -> None:
        """Add a mount point to the configuration.

        Args:
            source: The source path (local or GCS path).
            target: The target mount path inside the container.
        """
        mount = self.config.get("mount", [])
        if not isinstance(mount, (list, tuple, set)):
            mount = [mount]
        else:
            mount = list(mount)
        # mount the workdir
        mount.append(f"{source}:{target}")

        self.config["mount"] = mount

    async def _get_xqute(self, stdout_file: Path | None = None) -> Xqute:
        """Create and configure an Xqute instance for job execution.

        Returns:
            Configured Xqute instance with appropriate plugins and scheduler options.
        """
        plugins: list = ["-xqute.pipen"]
        if (
            not self.config.get("nowait")
            and not self.config.get("view_logs")
            and "logging" not in plugin.get_all_plugin_names()
        ):
            plugins.append(XquteCliGbatchPlugin(stdout_file=stdout_file))

        return Xqute(
            "gbatch",
            error_strategy=self.config.get("error_strategy"),
            num_retries=self.config.get("num_retries"),
            jobname_prefix=self.config.get("jobname_prefix"),
            scheduler_opts={
                key: val
                for key, val in self.config.items()
                if key
                not in (
                    "workdir",
                    "error_strategy",
                    "num_retries",
                    "jobname_prefix",
                    "COMMAND",
                    "nowait",
                    "view_logs",
                    "command",
                    "name",
                    "profile",
                    "version",
                    "loglevel",
                    "mounts",
                    "plain",
                )
            },
            workdir=f'{self.config.get("workdir")}/{self.daemon_name}',
            plugins=plugins,
        )

    def _run_version(self):
        """Print version information for pipen-cli-gbatch and pipen."""
        print(f"pipen-cli-gbatch version: v{__version__}")
        print(f"pipen version: v{pipen_version}")

    def _show_versions(self):
        """Log the version information for debugging purposes."""
        logger.info(f"pipen version: v{pipen_version}")
        logger.info(f"pipen-cli-gbatch version: v{__version__}")

    def _show_scheduler_opts(self):
        """Log the scheduler options for debugging purposes."""
        logger.info("Scheduler Options:")
        for key, val in self.config.items():
            if key in (
                "workdir",
                "error_strategy",
                "num_retries",
                "jobname_prefix",
                "COMMAND",
                "nowait",
                "view_logs",
                "command",
                "name",
                "profile",
                "version",
                "loglevel",
                "mounts",
                "plain",
            ):
                continue

            logger.info(f"- {key}: {val}")

    async def setup(self):
        """Set up logging and configuration for the daemon.

        Configures logging handlers and filters, validates workdir requirements,
        and initializes daemon name and job name prefix.

        Raises:
            SystemExit: If workdir is not a valid Google Storage bucket path.
        """
        logger.addHandler(RichHandler(show_path=False, show_time=False))
        # logger.addFilter(DuplicateFilter())
        logger.setLevel(self.config.get("loglevel", "INFO").upper())

        await self.handle_workdir()
        self.config["jobname_prefix"] = await self.jobname_prefix()

    async def _run_wait(self, stdout_file: Path | None = None):
        """Run the pipeline and wait for completion.

        Raises:
            SystemExit: If no command is provided.
        """
        if not self.command:
            _error_and_exit("No command to run is provided.")

        xqute = await self._get_xqute(stdout_file=stdout_file)
        job = await xqute.scheduler.create_job(0, self.command, envs=self.envs)
        if await xqute.scheduler.job_is_running(job):
            await self._run_nowait(xqute)
            return

        await xqute.feed(self.command, envs=self.envs)
        await xqute.run_until_complete()

    async def _run_nowait(
        self,
        xqute: Xqute | None = None,
        stdout_file: Path | None = None,
    ):
        """Run the pipeline without waiting for completion.

        Submits the job to Google Cloud Batch and prints information about
        how to monitor the job status and retrieve logs.

        Raises:
            SystemExit: If no command is provided.
        """
        """Run the pipeline without waiting for completion."""
        if not self.command:
            _error_and_exit("No command to run is provided.")

        xqute = xqute or await self._get_xqute(stdout_file=stdout_file)

        try:
            job = await xqute.scheduler.create_job(0, self.command, envs=self.envs)
            jid = await job.get_jid()
            if await xqute.scheduler.job_is_running(job):
                logger.info(f"Job is already submited or running: {jid}")
                logger.info("")
                logger.info("To cancel the job, run:")
                logger.info(
                    "> gcloud batch jobs cancel "
                    f"--location {xqute.scheduler.location} {jid}"  # type: ignore
                )
            else:
                await xqute.scheduler.submit_job_and_update_status(job)
                if jid is None:
                    jid = await job.get_jid()
                logger.info(f"Job is running in a detached mode: {jid}")

            logger.info("")
            logger.info("To check the job status, run:")
            logger.info(
                "💻> gcloud batch jobs describe"
                f" --location {xqute.scheduler.location} {jid}"  # type: ignore
            )
            logger.info("")
            # Use the scheduler-resolved workdir, so that a relative workdir
            # (e.g. resolved against the cwd mount when mount_as_cwd is used)
            # can be used to view the logs later
            resolved_workdir = xqute.scheduler.workdir.parent
            logger.info("To pull the logs from both stdout and stderr, run:")
            logger.info(
                f"💻> pipen gbatch --view-logs all"
                f" --name {self.config['name']}"
                f" --workdir {resolved_workdir}"
            )
            logger.info("To pull the logs from stdout only, run:")
            logger.info(
                f"💻> pipen gbatch --view-logs stdout"
                f" --name {self.config['name']}"
                f" --workdir {resolved_workdir}"
            )
            logger.info("To pull the logs from stderr only, run:")
            logger.info(
                f"💻> pipen gbatch --view-logs stderr"
                f" --name {self.config['name']}"
                f" --workdir {resolved_workdir}"
            )
            logger.info("")
            logger.info("To check the meta information of the daemon job, go to:")
            logger.info(f"📁 {xqute.scheduler.workdir}/0/")
            logger.info("")
        finally:
            if xqute.plugin_context:
                xqute.plugin_context.__exit__()

    async def _run_view_logs(self):
        """Pull and display logs from the Google Cloud Batch job.

        Continuously monitors and displays stdout/stderr logs based on the
        view_logs configuration. Supports viewing 'stdout', 'stderr', or 'all'.

        Raises:
            SystemExit: If workdir is not found or when interrupted by user.
        """
        log_source = {}
        workdir = PanPath(self.config["workdir"]) / self.config["name"] / "0"
        if not await workdir.a_exists():
            _error_and_exit(f"Workdir not found: {workdir}")

        if self.config.view_logs == "stdout":
            log_source["STDOUT"] = workdir.joinpath("job.stdout")
        elif self.config.view_logs == "stderr":
            log_source["STDERR"] = workdir.joinpath("job.stderr")
        else:  #
            log_source["STDOUT"] = workdir.joinpath("job.stdout")
            log_source["STDERR"] = workdir.joinpath("job.stderr")

        poplulators = {
            key: LogsPopulator(logfile=val) for key, val in log_source.items()
        }

        logger.info(f"Pulling logs from: {', '.join(log_source.keys())}")
        logger.info("Press Ctrl-C (twice if needed) to stop.")
        print("")

        try:
            while True:
                for key, populator in poplulators.items():
                    lines = await populator.populate()
                    for line in lines:
                        if len(log_source) > 1:
                            print(f"/{key} {line}")
                        else:
                            print(line)
                await asyncio.sleep(5)
        except KeyboardInterrupt:
            for key, populator in poplulators.items():
                if populator.residue:
                    if len(log_source) > 1:
                        print(f"/{key} {populator.residue.decode()}")
                    else:
                        print(populator.residue.decode())
            print("")
            logger.info("Stopped pulling logs.")
            sys.exit(0)

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
        self._show_versions()
        self._show_scheduler_opts()
        if self.config.get("nowait"):
            await self._run_nowait()
        elif self.config.get("view_logs"):
            await self._run_view_logs()
        else:
            await self._run_wait()


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
            _error_and_exit(
                "A Google Storage Bucket path is required for --workdir "
                "or 'workdir' in configuration file."
            )

        if workdir.is_absolute() and not isinstance(workdir, GSPath):
            _error_and_exit(
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
            _error_and_exit(
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
                _error_and_exit(
                    "The 'cwd' option cannot be used when 'mount_as_cwd' is set "
                    "for `pipen gbatch`."
                )

            self.cwd = PanPath(self.cwd)
            if not isinstance(self.cwd, LocalPath):
                _error_and_exit(
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
                    _error_and_exit(
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
            _error_and_exit(
                "`mount_as_cwd` or `cwd` is required for relative workdir, "
                "or a Google Storage Bucket path is required for --workdir or "
                "'workdir' in configuration file for the pipeline."
            )

        if workdir.is_absolute() and not isinstance(workdir, GSPath):
            _error_and_exit(
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
            _error_and_exit(
                "`mount_as_cwd` or `cwd` is required for relative outdir, "
                "or a Google Storage Bucket path is required for --outdir or "
                "'outdir' in configuration file for the pipeline."
            )

        if command_outdir.is_absolute() and not isinstance(command_outdir, GSPath):
            _error_and_exit(
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
            _error_and_exit(
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


class XquteCliGbatchPlugin:
    """Plugin for pulling logs during pipeline execution.

    This plugin monitors job execution and continuously pulls stdout/stderr logs
    from the Google Cloud Batch job, displaying them in real-time during execution.

    Attributes:
        name (str): The plugin name.
        stdout_populator (LogsPopulator): Handles stdout log population.
        stderr_populator (LogsPopulator): Handles stderr log population.
    """

    def __init__(
        self,
        name: str = "logging",
        stdout_file: str | Path | GSPath | None = None,
    ):
        """Initialize the logging plugin.

        Args:
            name: The plugin name.
            log_start: Whether to start logging when job starts.
        """
        self.name = name
        self.stdout_file = stdout_file
        self.stdout_populator = LogsPopulator()
        self.stderr_populator = LogsPopulator()

    def _clear_residues(self):
        """Clear any remaining log residues and display them."""
        if self.stdout_populator and self.stdout_populator.residue:
            logger.info(f"/STDOUT {self.stdout_populator.residue.decode()}")
            self.stdout_populator.residue = ""
        if self.stderr_populator and self.stderr_populator.residue:
            logger.error(f"/STDERR {self.stderr_populator.residue.decode()}")
            self.stderr_populator.residue = ""

    @plugin.impl
    async def on_job_started(self, scheduler, job):
        """Handle job start event by setting up log file paths.

        Args:
            scheduler: The scheduler instance.
            job: The job that started.
        """
        logger.info("Job is picked up by Google Batch, pulling stdout/stderr ...")
        if not self.stdout_file:
            self.stdout_populator.logfile = scheduler.workdir.joinpath(  # type: ignore
                "0", "job.stdout"
            )
        elif not await self.stdout_file.a_exists():  # type: ignore
            logger.warning(f"Running logs file not found: {self.stdout_file}")
            logger.warning("  Waiting for it to be created ...")
            i = 0
            while not await self.stdout_file.a_exists():  # type: ignore
                await asyncio.sleep(3)
                i += 1
                if i >= 20:
                    break

            if not await self.stdout_file.a_exists():  # type: ignore
                logger.warning(
                    "  Still not found, "
                    "falling back to pull stdout/stderr from daemon ..."
                )
                logger.warning(
                    "  Make sure pipen-log2file plugin is enabled for your pipeline."
                )
                logger.warning(
                    "  Or use --plain if you are not running a pipen pipeline."
                )
                self.stdout_populator.logfile = (  # type: ignore
                    scheduler.workdir.joinpath("0", "job.stdout")
                )
            else:
                logger.info("  Found the running logs, pulling ...")
                self.stdout_populator.logfile = (  # type: ignore
                    await self.stdout_file.a_resolve()  # type: ignore
                    if await self.stdout_file.a_is_symlink()  # type: ignore
                    else self.stdout_file
                )
        else:
            self.stdout_populator.logfile = (  # type: ignore
                await self.stdout_file.a_resolve()  # type: ignore
                if await self.stdout_file.a_is_symlink()  # type: ignore
                else self.stdout_file
            )

        self.stderr_populator.logfile = scheduler.workdir.joinpath(  # type: ignore
            "0",
            "job.stderr",
        )

    @plugin.impl
    async def on_job_polling(self, scheduler, job, counter):
        """Handle job polling event by pulling and displaying logs.

        Args:
            scheduler: The scheduler instance.
            job: The job being polled.
            counter: The polling counter.
        """
        if counter % 5 != 0:
            # Make it less frequent
            return

        if self.stdout_populator:
            stdout_lines = await self.stdout_populator.populate()  # type: ignore
            self.stdout_populator.increment_counter(len(stdout_lines))  # type: ignore
            for line in stdout_lines:
                logger.info(f"/STDOUT {line}")

        if self.stderr_populator:
            stderr_lines = await self.stderr_populator.populate()
            self.stderr_populator.increment_counter(len(stderr_lines))
            for line in stderr_lines:
                logger.error(f"/STDERR {line}")

    @plugin.impl
    async def on_job_killed(self, scheduler, job):
        """Handle job killed event by pulling final logs.

        Args:
            scheduler: The scheduler instance.
            job: The job that was killed.
        """
        await self.on_job_polling(scheduler, job, 0)
        self._clear_residues()

    @plugin.impl
    async def on_job_failed(self, scheduler, job):
        """Handle job failed event by pulling final logs.

        Args:
            scheduler: The scheduler instance.
            job: The job that failed.
        """
        with suppress(AttributeError, FileNotFoundError):
            # in case the job failed before started
            await self.on_job_polling(scheduler, job, 0)
        self._clear_residues()

    @plugin.impl
    async def on_job_succeeded(self, scheduler, job):
        """Handle job succeeded event by pulling final logs.

        Args:
            scheduler: The scheduler instance.
            job: The job that succeeded.
        """
        with suppress(AttributeError, FileNotFoundError):
            await self.on_job_polling(scheduler, job, 0)
        self._clear_residues()

    @plugin.impl
    def on_shutdown(self, xqute, sig):
        """Handle shutdown event by cleaning up resources.

        Args:
            xqute: The Xqute instance.
            sig: The shutdown signal.
        """
        # we need to await self.stdout_populator.destroy() but on_shutdown
        # cannot be async. Since the event loop is already running, we need to
        # create tasks instead of using run_until_complete
        if self.stdout_populator:
            asyncio.create_task(self.stdout_populator.destroy())
            self.stdout_populator = None
        if self.stderr_populator:
            asyncio.create_task(self.stderr_populator.destroy())
            self.stderr_populator = None


class CliGbatchPlugin(AsyncCLIPlugin):
    """Simplify running commands via Google Cloud Batch.

    This CLI plugin provides a command-line interface for executing arbitrary
    commands on Google Cloud Batch through the pipen framework. It wraps
    commands as single-process pipelines and provides various execution modes.
    """

    __version__ = __version__
    name = "gbatch"  # type: ignore

    @classmethod
    async def _get_defaults_from_config(
        cls,
        config_files: Sequence[str | Path],
        profile: str | None,
    ) -> dict:
        """Get the default configurations from the given config files and profile.

        Args:
            config_files: List of configuration file paths to load.
            profile: The profile name to use for configuration.

        Returns:
            Dictionary containing scheduler options from the configuration.
        """
        if not profile:
            return {}

        conf = await ProfileConfig.a_load(
            *config_files,
            ignore_nonexist=True,
            allow_missing_base=True,
        )
        conf = ProfileConfig.use_profile(conf, profile, allow_missing_base=True)
        conf = ProfileConfig.detach(conf)
        return conf

    def __init__(self, parser, subparser):
        """Initialize the CLI plugin with argument parsing configuration.

        Args:
            parser: The main argument parser.
            subparser: The subparser for this specific command.
        """
        super().__init__(parser, subparser)
        subparser.usage = "pipen gbatch [options] -- <command>"
        subparser.pre_parse = _pre_parse  # type: ignore
        subparser.epilog = """\033[1;4mExamples\033[0m:

  \u200b
  # Run a command and wait for it to complete
  > pipen gbatch --mount-as-cwd gs://my-bucket/workdir -- \\
      python myscript.py --input input.txt --output output.txt

  \u200b
  # Use named mounts
  > pipen gbatch --mount-as-cwd  gs://my-bucket/workdir \\
      --mount INFILE=gs://bucket/path/to/file \\
      --mount OUTDIR=gs://bucket/path/to/outdir -- \\
      bash -c 'cat $INFILE > $OUTDIR/output.txt'

  \u200b
  # Run a command in a detached mode
  > pipen gbatch --nowait --project $PROJECT --location $LOCATION \\
      --workdir gs://my-bucket/workdir -- \\
      python myscript.py --input input.txt --output output.txt

  \u200b
  # If you have a profile defined in ~/.pipen.toml or ./.pipen.toml
  # `scheduler_opts` in the profile will be used to start the daemon,
  # other options will be brought as default to the pipen pipeline by the command
  > pipen gbatch --profile myprofile -- \\
      python myscript.py --input input.txt --output output.txt

  \u200b
  # View the logs of a previously run command
  > pipen gbatch --view-logs all --name my-daemon-name \\
      --workdir gs://my-bucket/workdir
        """  # noqa: E501

        """Add command-line arguments specific to the gbatch plugin."""
        argfile = PanPath(__file__).parent / "daemon_args.toml"
        args_def = Config.load(argfile, loader="toml")
        mutually_exclusive_groups = args_def.get("mutually_exclusive_groups", [])
        groups = args_def.get("groups", [])
        arguments = args_def.get("arguments", [])
        self.subparser._add_decedents(
            mutually_exclusive_groups, groups, [], arguments, []
        )

    async def parse_args(self, known_parsed, unparsed_argv: list[str]) -> Namespace:
        """Parse command-line arguments and apply configuration defaults.

        Args:
            known_parsed: Previously parsed arguments.
            unparsed_argv: List of unparsed command-line arguments.

        Returns:
            Namespace containing parsed arguments with applied defaults.

        Raises:
            SystemExit: If command arguments are not properly formatted.
        """
        # Check if there is any unknown args
        known_parsed = await super().parse_args(known_parsed, unparsed_argv)
        # pipen gbatch with no arguments
        if not hasattr(known_parsed, "command"):
            self.subparser.print_help()
            sys.exit(0)

        if known_parsed.command:
            if known_parsed.command[0] != "--":
                _error_and_exit("The command to run must be after '--'.")

            known_parsed.command = known_parsed.command[1:]

        defaults = await self.__class__._get_defaults_from_config(
            CONFIG_FILES,
            known_parsed.profile,
        )
        default_scheduler_opts = defaults.pop("scheduler_opts", {})

        def is_valid(val: Any) -> bool:
            """Check if a value is valid (not None, not empty string, not empty list).
            """
            if val is None:
                return False
            if isinstance(val, bool):
                return True
            return bool(val)

        # update parsed with the defaults
        for key, val in default_scheduler_opts.items():
            if key == "mount" and val and getattr(known_parsed, key, None):
                if not isinstance(val, (tuple, list)):
                    val = [val]
                val = list(val)

                kp_mount = getattr(known_parsed, key)
                val.extend(kp_mount)
                setattr(known_parsed, key, val)
                continue

            if (
                key == "command"
                or val is None
                or is_valid(getattr(known_parsed, key, None))
            ):
                continue

            setattr(known_parsed, key, val)

        if not getattr(known_parsed, "plain", None):
            setattr(known_parsed, "_other_opts", defaults)
        return known_parsed

    async def exec_command(self, args: Namespace) -> None:
        """Execute the gbatch command with the provided arguments.

        Args:
            args: Parsed command-line arguments containing configuration and command.
        """
        if args.plain:
            await CliGbatchDaemonPlain(args, args.command).run()
        else:
            await CliGbatchDaemonPipeline(args, args.command).run()
