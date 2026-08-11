from __future__ import annotations

import asyncio
import sys
from abc import abstractmethod
from argparse import Namespace
from pathlib import Path

from diot import Diot
from simpleconf import Config
from panpath import PanPath, GSPath
from rich.logging import RichHandler
from xqute import Xqute, plugin
from xqute.utils import logger
from pipen import __version__ as pipen_version
from pipen_poplog import LogsPopulator

from .version import __version__


def error_and_exit(msg: str) -> None:
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
            from .plugins import XquteCliGbatchPlugin
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
            error_and_exit("No command to run is provided.")

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
            error_and_exit("No command to run is provided.")

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
            error_and_exit(f"Workdir not found: {workdir}")

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
