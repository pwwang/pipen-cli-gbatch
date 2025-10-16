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

from __future__ import annotations

import sys
import asyncio
from contextlib import suppress
from pathlib import Path
from time import sleep
from typing import Any
from diot import Diot
from argx import Namespace
from yunpath import AnyPath, GSPath
from simpleconf import Config, ProfileConfig
from xqute import Xqute, plugin
from xqute.utils import logger, RichHandler, DuplicateFilter
from pipen import __version__ as pipen_version
from pipen.defaults import CONFIG_FILES
from pipen.cli import CLIPlugin
from pipen.scheduler import GbatchScheduler
from pipen_poplog import LogsPopulator

__version__ = "0.1.2"
__all__ = ("CliGbatchPlugin", "CliGbatchDaemon")
MOUNTED_CWD = "/mnt/disks/.cwd"


class CliGbatchDaemon:
    """A daemon pipeline wrapper for running commands via Google Cloud Batch.

    This class wraps arbitrary commands as single-process pipen pipelines and executes
    them using the Google Cloud Batch scheduler. It handles configuration management,
    path mounting, and provides both synchronous and asynchronous execution modes.

    Attributes:
        config (Diot): Configuration dictionary containing all daemon settings.
        command (list[str]): The command to be executed as a list of arguments.

    Example:
        >>> daemon = CliGbatchDaemon(
        ...     {"workdir": "gs://my-bucket/workdir", "project": "my-project"},
        ...     ["python", "script.py", "--input", "data.txt"]
        ... )
        >>> await daemon.run()
    """

    def __init__(self, config: dict | Namespace, command: list[str]):
        """Initialize the CliGbatchDaemon.

        Args:
            config: Configuration dictionary or Namespace containing daemon settings.
                Must include 'workdir' pointing to a Google Storage bucket path.
            command: List of command arguments to execute.
        """
        if isinstance(config, Namespace):
            self.config = Diot(vars(config))
        else:
            self.config = Diot(config)

        self.mount_as_cwd = self.config.pop("mount_as_cwd", None)
        if self.mount_as_cwd:
            if self.config.cwd:
                print(
                    "\033[1;4mError\033[0m: --mount-as-cwd cannot be used with "
                    "--cwd at the same time.\n"
                )
                sys.exit(1)
            self.config.cwd = MOUNTED_CWD
            self._add_mount(self.mount_as_cwd, MOUNTED_CWD)

        self.config.prescript = self.config.get("prescript", None) or ""
        self.config.postscript = self.config.get("postscript", None) or ""
        if "labels" in self.config and isinstance(self.config.labels, list):
            self.config.labels = {
                key: val
                for key, val in (item.split("=", 1) for item in self.config.labels)
            }
        self.command = command
        self._command_workdir = None

    def _get_arg_from_command(self, arg: str) -> str | None:
        """Get the value of the given argument from the command line.

        Args:
            arg: The argument name to search for (without '--' prefix).

        Returns:
            The value of the argument if found, None otherwise.

        Raises:
            FileNotFoundError: If a config file is specified but doesn't exist.
        """
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
            config_file = AnyPath(self.command[index][1:])
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")

            conf = Config.load_one(config_file)
            value = conf.get(arg, None)
        else:
            value = None

        return value

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

    def _handle_workdir(self):
        """Handle workdir configuration and mounting.

        Validates that workdir is a Google Storage bucket path and sets up
        the appropriate mount configuration for the container.

        Raises:
            SystemExit: If workdir is not a valid Google Storage bucket path.
        """
        command_name = self._get_arg_from_command("name") or self.config["name"]
        from_mount_as_cwd = self.mount_as_cwd and not self.config.workdir
        if from_mount_as_cwd:
            self.config.workdir = f"{self.mount_as_cwd}/.pipen/{command_name}"

        # self._command_workdir to save the original command workdir
        self._command_workdir = workdir = (
            self.config.get("workdir", None) or self._get_arg_from_command("workdir")
        )

        if not workdir or not isinstance(AnyPath(workdir), GSPath):
            print(
                "\033[1;4mError\033[0m: An existing Google Storage Bucket path is "
                "required for --workdir.\n"
            )
            sys.exit(1)

        self.config["workdir"] = workdir
        if from_mount_as_cwd:  # already mounted
            self._replace_arg_in_command("workdir", f"{MOUNTED_CWD}/.pipen")
        else:
            # If command workdir is different from config workdir, we need to mount it
            self._add_mount(workdir, GbatchScheduler.MOUNTED_METADIR)

            # replace --workdir value with the mounted workdir in the command
            self._replace_arg_in_command("workdir", GbatchScheduler.MOUNTED_METADIR)

    def _handle_outdir(self):
        """Handle output directory configuration and mounting.

        If an output directory is specified in the command, mounts it to the
        container and updates the command to use the mounted path.
        """
        command_outdir = self._get_arg_from_command("outdir")

        if command_outdir:
            coudir = AnyPath(command_outdir)
            if (
                not isinstance(coudir, GSPath)
                and not coudir.is_absolute()
                and self.mount_as_cwd
            ):
                self._replace_arg_in_command("outdir", f"{MOUNTED_CWD}/{coudir}")
            else:
                self._add_mount(command_outdir, GbatchScheduler.MOUNTED_OUTDIR)
                self._replace_arg_in_command("outdir", GbatchScheduler.MOUNTED_OUTDIR)
        elif self.mount_as_cwd:
            command_name = self._get_arg_from_command("name") or self.config.name
            self._replace_arg_in_command(
                "outdir",
                f"{MOUNTED_CWD}/{command_name}-output",
            )

    def _infer_name(self):
        """Infer the daemon name from configuration or command arguments.

        Priority order:
        1. config.name
        2. --name from command + "GbatchDaemon" suffix
        3. Default "PipenCliGbatchDaemon"
        """
        name = self.config.get("name", None)
        if not name:
            command_name = self._get_arg_from_command("name")
            if not command_name:
                name = "PipenCliGbatchDaemon"
            else:
                name = f"{command_name}GbatchDaemon"

        self.config["name"] = name

    def _infer_jobname_prefix(self):
        """Infer the job name prefix for the Google Cloud Batch scheduler.

        Priority order:
        1. config.jobname_prefix
        2. --name from command + "-gbatch-daemon" suffix (lowercase)
        3. Default "pipen-cli-gbatch-daemon"
        """
        prefix = self.config.get("jobname_prefix", None)
        if not prefix:
            command_name = self._get_arg_from_command("name")
            if not command_name:
                prefix = "pipen-cli-gbatch-daemon"
            else:
                prefix = f"{command_name.lower()}-gbatch-daemon"

        self.config["jobname_prefix"] = prefix

    def _get_xqute(self) -> Xqute:
        """Create and configure an Xqute instance for job execution.

        Returns:
            Configured Xqute instance with appropriate plugins and scheduler options.
        """
        plugins = ["-xqute.pipen"]
        if (
            not self.config.nowait
            and not self.config.view_logs
            and "logging" not in plugin.get_all_plugin_names()
        ):
            if self.config.plain:
                # use the stdout file from daemon
                stdout_file = None
            else:
                stdout_file = AnyPath(f"{self._command_workdir}/run-latest.log")

            plugins.append(XquteCliGbatchPlugin(stdout_file=stdout_file))

        return Xqute(
            "gbatch",
            error_strategy=self.config.error_strategy,
            num_retries=self.config.num_retries,
            jobname_prefix=self.config.jobname_prefix,
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
                    "mount_as_cwd",
                    "plain",
                )
            },
            workdir=(f'{self.config.workdir}/{self.config["name"]}'),
            plugins=plugins,
        )

    def _run_version(self):
        """Print version information for pipen-cli-gbatch and pipen."""
        print(f"pipen-cli-gbatch version: v{__version__}")
        print(f"pipen version: v{pipen_version}")

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
                "mount_as_cwd",
                "plain",
            ):
                continue

            logger.info(f"- {key}: {val}")

    async def _run_wait(self):  # pragma: no cover
        """Run the pipeline and wait for completion.

        Raises:
            SystemExit: If no command is provided.
        """
        if not self.command:
            print("\033[1;4mError\033[0m: No command to run is provided.\n")
            sys.exit(1)

        xqute = self._get_xqute()

        await xqute.put(self.command)
        await xqute.run_until_complete()

    async def _run_nowait(self):
        """Run the pipeline without waiting for completion.

        Submits the job to Google Cloud Batch and prints information about
        how to monitor the job status and retrieve logs.

        Raises:
            SystemExit: If no command is provided.
        """
        """Run the pipeline without waiting for completion."""
        if not self.command:
            print("\033[1;4mError\033[0m: No command to run is provided.\n")
            sys.exit(1)

        xqute = self._get_xqute()

        try:
            job = xqute.scheduler.create_job(0, self.command)
            if await xqute.scheduler.job_is_running(job):
                logger.info(f"Job is already submited or running: {job.jid}")
                logger.info("")
                logger.info("To cancel the job, run:")
                logger.info(
                    "> gcloud batch jobs cancel "
                    f"--location {xqute.scheduler.location} {job.jid}"
                )
            else:
                await xqute.scheduler.submit_job_and_update_status(job)
                logger.info(f"Job is running in a detached mode: {job.jid}")

            logger.info("")
            logger.info("To check the job status, run:")
            logger.info(
                "💻> gcloud batch jobs describe"
                f" --location {xqute.scheduler.location} {job.jid}"
            )
            logger.info("")
            logger.info("To pull the logs from both stdout and stderr, run:")
            logger.info(
                f"💻> pipen gbatch --view-logs all"
                f" --name {self.config['name']}"
                f" --workdir {self.config['workdir']}"
            )
            logger.info("To pull the logs from both stdout, run:")
            logger.info(
                f"💻> pipen gbatch --view-logs stdout"
                f" --name {self.config['name']}"
                f" --workdir {self.config['workdir']}"
            )
            logger.info("To pull the logs from both stderr, run:")
            logger.info(
                f"💻> pipen gbatch --view-logs stderr"
                f" --name {self.config['name']}"
                f" --workdir {self.config['workdir']}"
            )
            logger.info("")
            logger.info("To check the meta information of the daemon job, go to:")
            logger.info(f'📁 {self.config["workdir"]}/{self.config["name"]}/0/')
            logger.info("")
        finally:
            if xqute.plugin_context:  # pragma: no cover
                xqute.plugin_context.__exit__()

    def _run_view_logs(self):  # pragma: no cover
        """Pull and display logs from the Google Cloud Batch job.

        Continuously monitors and displays stdout/stderr logs based on the
        view_logs configuration. Supports viewing 'stdout', 'stderr', or 'all'.

        Raises:
            SystemExit: If workdir is not found or when interrupted by user.
        """
        log_source = {}
        workdir = AnyPath(self.config["workdir"]) / self.config["name"] / "0"
        if not workdir.exists():
            print(f"\033[1;4mError\033[0m: Workdir not found: {workdir}\n")
            sys.exit(1)

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
                    lines = populator.populate()
                    for line in lines:
                        if len(log_source) > 1:
                            print(f"/{key} {line}")
                        else:
                            print(line)
                sleep(5)
        except KeyboardInterrupt:
            for key, populator in poplulators.items():
                if populator.residue:
                    if len(log_source) > 1:
                        print(f"/{key} {populator.residue}")
                    else:
                        print(populator.residue)
            print("")
            logger.info("Stopped pulling logs.")
            sys.exit(0)

    def setup(self):
        """Set up logging and configuration for the daemon.

        Configures logging handlers and filters, validates workdir requirements,
        and initializes daemon name and job name prefix.

        Raises:
            SystemExit: If workdir is not a valid Google Storage bucket path.
        """
        logger.addHandler(RichHandler(show_path=False, show_time=False))
        logger.addFilter(DuplicateFilter())
        logger.setLevel(self.config.loglevel.upper())

        if not self.config.plain:
            self._infer_name()
            self._handle_workdir()
            self._handle_outdir()
            self._infer_jobname_prefix()
        else:
            if "name" not in self.config or not self.config.name:
                self.config["name"] = "PipenCliGbatchDaemon"

            if not self.config.workdir and self.mount_as_cwd:
                self.config.workdir = f"{self.mount_as_cwd}/.pipen"

            if not self.config.workdir or not isinstance(
                AnyPath(self.config.workdir),
                GSPath,
            ):
                print(
                    "\033[1;4mError\033[0m: An existing Google Storage Bucket path is "
                    "required for --workdir.\n"
                )
                sys.exit(1)

    async def run(self):  # pragma: no cover
        """Execute the daemon pipeline based on configuration.

        Determines the execution mode based on configuration flags:
        - version: Print version information
        - nowait: Run in detached mode
        - view_logs: Display logs from existing job
        - default: Run and wait for completion
        """
        if self.config.version:
            self._run_version()
            return

        self.setup()
        self._show_scheduler_opts()
        if self.config.nowait:
            await self._run_nowait()
        elif self.config.view_logs:
            self._run_view_logs()
        else:
            await self._run_wait()


class XquteCliGbatchPlugin:  # pragma: no cover
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
            logger.info(f"/STDOUT {self.stdout_populator.residue}")
            self.stdout_populator.residue = ""
        if self.stderr_populator and self.stderr_populator.residue:
            logger.error(f"/STDERR {self.stderr_populator.residue}")
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
            self.stdout_populator.logfile = scheduler.workdir.joinpath(
                "0", "job.stdout"
            )
        elif not self.stdout_file.exists():
            logger.warning(f"Running logs file not found: {self.stdout_file}")
            logger.warning("  Waiting for it to be created ...")
            i = 0
            while not self.stdout_file.exists():
                await asyncio.sleep(3)
                i += 1
                if i >= 20:
                    break

            if not self.stdout_file.exists():
                logger.warning(
                    "  Still not found, falling back to pull logs from daemon ..."
                )
                logger.warning(
                    "  Make sure pipen-log2file plugin is enabled for your pipeline."
                )
                self.stdout_populator.logfile = scheduler.workdir.joinpath(
                    "0", "job.stdout"
                )
            else:
                logger.info("  Found the running logs, pulling ...")
                self.stdout_populator.logfile = (
                    self.stdout_file.resolve()
                    if self.stdout_file.is_symlink()
                    else self.stdout_file
                )
        else:
            self.stdout_populator.logfile = (
                self.stdout_file.resolve()
                if self.stdout_file.is_symlink()
                else self.stdout_file
            )

        self.stderr_populator.logfile = scheduler.workdir.joinpath("0", "job.stderr")

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

        if self.stderr_populator:
            stdout_lines = self.stdout_populator.populate()
            self.stdout_populator.increment_counter(len(stdout_lines))
            for line in stdout_lines:
                logger.info(f"/STDOUT {line}")

        if self.stderr_populator:
            stderr_lines = self.stderr_populator.populate()
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
        del self.stdout_populator
        self.stdout_populator = None
        del self.stderr_populator
        self.stderr_populator = None


class CliGbatchPlugin(CLIPlugin):  # pragma: no cover
    """Simplify running commands via Google Cloud Batch.

    This CLI plugin provides a command-line interface for executing arbitrary
    commands on Google Cloud Batch through the pipen framework. It wraps
    commands as single-process pipelines and provides various execution modes.

    Attributes:
        __version__ (str): The version of the plugin.
        name (str): The CLI command name.
    """

    __version__ = __version__
    name = "gbatch"

    @staticmethod
    def _get_defaults_from_config(
        config_files: list[str],
        profile: str | None,
    ) -> dict:
        """Get the default configurations from the given config files and profile.

        Args:
            config_files: List of configuration file paths to load.
            profile: The profile name to use for configuration.

        Returns:
            Dictionary containing scheduler options from the configuration.
        """
        """Get the default configurations from the given config files and profile."""
        if not profile:
            return {}

        conf = ProfileConfig.load(
            *config_files,
            ignore_nonexist=True,
            base=profile,
            allow_missing_base=True,
        )
        conf = ProfileConfig.detach(conf)
        return conf.get("scheduler_opts", {})

    def __init__(self, parser, subparser):
        """Initialize the CLI plugin with argument parsing configuration.

        Args:
            parser: The main argument parser.
            subparser: The subparser for this specific command.
        """
        super().__init__(parser, subparser)
        subparser.epilog = """\033[1;4mExamples\033[0m:

  \u200b
  # Run a command and wait for it to complete
  > pipen gbatch --workdir gs://my-bucket/workdir -- \\
    python myscript.py --input input.txt --output output.txt

  \u200b
  # Use named mounts
  > pipen gbatch --workdir gs://my-bucket/workdir --mount INFILE=gs://bucket/path/to/file \\
    --mount OUTDIR=gs://bucket/path/to/outdir -- \\
    bash -c 'cat $INFILE > $OUTDIR/output.txt'

  \u200b
  # Run a command in a detached mode
  > pipen gbatch --nowait --project $PROJECT --location $LOCATION \\
    --workdir gs://my-bucket/workdir -- \\
    python myscript.py --input input.txt --output output.txt

  \u200b
  # If you have a profile defined in ~/.pipen.toml or ./.pipen.toml
  > pipen gbatch --profile myprofile -- \\
    python myscript.py --input input.txt --output output.txt

  \u200b
  # View the logs of a previously run command
  > pipen gbatch --view-logs all --name my-daemon-name \\
    --workdir gs://my-bucket/workdir
        """  # noqa: E501
        argfile = Path(__file__).parent / "daemon_args.toml"
        args_def = Config.load(argfile, loader="toml")
        mutually_exclusive_groups = args_def.get("mutually_exclusive_groups", [])
        groups = args_def.get("groups", [])
        arguments = args_def.get("arguments", [])
        subparser._add_decedents(mutually_exclusive_groups, groups, [], arguments, [])

    def parse_args(self, known_parsed, unparsed_argv: list[str]) -> Namespace:
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
        known_parsed = super().parse_args(known_parsed, unparsed_argv)
        if known_parsed.command:
            if known_parsed.command[0] != "--":
                print("\033[1;4mError\033[0m: The command to run must be after '--'.\n")
                sys.exit(1)

            known_parsed.command = known_parsed.command[1:]

        defaults = self.__class__._get_defaults_from_config(
            CONFIG_FILES,
            known_parsed.profile,
        )

        def is_valid(val: Any) -> bool:
            """Check if a value is valid (not None, not empty string, not empty list).
            """
            if val is None:
                return False
            if isinstance(val, bool):
                return True
            return bool(val)

        # update parsed with the defaults
        for key, val in defaults.items():
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

        return known_parsed

    def exec_command(self, args: Namespace) -> None:
        """Execute the gbatch command with the provided arguments.

        Args:
            args: Parsed command-line arguments containing configuration and command.
        """
        asyncio.run(CliGbatchDaemon(args, args.command).run())
