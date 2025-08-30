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
from pathlib import Path
from time import sleep
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

__version__ = "0.0.0"
__all__ = ("CliGbatchPlugin", "CliGbatchDaemon")


class XquteCliGbatchPlugin:
    """The plugin used to pull logs for the real pipeline."""

    def __init__(self, name: str = "logging", log_start: bool = True):
        self.name = name
        self.log_start = log_start
        self.stdout_populator = LogsPopulator()
        self.stderr_populator = LogsPopulator()

    @plugin.impl
    async def on_job_started(self, scheduler, job):
        if not self.log_start:
            return

        self.stdout_populator.logfile = scheduler.workdir.joinpath("0", "job.stdout")
        self.stderr_populator.logfile = scheduler.workdir.joinpath("0", "job.stderr")
        logger.info("Job is picked up by Google Batch, pulling stdout/stderr...")

    @plugin.impl
    async def on_job_polling(self, scheduler, job, counter):
        if counter % 5 != 0:
            # Make it less frequent
            return

        stdout_lines = self.stdout_populator.populate()
        self.stdout_populator.increment_counter(len(stdout_lines))
        for line in stdout_lines:
            logger.info(f"/STDOUT {line}")

        stderr_lines = self.stderr_populator.populate()
        self.stderr_populator.increment_counter(len(stderr_lines))
        for line in stderr_lines:
            logger.error(f"/STDERR {line}")

    @plugin.impl
    async def on_job_killed(self, scheduler, job):
        await self.on_job_polling.impl(self, scheduler, job, 0)

    @plugin.impl
    async def on_job_failed(self, scheduler, job):
        await self.on_job_polling.impl(self, scheduler, job, 0)

    @plugin.impl
    async def on_job_succeeded(self, scheduler, job):
        await self.on_job_polling.impl(self, scheduler, job, 0)

    @plugin.impl
    def on_shutdown(self, xqute, sig):
        del self.stdout_populator
        self.stdout_populator = None
        del self.stderr_populator
        self.stderr_populator = None


class CliGbatchDaemon:

    def __init__(self, config: dict | Namespace, command: list[str]):
        if isinstance(config, Namespace):
            self.config = Diot(vars(config))
        else:
            self.config = Diot(config)
        self.command = command

    def _get_arg_from_command(self, arg: str) -> str | None:
        """Get the value of the given argument from the command line."""
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
            value = conf.get("workdir", None)
        else:
            value = None

        return value

    def _check_workdir(self):
        workdir = self.config.get("workdir", self._get_arg_from_command("workdir"))

        if not workdir or not isinstance(AnyPath(workdir), GSPath):
            print(
                "\033[1;4mError\033[0m: A Google Storage Bucket path is required for "
                "--workdir.\n"
            )
            sys.exit(1)

        self.config["workdir"] = workdir

    def _infer_name(self):
        name = self.config.get("name", None)
        if not name:
            command_name = self._get_arg_from_command("name")
            if not command_name:
                name = "PipenCliGbatchDaemon"
            else:
                name = f"{name}GbatchDaemon"

        self.config["name"] = name

    def _infer_jobname_prefix(self):
        prefix = self.config.get("jobname_prefix", None)
        if not prefix:
            command_name = self._get_arg_from_command("name")
            if not command_name:
                prefix = "pipen-gbatch-daemon"
            else:
                prefix = f"{command_name.lower()}-gbatch-daemon"

        self.config["jobname_prefix"] = prefix

    def _setup_mount(self):
        mount = self.config.get("mount", [])
        # mount the workdir
        mount.append(f'{self.config["workdir"]}:{GbatchScheduler.MOUNTED_METADIR}')

        self.config["mount"] = mount

    def _get_xqute(self) -> Xqute:
        plugins = ["-xqute.pipen"]
        if not self.config.nowait and not self.config.view_logs:
            plugins.append(XquteCliGbatchPlugin())

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
                )
            },
            workdir=(f'{self.config.workdir}/{self.config["name"]}'),
            plugins=plugins,
        )

    def _run_version(self):
        print(f"pipen-cli-gbatch version: v{__version__}")
        print(f"pipen version: v{pipen_version}")

    def _show_scheduler_opts(self):
        logger.debug("Scheduler Options:")
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
            ):
                continue

            logger.debug(f"- {key}: {val}")

    async def _run_wait(self):
        if not self.command:
            print("\033[1;4mError\033[0m: No command to run is provided.\n")
            sys.exit(1)

        xqute = self._get_xqute()

        await xqute.put(self.command)
        await xqute.run_until_complete()

    async def _run_nowait(self):
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
            if xqute.plugin_context:
                xqute.plugin_context.__exit__()

    def _run_view_logs(self):
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
        logger.info("Press Ctrl-C (twice) to stop.")
        print("")
        while True:
            for key, populator in poplulators.items():
                lines = populator.populate()
                for line in lines:
                    if len(log_source) > 1:
                        print(f"/{key} {line}")
                    else:
                        print(line)
            sleep(5)

    def setup(self):
        logger.addHandler(RichHandler(show_path=False, show_time=False))
        logger.addFilter(DuplicateFilter())
        logger.setLevel(self.config.loglevel.upper())

        self._check_workdir()
        self._infer_name()
        self._infer_jobname_prefix()
        self._setup_mount()

    async def run(self):
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


class CliGbatchPlugin(CLIPlugin):
    """Simplify running commands via Google Cloud Batch."""

    __version__ = __version__
    name = "gbatch"

    @staticmethod
    def _get_defaults_from_config(
        config_files: list[str],
        profile: str | None,
    ) -> dict:
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
        super().__init__(parser, subparser)
        subparser.epilog = """\033[1;4mExamples\033[0m:

  \u200B
  # Run a command and wait for it to complete
  > pipen gbatch --workdir gs://my-bucket/workdir -- \\
    python myscript.py --input input.txt --output output.txt

  \u200B
  # Run a command in a detached mode
  > pipen gbatch --nowait --project $PROJECT --location $LOCATION \\
    --workdir gs://my-bucket/workdir -- \\
    python myscript.py --input input.txt --output output.txt

  \u200B
  # If you have a profile defined in ~/.pipen.toml or ./.pipen.toml
  > pipen gbatch --profile myprofile -- \\
    python myscript.py --input input.txt --output output.txt

  \u200B
  # View the logs of a previously run command
  > pipen gbatch --view-logs all --name my-daemon-name \\
    --workdir gs://my-bucket/workdir
        """
        argfile = Path(__file__).parent / "daemon_args.toml"
        args_def = Config.load(argfile, loader="toml")
        mutually_exclusive_groups = args_def.get("mutually_exclusive_groups", [])
        groups = args_def.get("groups", [])
        arguments = args_def.get("arguments", [])
        subparser._add_decedents(mutually_exclusive_groups, groups, [], arguments, [])

    def parse_args(self, known_parsed, unparsed_argv: list[str]) -> Namespace:
        """Define arguments for the command"""
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
        # update parsed with the defaults
        for key, val in defaults.items():
            if (
                key == "command"
                or val is None
                or getattr(known_parsed, key, None) is not None
            ):
                continue

            setattr(known_parsed, key, val)

        return known_parsed

    def exec_command(self, args: Namespace) -> None:
        """Execute the command"""
        asyncio.run(CliGbatchDaemon(args, args.command).run())
