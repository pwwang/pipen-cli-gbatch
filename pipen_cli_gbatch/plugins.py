from __future__ import annotations

import asyncio
import sys
from typing import Any, Sequence
from argparse import Namespace
from contextlib import suppress
from pathlib import Path

from simpleconf import Config, ProfileConfig
from panpath import GSPath, PanPath
from xqute import plugin
from xqute.utils import logger
from pipen.defaults import CONFIG_FILES
from pipen.cli import AsyncCLIPlugin
from pipen_args.parser_ import _pre_parse
from pipen_poplog import LogsPopulator
from .version import __version__


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
                from .mixin import error_and_exit
                error_and_exit("The command to run must be after '--'.")

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
        from .daemons import CliGbatchDaemonPlain, CliGbatchDaemonPipeline
        if args.plain:
            await CliGbatchDaemonPlain(args, args.command).run()
        else:
            await CliGbatchDaemonPipeline(args, args.command).run()
