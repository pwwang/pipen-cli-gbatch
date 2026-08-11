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

from .plugins import CliGbatchPlugin
from .daemons import CliGbatchDaemonPlain, CliGbatchDaemonPipeline
from .version import __version__

__all__ = (
    "CliGbatchPlugin",
    "CliGbatchDaemonPlain",
    "CliGbatchDaemonPipeline",
    "__version__",
)
