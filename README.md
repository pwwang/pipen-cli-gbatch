# pipen-cli-gbatch

A pipen CLI plugin to run commands via Google Cloud Batch.

The idea is to submit the command using xqute and use the gbatch scheduler to run it on Google Cloud Batch.

## Installation

```bash
pip install pipen-cli-gbatch
```

## Usage

### Basic Command Execution

To run a command like:

```bash
python myscript.py --input input.txt --output output.txt
```

You can run it with:

```bash
pipen gbatch -- python myscript.py --input input.txt --output output.txt
```

### With Configuration File

In order to provide configurations like we do for a normal pipen pipeline, you can also provide a config file (the `[pipen-cli-gbatch]` section will be used):

```bash
pipen gbatch @config.toml -- \
    python myscript.py --input input.txt --output output.txt
```

### Detached Mode

We can also use the `--nowait` option to run the command in a detached mode:

```bash
pipen gbatch --nowait -- \
    python myscript.py --input input.txt --output output.txt
```

Or by default, it will wait for the command to complete:

```bash
pipen gbatch -- \
    python myscript.py --input input.txt --output output.txt
```

While waiting, the running logs will be pulled and shown in the terminal.

### View Logs

When running in detached mode, one can also pull the logs later by:

```bash
pipen gbatch --view-logs -- \
    python myscript.py --input input.txt --output output.txt

# or  just provide the workdir
pipen gbatch --view-logs --workdir gs://my-bucket/workdir
```

### Cancel or Check Status

We also have shortcuts to cancel or check the status of the running job:

```bash
pipen gbatch --cancel -- \
    python myscript.py --input input.txt --output output.txt

# or just provide the workdir
pipen gbatch --cancel --workdir gs://my-bucket/workdir

pipen gbatch --status -- \
    python myscript.py --input input.txt --output output.txt

# or just provide the workdir
pipen gbatch --status --workdir gs://my-bucket/workdir
```

## Configuration

Because the daemon pipeline is running on Google Cloud Batch, a Google Storage Bucket path is required for the workdir. For example: `gs://my-bucket/workdir`

A unique job ID will be generated per the name (`--name`) and workdir, so that if the same command is run again with the same name and workdir, it will not start a new job, but just attach to the existing job and pull the logs.

If `--name` is not provided in the command line or `pipen-cli-gbatch.name` is not provided from the configuration file, it will try to grab the name (`--name`) from the command line arguments after `--`, or else use "name" from the root section of the configuration file, with a "CliGbatchDaemon" suffix. If nothing can be found, a default name "PipenCliGbatchDaemon" will be used.

Then a workdir `{workdir}/<daemon pipeline name>/` will be created to store the meta information.

One can have some default configuration file for the daemon pipeline in either/both the user home directory `~/.pipen-cli-gbatch.toml` or the current working directory `./.pipen-cli-gbatch.toml`. The configurations in these files will be overridden by the command line arguments.

Example `config.toml`:

```toml
[pipen-cli-gbatch]
workdir = "gs://my-bucket/workdir"
name = "MyDaemon"

[DEFAULT]
# Other pipen configurations
```

## API

The API can also be used to run commands programmatically:

```python
import asyncio
from pipen_cli_gbatch import CliGbatchDaemon

pipe = CliGbatchDaemon(config_for_daemon, command)
asyncio.run(pipe.run())
```

Note that the daemon pipeline will always be running without caching, so that the command will always be executed when the pipeline is run.
