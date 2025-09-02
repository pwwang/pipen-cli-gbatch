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

## Configuration

Because the daemon pipeline is running on Google Cloud Batch, a Google Storage Bucket path is required for the workdir. For example: `gs://my-bucket/workdir`

A unique job ID will be generated per the name (`--name`) and workdir, so that if the same command is run again with the same name and workdir, it will not start a new job, but just attach to the existing job and pull the logs.

If `--name` is not provided in the command line, it will try to grab the name (`--name`) from the command line arguments after `--`, or else use "name" from the root section of the configuration file, with a "GbatchDaemon" suffix. If nothing can be found, a default name "PipenGbatchDaemon" will be used.

Then a workdir `{workdir}/<daemon pipeline name>/` will be created to store the meta information.

With `--profile` provided, the scheduler options (`scheduler_opts`) defined in `~/.pipen.toml` and `./.pipen.toml` will be used as default.

## All Options

```bash
> pipen gbatch --help
Usage: pipen gbatch [-h] [--nowait | --view-logs {all,stdout,stderr}] [--workdir WORKDIR]
                    [--error-strategy {retry,halt}] [--num-retries NUM_RETRIES] [--prescript PRESCRIPT]
                    [--postscript POSTSCRIPT] [--jobname-prefix JOBNAME_PREFIX] [--recheck-interval RECHECK_INTERVAL]
                    [--cwd CWD] [--project PROJECT] [--location LOCATION] [--mount MOUNT]
                    [--service-account SERVICE_ACCOUNT] [--network NETWORK] [--subnetwork SUBNETWORK]
                    [--no-external-ip-address] [--machine-type MACHINE_TYPE] [--provisioning-model {STANDARD,SPOT}]
                    [--image-uri IMAGE_URI] [--entrypoint ENTRYPOINT] [--commands COMMANDS] [--runnables RUNNABLES]
                    [--allocationPolicy ALLOCATIONPOLICY] [--taskGroups TASKGROUPS] [--labels LABELS] [--gcloud GCLOUD]
                    [--name NAME] [--profile PROFILE] [--version]
                    [--loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL,debug,info,warning,error,critical}]
                    ...

Simplify running commands via Google Cloud Batch.

Key Options:
  The key options to run the command.

  --workdir WORKDIR     The workdir (a Google Storage Bucket path is required) to store the meta information of the
                        daemon pipeline.
                        If not provided, the one from the command will be used.
  command               The command passed after `--` to run, with all its arguments. Note that the command should be
                        provided after `--`.

Scheduler Options:
  The options to configure the gbatch scheduler.

  --error-strategy {retry,halt}
                        The strategy when there is error happened [default: halt]
  --num-retries NUM_RETRIES
                        The number of retries when there is error happened. Only valid when --error-strategy is 'retry'.
                        [default: 0]
  --prescript PRESCRIPT
                        The prescript to run before the main command.
  --postscript POSTSCRIPT
                        The postscript to run after the main command.
  --jobname-prefix JOBNAME_PREFIX
                        The prefix of the name prefix of the daemon job.
                        If not provided, try to generate one from the command to run.
                        If the command is also not provided, use 'pipen-gbatch-daemon' as the prefix.
  --recheck-interval RECHECK_INTERVAL
                        The interval to recheck the job status, each takes about 0.1 seconds. [default: 600]
  --cwd CWD             The working directory to run the command. If not provided, the current directory is used. You
                        can pass either a mounted path (inside the VM) or a Google Storage Bucket path (gs://...). If a
                        Google Storage Bucket path is provided, the mounted path will be inferred from the mounted paths
                        of the VM.
  --project PROJECT     The Google Cloud project to run the job.
  --location LOCATION   The location to run the job.
  --mount MOUNT         The list of mounts to mount to the VM, each in the format of SOURCE:TARGET, where SOURCE must be
                        either a Google Storage Bucket path (gs://...). [default: []]
  --service-account SERVICE_ACCOUNT
                        The service account to run the job.
  --network NETWORK     The network to run the job.
  --subnetwork SUBNETWORK
                        The subnetwork to run the job.
  --no-external-ip-address
                        Whether to disable external IP address for the VM.
  --machine-type MACHINE_TYPE
                        The machine type of the VM.
  --provisioning-model {STANDARD,SPOT}
                        The provisioning model of the VM.
  --image-uri IMAGE_URI
                        The custom image URI of the VM.
  --entrypoint ENTRYPOINT
                        The entry point of the container to run the command.
  --commands COMMANDS   The list of commands to run in the container, each as a separate string. [default: []]
  --runnables RUNNABLES
                        The JSON string of extra settings of runnables add to the job.json.
                        Refer to https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#Runnable
                        for details.
                        You can have an extra key 'order' for each runnable, where negative values mean to run before
                        the main command,
                        and positive values mean to run after the main command.
  --allocationPolicy ALLOCATIONPOLICY
                        The JSON string of extra settings of allocationPolicy add to the job.json. Refer to
                        https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#AllocationPolicy
                        for details. [default: {}]
  --taskGroups TASKGROUPS
                        The JSON string of extra settings of taskGroups add to the job.json. Refer to
                        https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#TaskGroup for
                        details. [default: []]
  --labels LABELS       The JSON string of labels to add to the job. Refer to
                        https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#Job.FIELDS.labels
                        for details. [default: {}]
  --gcloud GCLOUD       The path to the gcloud command. [default: gcloud]

Options:
  -h, --help            show this help message and exit
  --nowait              Run the command in a detached mode without waiting for its completion. [default: False]
  --view-logs {all,stdout,stderr}
                        View the logs of a job.
  --name NAME           The name of the daemon pipeline.
                        If not provided, try to generate one from the command to run.
                        If the command is also not provided, use 'PipenCliGbatchDaemon' as the name.
  --profile PROFILE     Use the `scheduler_opts` as the Scheduler Options of a given profile from pipen configuration
                        files,
                        including ~/.pipen.toml and ./pipen.toml.
                        Note that if not provided, nothing will be loaded from the configuration files.
  --version             Show the version of the pipen-cli-gbatch package. [default: False]
  --loglevel {DEBUG,INFO,WARNING,ERROR,CRITICAL,debug,info,warning,error,critical}
                        Set the logging level for the daemon process. [default: INFO]

Examples:
  ​
  # Run a command and wait for it to complete
  > pipen gbatch --workdir gs://my-bucket/workdir -- \
    python myscript.py --input input.txt --output output.txt

  # Use named mounts
  > pipen gbatch --workdir gs://my-bucket/workdir --mount INFILE=gs://bucket/path/to/file \
    --mount OUTDIR=gs://bucket/path/to/outdir -- \
    cat $INFILE > $OUTDIR/output.txt
  ​
  # Run a command in a detached mode
  > pipen gbatch --nowait --project $PROJECT --location $LOCATION \
    --workdir gs://my-bucket/workdir -- \
    python myscript.py --input input.txt --output output.txt
  ​
  # If you have a profile defined in ~/.pipen.toml or ./.pipen.toml
  > pipen gbatch --profile myprofile -- \
    python myscript.py --input input.txt --output output.txt
  ​
  # View the logs of a previously run command
  > pipen gbatch --view-logs all --name my-daemon-name \
    --workdir gs://my-bucket/workdir
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
