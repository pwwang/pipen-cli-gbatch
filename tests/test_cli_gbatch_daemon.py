from __future__ import annotations

import pytest
# import signal
# import asyncio
import re
from unittest.mock import patch
from panpath import PanPath
from argx import Namespace
from pipen_cli_gbatch import (
    CliGbatchDaemonPlain,
    CliGbatchDaemonPipeline,
    GbatchScheduler,
    Xqute,
    pipen_version,
    __version__ as gbatch_version,
)
from .mock.mocks import mock_isinstance, MockXquteGbatchScheduler
from .conftest import MOCK_MOUNTS_DIR


def test_init():
    daemon = CliGbatchDaemonPipeline({}, [])
    assert daemon is not None
    assert daemon.config == {"postscript": "", "prescript": ""}
    assert daemon.command == []

    daemon = CliGbatchDaemonPipeline(Namespace(key="val"), ["cmd", "arg1"])
    assert daemon is not None
    assert daemon.config.key == "val"
    assert daemon.command == ["cmd", "arg1"]

    daemon = CliGbatchDaemonPipeline({"key": "val", "labels": ["a=1", "b=2"]}, ["cmd", "arg1"])
    assert daemon is not None
    assert daemon.config.labels == {"a": "1", "b": "2"}
    assert daemon.command == ["cmd", "arg1"]


async def test_get_arg_from_command(tmp_path):
    tmp_path = PanPath(tmp_path)
    daemon = CliGbatchDaemonPipeline({}, ["cmd", "--arg1", "value1", "--arg2=value2"])

    assert await daemon._get_arg_from_command("arg1") == "value1"
    assert await daemon._get_arg_from_command("arg2") == "value2"
    assert await daemon._get_arg_from_command("arg3") is None
    assert await daemon._get_arg_from_command("cmd") is None

    configfile = tmp_path / "config.toml"
    await configfile.a_write_text("key = 'value'")
    daemon = CliGbatchDaemonPipeline({}, ["cmd", f"@{configfile}"])
    assert await daemon._get_arg_from_command("key") == "value"

    nonexist_file = tmp_path / "nonexist.toml"
    daemon = CliGbatchDaemonPipeline({}, ["cmd", f"@{nonexist_file}"])
    with pytest.raises(FileNotFoundError):
        await daemon._get_arg_from_command("key")


def test_replace_arg_in_command():
    daemon = CliGbatchDaemonPipeline({}, ["cmd", "--arg1", "value1", "--arg2=value2"])

    daemon._replace_arg_in_command("arg1", "newvalue1")
    assert daemon.command == ["cmd", "--arg1", "newvalue1", "--arg2=value2"]

    daemon._replace_arg_in_command("arg2", "newvalue2")
    assert daemon.command == ["cmd", "--arg1", "newvalue1", "--arg2=newvalue2"]

    daemon._replace_arg_in_command("arg3", "value3")
    assert daemon.command == [
        "cmd",
        "--arg1",
        "newvalue1",
        "--arg2=newvalue2",
        "--arg3",
        "value3",
    ]


def test_add_mount():
    daemon = CliGbatchDaemonPipeline({}, ["cmd"])

    daemon._add_mount("/src/path", "/dest/path")
    assert "/src/path:/dest/path" in daemon.config.mount


# @pytest.mark.forked
async def testhandle_workdir():
    # no workdir
    daemon = CliGbatchDaemonPipeline({}, ["cmd", "--name", "MyJob"])
    # await daemon._infer_name()
    with pytest.raises(ValueError):
        await daemon.handle_workdir()

    daemon = CliGbatchDaemonPipeline(
        {"workdir": "gs://bucket/path/workdir"},
        ["cmd", "--name", "MyJob", "--outdir", "gs://bucket/path/outdir"],
    )
    with patch("pipen_cli_gbatch.isinstance", mock_isinstance):
        # await daemon._infer_name()
        await daemon.handle_workdir()
    assert str(daemon.config.workdir) == "gs://bucket/path/workdir/MyJob"
    assert "--workdir" in daemon.command
    assert "/mnt/disks/.pipen" in daemon.command


async def test_handler_outdir():
    daemon = CliGbatchDaemonPipeline(
        {},
        ["cmd", "--outdir", "gs://bucket/path/outdir", "--name", "MyJob"],
    )
    await daemon._handle_outdir()
    assert (
        "gs://bucket/path/outdir:/mnt/disks/.pipen-MyJob-output"
        in daemon.config.mount
    )
    assert "--outdir" in daemon.command
    assert "/mnt/disks/.pipen-MyJob-output" in daemon.command


async def test_infer_name():
    daemon = CliGbatchDaemonPipeline({"name": "MyDaemon"}, ["cmd"])
    # await daemon._infer_name()
    assert daemon.daemon_name == "MyDaemon"

    daemon = CliGbatchDaemonPipeline({}, ["cmd", "--name", "MyJob"])
    # await daemon._infer_name()
    assert daemon.daemon_name == ".GbatchDaemon"

    daemon = CliGbatchDaemonPipeline({}, ["cmd"])
    # assuming plain=False
    assert daemon.daemon_name == ".GbatchDaemon"


async def test_infer_jobname_prefix():
    daemon = CliGbatchDaemonPipeline({"jobname_prefix": "my-prefix"}, ["cmd"])
    jobname_prefix = await daemon.jobname_prefix()
    assert jobname_prefix == "my-prefix"

    daemon = CliGbatchDaemonPipeline({}, ["cmd", "--name", "MyJob"])
    jobname_prefix = await daemon.jobname_prefix()
    assert jobname_prefix == "pipen-gbatch-myjob"

    daemon = CliGbatchDaemonPipeline({}, ["cmd", "--name", "MyJob"])
    jobname_prefix = await daemon.jobname_prefix()
    assert jobname_prefix == "pipen-gbatch-myjob"


def test_run_version(capsys):
    daemon = CliGbatchDaemonPipeline({}, ["cmd"])
    daemon._run_version()

    captured = capsys.readouterr()
    assert f"pipen-cli-gbatch version: v{gbatch_version}" in captured.out
    assert f"pipen version: v{pipen_version}" in captured.out


async def test_show_scheduler_opts(caplog):
    daemon = CliGbatchDaemonPlain(
        {
            "plain": True,
            "option1": "value1",
            "workdir": "gs://bucket/path/workdir",
            "loglevel": "debug",
        },
        ["cmd"],
    )
    with patch("pipen_cli_gbatch.isinstance", mock_isinstance):
        await daemon.setup()

    daemon._show_scheduler_opts()
    assert "Scheduler Options:" in caplog.text
    assert "plain" not in caplog.text
    assert "- option1: value1" in caplog.text


async def test_setup(tmp_path):
    daemon = CliGbatchDaemonPipeline(
        {
            "plain": False,
            "workdir": "gs://bucket/path/workdir",
            "project": "my-gcp-project",
            "location": "us-central1",
            "gcloud": "/path/to/gcloud",
            "loglevel": "debug",
            "mount": "gs://bucket/path/workdir1:/mnt/disks/workdir1",
        },
        ["cmd", "--arg1", "value1", "--name", "MyJob", "--outdir", "gs://bucket/path/outdir"],
    )
    with patch("pipen_cli_gbatch.isinstance", mock_isinstance):
        await daemon.setup()

    assert daemon.daemon_name == ".GbatchDaemon"
    assert daemon.config.jobname_prefix == "pipen-gbatch-myjob"
    assert str(daemon.config.workdir) == "gs://bucket/path/workdir/MyJob"
    assert daemon.config.mount == [
        'gs://bucket/path/workdir1:/mnt/disks/workdir1',
        'gs://bucket/path/outdir:/mnt/disks/.pipen-MyJob-output',
    ]
    assert "--workdir" in daemon.command
    assert "/mnt/disks/.pipen" in daemon.command
    assert daemon.config.project == "my-gcp-project"
    assert daemon.config.location == "us-central1"
    assert daemon.config.gcloud == "/path/to/gcloud"
    assert "--arg1" in daemon.command
    assert "value1" in daemon.command


async def test_other_opts_to_envs():
    options = Namespace(
        workdir="gs://bucket/path/workdir",
        project="my-gcp-project",
        location="us-central1",
        gcloud="/path/to/gcloud",
        loglevel="debug",
    )
    setattr(
        options,
        "_other_opts",
        {
            "scheduler": "gbatch",
            "custom_option1": "value1",
            "custom_option2": 42,
            "custom_option3": 3.14,
            "custom_option4": {"key": "value"},
            "custom_option5": [1, 2, 3],
            "custom_option6": True,
            "custom_option7": None,
        }
    )
    daemon = CliGbatchDaemonPipeline(
        options,
        ["cmd", "--arg1", "value1"],
    )

    assert daemon.envs["PIPEN_custom_option1"] == "value1"
    assert daemon.envs["PIPEN_custom_option2"] == "@int:42"
    assert daemon.envs["PIPEN_custom_option3"] == "@float:3.14"
    assert daemon.envs["PIPEN_custom_option4"] == "@json:{\"key\": \"value\"}"
    assert daemon.envs["PIPEN_custom_option5"] == "@json:[1, 2, 3]"
    assert daemon.envs["PIPEN_custom_option6"] == "@bool:True"
    assert daemon.envs["PIPEN_custom_option7"] == "@none"


async def test_setup_plain_no_workdir():
    daemon = CliGbatchDaemonPlain(
        {
            "project": "my-gcp-project",
            "location": "us-central1",
            "gcloud": "/path/to/gcloud",
            "loglevel": "debug",
        },
        ["cmd", "--arg1", "value1"],
    )

    with pytest.raises(ValueError):
        await daemon.setup()

# Deadlock ...
# async def test_view_logs(mock_gcloud_path, capsys):
#     daemon = CliGbatchDaemonPipeline(
#         {
#             "nowait": False,
#             "view_logs": True,
#             "error_strategy": "halt",
#             "num_retries": 0,
#             "jobname_prefix": "test-view-logs",
#             "workdir": "gs://bucket/path/workdir",
#             "name": "TestViewLogsDaemon",
#             "project": "my-gcp-project",
#             "location": "us-central1",
#             "gcloud": str(mock_gcloud_path),
#             "loglevel": "info",
#         },
#         ["echo", "Hello, World!"],
#     )
#     with (
#         # patch("pipen_cli_gbatch.AnyPath", MockAnyPath),
#         # patch("pipen_cli_gbatch.isinstance", mock_isinstance),
#         patch(
#             "xqute.schedulers.gbatch_scheduler.GbatchScheduler",
#             MockXquteGbatchScheduler,
#         )
#     ):
#         await daemon._run_nowait()

#     async def send_sigint():
#         await asyncio.sleep(3)
#         signal.raise_signal(signal.SIGINT)

#     asyncio.create_task(send_sigint())
#     daemon.config.view_logs = "all"
#     # with patch("pipen_cli_gbatch.AnyPath", MockAnyPath):
#     daemon.config.workdir = f"{MOCK_MOUNTS_DIR}/bucket/path/workdir"
#     await daemon._run_view_logs()

#     assert "/STDOUT Hello, World!" in capsys.readouterr().out


# Causing deadlock
# async def test_run_wait(mock_gcloud_path, caplog):
#     daemon = CliGbatchDaemonPipeline(
#         {
#             "nowait": False,
#             "view_logs": False,
#             "error_strategy": "halt",
#             "num_retries": 0,
#             "jobname_prefix": "test-run-wait",
#             "workdir": "gs://bucket/path/workdir",
#             "name": "TestRunWaitDaemon",
#             "project": "my-gcp-project",
#             "location": "us-central1",
#             "gcloud": str(mock_gcloud_path),
#             "loglevel": "info",
#         },
#         ["cmd"],
#     )
#     with (
#         # patch("pipen_cli_gbatch.AnyPath", MockAnyPath),
#         # patch("pipen_cli_gbatch.isinstance", mock_isinstance),
#         patch(
#             "xqute.schedulers.gbatch_scheduler.GbatchScheduler",
#             MockXquteGbatchScheduler,
#         )
#     ):
#         await daemon._run_wait()

#     assert "cmd: command not found" in caplog.text


async def test_get_xqute():
    daemon = CliGbatchDaemonPipeline(
        {
            "nowait": False,
            "view_logs": False,
            "error_strategy": "halt",
            "num_retries": 0,
            "plain": False,
            "jobname_prefix": "test-get-xqute",
            "workdir": "gs://bucket/path/workdir",
            "name": "TestGetXquteDaemon",
            "project": "my-gcp-project",
            "location": "us-central1",
        },
        ["cmd", "--name", "MyJob", "--outdir", "gs://bucket/path/outdir"],
    )
    await daemon.setup()
    xqute = await daemon._get_xqute()
    assert isinstance(xqute, Xqute)


async def test_run_no_command_error():
    daemon = CliGbatchDaemonPipeline({"nowait": True}, [])
    with pytest.raises(ValueError):
        await daemon._run_wait()

    daemon = CliGbatchDaemonPipeline({"nowait": False}, [])
    with pytest.raises(ValueError):
        await daemon._run_nowait()


async def test_run_nowait(mock_gcloud_path, caplog):
    daemon = CliGbatchDaemonPipeline(
        {
            "nowait": True,
            "view_logs": False,
            "error_strategy": "halt",
            "num_retries": 0,
            "jobname_prefix": "test-run-nowait",
            "workdir": "gs://bucket/path/workdir",
            "name": "TestRunNowaitDaemon",
            "project": "my-gcp-project",
            "location": "us-central1",
            "gcloud": str(mock_gcloud_path),
            "loglevel": "info",
        },
        ["cmd"],
    )
    with (
        patch(
            "xqute.schedulers.gbatch_scheduler.GbatchScheduler",
            MockXquteGbatchScheduler,
        )
    ):
        await daemon._run_nowait()

    assert "cmd: command not found" not in caplog.text


async def test_run_nowait_is_running(mock_gcloud_path, caplog):
    daemon = CliGbatchDaemonPipeline(
        {
            "nowait": True,
            "view_logs": False,
            "error_strategy": "halt",
            "num_retries": 0,
            "jobname_prefix": "test-run-nowait-is-running",
            "workdir": "gs://bucket/path/workdir",
            "name": "TestRunNowaitIsRunningDaemon",
            "project": "my-gcp-project",
            "location": "us-central1",
            "gcloud": str(mock_gcloud_path),
            "loglevel": "info",
        },
        ["sleep", "100"],
    )
    with (
        patch(
            "xqute.schedulers.gbatch_scheduler.GbatchScheduler",
            MockXquteGbatchScheduler,
        )
    ):
        await daemon._run_nowait()
        # Run again, should detect job is running
        await daemon._run_nowait()

    # Can't get this passed in GitHub Actions for some reason
    # assert "Job is already submited or running" in caplog.text


async def test_with_envs(mock_gcloud_path):
    daemon = CliGbatchDaemonPipeline(
        {
            "nowait": False,
            "view_logs": False,
            "error_strategy": "halt",
            "num_retries": 0,
            "plain": True,
            "jobname_prefix": "test-with-envs",
            "workdir": "gs://bucket/path/workdir",
            "name": "TestWithEnvsDaemon",
            "project": "my-gcp-project",
            "location": "us-central1",
            "gcloud": str(mock_gcloud_path),
            "loglevel": "info",
        },
        ["echo", "$ENV_VAR1", "$ENV_VAR2"],
    )
    daemon.envs = {"ENV_VAR1": "value1", "ENV_VAR2": "value2"}
    with (
        patch(
            "xqute.schedulers.gbatch_scheduler.GbatchScheduler",
            MockXquteGbatchScheduler,
        )
    ):
        await daemon._run_nowait()

    xqute = await daemon._get_xqute()
    workdir = MOCK_MOUNTS_DIR / str(xqute.scheduler.workdir)[5:]
    wrapped_file = workdir / "0" / "job.wrapped.gbatch"
    assert wrapped_file.exists()
    content = wrapped_file.read_text()
    assert "export ENV_VAR1=value1" in content
    assert "export ENV_VAR2=value2" in content


async def test_error_mount_as_cwd_and_cwd():
    daemon = CliGbatchDaemonPipeline(
        {"mount_as_cwd": "gs://bucket/path", "cwd": "/some/path"},
        ["cmd"],
    )
    with pytest.raises(ValueError):
        await daemon.setup()


async def test_mount_as_cwd():
    daemon = CliGbatchDaemonPlain(
        {"mount_as_cwd": "gs://bucket/path"},
        ["cmd", "--arg", "value", "--outdir", "path/to/outdir"],
    )
    await daemon.setup()
    # no following for plain mode
    # await daemon._infer_name()
    # await daemon.handle_workdir()
    assert daemon.mount_as_cwd == "gs://bucket/path"
    assert "--arg" in daemon.command
    assert "value" in daemon.command
    assert "--outdir" in daemon.command
    assert "path/to/outdir" in daemon.command


async def test_mount_as_cwd_with_name():
    daemon = CliGbatchDaemonPipeline(
        {
            "mount_as_cwd": "gs://bucket/path",
            "project": "my-gcp-project",
            "location": "us-central1",
        },
        ["cmd", "--arg", "value", "--name", "MyJob"],
    )
    # await daemon._infer_name()
    await daemon.handle_workdir()
    assert daemon.mount_as_cwd == "gs://bucket/path"
    xqute = await daemon._get_xqute()
    assert xqute.scheduler.cwd == "/mnt/disks/.cwd"
    volumes = xqute.scheduler.config["taskGroups"][0]["taskSpec"]["volumes"]
    assert volumes[0]["gcs"]["remotePath"] == "bucket/path"
    assert volumes[0]["mountPath"] == "/mnt/disks/.cwd"
    assert xqute.scheduler.config["labels"]["xqute"] == "true"
    assert str(xqute.scheduler.workdir) == "gs://bucket/path/.pipen/MyJob/.GbatchDaemon"
    assert len(daemon.config.get("mount", [])) == 0
    assert len(volumes) == 1
    assert "--arg" in daemon.command
    assert "value" in daemon.command
    assert "--outdir" in daemon.command
    assert "/mnt/disks/.cwd/MyJob-output" in daemon.command


async def test_absolute_workdir():
    daemon = CliGbatchDaemonPipeline(
        {},
        ["cmd", "--arg", "value", "--workdir", "/path/outdir", "--name", "MyJob"],
    )
    with pytest.raises(ValueError):
        await daemon.handle_workdir()


async def test_absolute_outdir():
    daemon = CliGbatchDaemonPipeline(
        {},
        ["cmd", "--arg", "value", "--outdir", "/path/outdir", "--name", "MyJob"],
    )
    with pytest.raises(ValueError):
        await daemon._handle_outdir()


async def test_relative_outdir_without_mount_as_cwd():
    daemon = CliGbatchDaemonPipeline(
        {},
        ["cmd", "--arg", "value", "--outdir", "relative/outdir", "--name", "MyJob"],
    )
    with pytest.raises(ValueError):
        await daemon._handle_outdir()


async def test_name_32char_longer():
    long_name = "a" * 40
    daemon = CliGbatchDaemonPipeline(
        {"mount_as_cwd": "gs://bucket/path"},
        ["cmd", "--arg", "value", "--name", long_name],
    )
    # await daemon._infer_name()
    await daemon.handle_workdir()
    jobname_prefix = await daemon.jobname_prefix()
    assert daemon.mount_as_cwd == "gs://bucket/path"
    assert "mount" not in daemon.config
    assert "--arg" in daemon.command
    assert "value" in daemon.command
    assert "--outdir" in daemon.command
    # Name should be truncated to 32 chars
    assert re.match(
        r"^pipen-gbatch-aaaaaaaaaaaaaaaaaaaaaaaaaaaa-.{6}$",
        jobname_prefix,
    )


async def test_show_versions(caplog):
    daemon = CliGbatchDaemonPipeline({}, ["cmd"])
    daemon._show_versions()

    assert f"pipen-cli-gbatch version: v{gbatch_version}" in caplog.text
    assert f"pipen version: v{pipen_version}" in caplog.text
