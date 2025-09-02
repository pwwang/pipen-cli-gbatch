from __future__ import annotations

import pytest
# import signal
# import asyncio
from unittest.mock import patch
from argx import Namespace
from pipen_cli_gbatch import (
    CliGbatchDaemon,
    GbatchScheduler,
    Xqute,
    pipen_version,
    __version__ as gbatch_version,
)
from .mock.mocks import MockAnyPath, mock_isinstance, MockXquteGbatchScheduler
# from .conftest import MOCK_MOUNTS_DIR


def test_init():
    daemon = CliGbatchDaemon({}, [])
    assert daemon is not None
    assert daemon.config == {"postscript": "", "prescript": ""}
    assert daemon.command == []

    daemon = CliGbatchDaemon(Namespace(key="val"), ["cmd", "arg1"])
    assert daemon is not None
    assert daemon.config.key == "val"
    assert daemon.command == ["cmd", "arg1"]


def test_get_arg_from_command(tmp_path):
    daemon = CliGbatchDaemon({}, ["cmd", "--arg1", "value1", "--arg2=value2"])

    assert daemon._get_arg_from_command("arg1") == "value1"
    assert daemon._get_arg_from_command("arg2") == "value2"
    assert daemon._get_arg_from_command("arg3") is None
    assert daemon._get_arg_from_command("cmd") is None

    configfile = tmp_path / "config.toml"
    configfile.write_text("key = 'value'")
    daemon = CliGbatchDaemon({}, ["cmd", f"@{configfile}"])
    assert daemon._get_arg_from_command("key") == "value"

    nonexist_file = tmp_path / "nonexist.toml"
    daemon = CliGbatchDaemon({}, ["cmd", f"@{nonexist_file}"])
    with pytest.raises(FileNotFoundError):
        daemon._get_arg_from_command("key")


def test_replace_arg_in_command(tmp_path):
    daemon = CliGbatchDaemon({}, ["cmd", "--arg1", "value1", "--arg2=value2"])

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
    daemon = CliGbatchDaemon({}, ["cmd"])

    daemon._add_mount("/src/path", "/dest/path")
    assert "/src/path:/dest/path" in daemon.config.mount


# @pytest.mark.forked
def test_handle_workdir(tmp_path):
    # no workdir
    daemon = CliGbatchDaemon({}, ["cmd"])
    with pytest.raises(SystemExit):
        daemon._handle_workdir()

    daemon = CliGbatchDaemon({"workdir": "gs://bucket/path/workdir"}, ["cmd"])
    with (
        patch("pipen_cli_gbatch.AnyPath", MockAnyPath),
        patch("pipen_cli_gbatch.isinstance", mock_isinstance),
    ):
        daemon._handle_workdir()
    assert daemon.config.workdir == "gs://bucket/path/workdir"
    assert (
        f"gs://bucket/path/workdir:{GbatchScheduler.MOUNTED_METADIR}"
        in daemon.config.mount
    )
    assert "--workdir" in daemon.command
    assert GbatchScheduler.MOUNTED_METADIR in daemon.command


def test_handler_outdir(tmp_path):
    daemon = CliGbatchDaemon({}, ["cmd", "--outdir", "gs://bucket/path/outdir"])
    daemon._handle_outdir()
    assert (
        f"gs://bucket/path/outdir:{GbatchScheduler.MOUNTED_OUTDIR}"
        in daemon.config.mount
    )
    assert "--outdir" in daemon.command
    assert GbatchScheduler.MOUNTED_OUTDIR in daemon.command


def test_infer_name():
    daemon = CliGbatchDaemon({"name": "MyDaemon"}, ["cmd"])
    daemon._infer_name()
    assert daemon.config.name == "MyDaemon"

    daemon = CliGbatchDaemon({}, ["cmd", "--name", "MyJob"])
    daemon._infer_name()
    assert daemon.config.name == "MyJobGbatchDaemon"

    daemon = CliGbatchDaemon({}, ["cmd"])
    daemon._infer_name()
    assert daemon.config.name == "PipenCliGbatchDaemon"


def test_infer_jobname_prefix():
    daemon = CliGbatchDaemon({"jobname_prefix": "my-prefix"}, ["cmd"])
    daemon._infer_jobname_prefix()
    assert daemon.config.jobname_prefix == "my-prefix"

    daemon = CliGbatchDaemon({}, ["cmd", "--name", "MyJob"])
    daemon._infer_jobname_prefix()
    assert daemon.config.jobname_prefix == "myjob-gbatch-daemon"

    daemon = CliGbatchDaemon({}, ["cmd"])
    daemon._infer_jobname_prefix()
    assert daemon.config.jobname_prefix == "pipen-cli-gbatch-daemon"


def test_run_version(capsys):
    daemon = CliGbatchDaemon({}, ["cmd"])
    daemon._run_version()

    captured = capsys.readouterr()
    assert f"pipen-cli-gbatch version: v{gbatch_version}" in captured.out
    assert f"pipen version: v{pipen_version}" in captured.out


def test_show_scheduler_opts(caplog):
    daemon = CliGbatchDaemon(
        {
            "plain": True,
            "option1": "value1",
            "workdir": "gs://bucket/path/workdir",
            "loglevel": "debug",
        },
        ["cmd"],
    )
    with (
        patch("pipen_cli_gbatch.AnyPath", MockAnyPath),
        patch("pipen_cli_gbatch.isinstance", mock_isinstance),
    ):
        daemon.setup()

    daemon._show_scheduler_opts()
    assert "Scheduler Options:" in caplog.text
    assert "plain" not in caplog.text
    assert "- option1: value1" in caplog.text


def test_setup(tmp_path):
    daemon = CliGbatchDaemon(
        {
            "plain": False,
            "workdir": "gs://bucket/path/workdir",
            "project": "my-gcp-project",
            "location": "us-central1",
            "gcloud": "/path/to/gcloud",
            "loglevel": "debug",
        },
        ["cmd", "--arg1", "value1"],
    )
    with (
        patch("pipen_cli_gbatch.AnyPath", MockAnyPath),
        patch("pipen_cli_gbatch.isinstance", mock_isinstance),
    ):
        daemon.setup()

    assert daemon.config.name == "PipenCliGbatchDaemon"
    assert daemon.config.jobname_prefix == "pipen-cli-gbatch-daemon"
    assert daemon.config.workdir == "gs://bucket/path/workdir"
    assert (
        f"gs://bucket/path/workdir:{GbatchScheduler.MOUNTED_METADIR}"
        in daemon.config.mount
    )
    assert "--workdir" in daemon.command
    assert GbatchScheduler.MOUNTED_METADIR in daemon.command
    assert daemon.config.project == "my-gcp-project"
    assert daemon.config.location == "us-central1"
    assert daemon.config.gcloud == "/path/to/gcloud"
    assert "--arg1" in daemon.command
    assert "value1" in daemon.command


def test_setup_plain_no_workdir():
    daemon = CliGbatchDaemon(
        {
            "plain": True,
            "project": "my-gcp-project",
            "location": "us-central1",
            "gcloud": "/path/to/gcloud",
            "loglevel": "debug",
            "workdir": None,
        },
        ["cmd", "--arg1", "value1"],
    )

    with pytest.raises(SystemExit):
        daemon.setup()


# Deadlock ...
# @pytest.mark.asyncio
# async def test_view_logs(mock_gcloud_path, capsys):
#     daemon = CliGbatchDaemon(
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
# @pytest.mark.asyncio
# async def test_run_wait(mock_gcloud_path, caplog):
#     daemon = CliGbatchDaemon(
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


@pytest.mark.asyncio
async def test_get_xqute():
    daemon = CliGbatchDaemon(
        {
            "nowait": False,
            "view_logs": False,
            "error_strategy": "halt",
            "num_retries": 0,
            "jobname_prefix": "test-get-xqute",
            "workdir": "gs://bucket/path/workdir",
            "name": "TestGetXquteDaemon",
            "project": "my-gcp-project",
            "location": "us-central1",
        },
        ["cmd"],
    )
    xqute = daemon._get_xqute()
    assert isinstance(xqute, Xqute)


@pytest.mark.asyncio
async def test_run_no_command_error():
    daemon = CliGbatchDaemon({"nowait": True}, [])
    with pytest.raises(SystemExit):
        await daemon._run_wait()

    daemon = CliGbatchDaemon({"nowait": False}, [])
    with pytest.raises(SystemExit):
        await daemon._run_nowait()


@pytest.mark.asyncio
async def test_run_nowait(mock_gcloud_path, caplog):
    daemon = CliGbatchDaemon(
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
        # patch("pipen_cli_gbatch.AnyPath", MockAnyPath),
        # patch("pipen_cli_gbatch.isinstance", mock_isinstance),
        patch(
            "xqute.schedulers.gbatch_scheduler.GbatchScheduler",
            MockXquteGbatchScheduler,
        )
    ):
        await daemon._run_nowait()

    assert "cmd: command not found" not in caplog.text


@pytest.mark.asyncio
async def test_run_nowait_is_running(mock_gcloud_path, caplog):
    daemon = CliGbatchDaemon(
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
        # patch("pipen_cli_gbatch.AnyPath", MockAnyPath),
        # patch("pipen_cli_gbatch.isinstance", mock_isinstance),
        patch(
            "xqute.schedulers.gbatch_scheduler.GbatchScheduler",
            MockXquteGbatchScheduler,
        )
    ):
        await daemon._run_nowait()
        # Run again, should detect job is running
        await daemon._run_nowait()

    assert "Job is already submited or running" in caplog.text
