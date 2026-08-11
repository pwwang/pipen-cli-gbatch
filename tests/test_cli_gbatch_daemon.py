from __future__ import annotations

import pytest
# import signal
# import asyncio
import re
from unittest.mock import AsyncMock, MagicMock, patch
from panpath import PanPath
from argx import Namespace
from xqute import Xqute
from pipen import __version__ as pipen_version
from pipen.scheduler import GbatchScheduler
from pipen_cli_gbatch import (
    CliGbatchDaemonPlain,
    CliGbatchDaemonPipeline,
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
    await xqute.scheduler.post_init()
    workdir = MOCK_MOUNTS_DIR / str(xqute.scheduler.workdir)[5:]
    wrapped_file = workdir / "0" / "job.wrapped.gbatch"
    assert wrapped_file.exists()
    content = wrapped_file.read_text()
    assert "export ENV_VAR1=value1" in content
    assert "export ENV_VAR2=value2" in content


async def test_pipeline_no_name():
    daemon = CliGbatchDaemonPipeline(
        {
            "mount_as_cwd": "gs://bucket/path",
            "project": "my-gcp-project",
            "location": "us-central1",
        },
        ["cmd"],
    )
    with pytest.raises(ValueError):
        await daemon.setup()


async def test_error_mount_as_cwd_and_cwd():
    with pytest.raises(ValueError):
        CliGbatchDaemonPlain(
            {
                "mount_as_cwd": "gs://bucket/path",
                "cwd": "/some/path",
                "project": "my-gcp-project",
                "location": "us-central1",
            },
            ["cmd"],
        )


async def test_error_mount_as_cwd_and_cwd_pipeline():
    with pytest.raises(ValueError):
        await CliGbatchDaemonPipeline(
            {
                "mount_as_cwd": "gs://bucket/path",
                "cwd": "/some/path",
                "project": "my-gcp-project",
                "location": "us-central1",
            },
            ["cmd"],
        )


async def test_error_cwd_nonlocal_pipeline():
    with pytest.raises(ValueError):
        CliGbatchDaemonPipeline(
            {
                "cwd": "gs://some/path",
                "project": "my-gcp-project",
                "location": "us-central1",
            },
            ["cmd"],
        )


async def test_error_cwd_notfound_pipeline():
    daemon = CliGbatchDaemonPipeline(
        {
            "mount": "gs://some/path:/mnt/disks/root",
            "cwd": "/tmp",
            "workdir": "workdir",
            "project": "my-gcp-project",
            "location": "us-central1",
        },
        ["cmd", "--name", "MyJob", "--outdir", "gs://bucket/path/outdir"],
    )
    await daemon.handle_workdir()
    with pytest.raises(ValueError):
        daemon.command_workdir


async def test_mount_as_cwd():
    daemon = CliGbatchDaemonPlain(
        {"mount_as_cwd": "gs://bucket/path"},
        ["cmd", "--arg", "value", "--outdir", "path/to/outdir"],
    )
    await daemon.setup()
    # no following for plain mode
    # await daemon._infer_name()
    # await daemon.handle_workdir()
    assert str(daemon.mount_as_cwd) == "gs://bucket/path"
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
    assert str(daemon.mount_as_cwd) == "gs://bucket/path"
    xqute = await daemon._get_xqute()
    await xqute.scheduler.post_init()
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
    assert str(daemon.mount_as_cwd) == "gs://bucket/path"
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


async def test_run_wait_not_running(tmp_path):
    daemon = CliGbatchDaemonPlain({}, ["cmd"])
    job = MagicMock()
    xqute = MagicMock()
    xqute.scheduler.workdir = PanPath(tmp_path)
    xqute.scheduler.location = "us-central1"
    xqute.scheduler.create_job = AsyncMock(return_value=job)
    xqute.scheduler.job_is_running = AsyncMock(return_value=False)
    xqute.feed = AsyncMock()
    xqute.run_until_complete = AsyncMock()
    with patch.object(daemon, "_get_xqute", AsyncMock(return_value=xqute)):
        await daemon._run_wait()
    xqute.scheduler.create_job.assert_awaited_once_with(0, ["cmd"], envs={})
    xqute.feed.assert_awaited_once_with(["cmd"], envs={})
    xqute.run_until_complete.assert_awaited_once()


async def test_run_wait_job_is_running(tmp_path):
    daemon = CliGbatchDaemonPlain({}, ["cmd"])
    job = MagicMock()
    xqute = MagicMock()
    xqute.scheduler.workdir = PanPath(tmp_path)
    xqute.scheduler.location = "us-central1"
    xqute.scheduler.create_job = AsyncMock(return_value=job)
    xqute.scheduler.job_is_running = AsyncMock(return_value=True)
    xqute.feed = AsyncMock()
    xqute.run_until_complete = AsyncMock()
    with (
        patch.object(daemon, "_get_xqute", AsyncMock(return_value=xqute)),
        patch.object(daemon, "_run_nowait", new_callable=AsyncMock) as m_run_nowait,
    ):
        await daemon._run_wait()
    xqute.scheduler.create_job.assert_awaited_once_with(0, ["cmd"], envs={})
    xqute.feed.assert_not_awaited()
    xqute.run_until_complete.assert_not_awaited()
    m_run_nowait.assert_awaited_once_with(xqute)


async def test_run_nowait_jid_refetch(tmp_path, caplog):
    daemon = CliGbatchDaemonPlain({"name": "MyName"}, ["cmd"])
    job = MagicMock()
    job.get_jid = AsyncMock(side_effect=[None, "jid-123"])
    xqute = MagicMock()
    xqute.scheduler.workdir = PanPath(tmp_path)
    xqute.scheduler.location = "us-central1"
    xqute.scheduler.create_job = AsyncMock(return_value=job)
    xqute.scheduler.job_is_running = AsyncMock(return_value=False)
    xqute.scheduler.submit_job_and_update_status = AsyncMock()
    xqute.plugin_context = MagicMock()
    with patch.object(daemon, "_get_xqute", AsyncMock(return_value=xqute)):
        await daemon._run_nowait()
    assert job.get_jid.await_count == 2
    assert "Job is running in a detached mode: jid-123" in caplog.text
    xqute.plugin_context.__exit__.assert_called_once()


async def test_run_view_logs_workdir_not_found(tmp_path):
    daemon = CliGbatchDaemonPlain(
        {"workdir": str(tmp_path), "name": "MyName", "view_logs": "stdout"},
        ["cmd"],
    )
    with pytest.raises(ValueError):
        await daemon._run_view_logs()


async def test_run_view_logs_stdout(tmp_path, capsys):
    (tmp_path / "MyName" / "0").mkdir(parents=True)
    (tmp_path / "MyName" / "0" / "job.stdout").write_text("out-line\n")
    daemon = CliGbatchDaemonPlain(
        {"workdir": str(tmp_path), "name": "MyName", "view_logs": "stdout"},
        ["cmd"],
    )
    with patch("asyncio.sleep", side_effect=KeyboardInterrupt):
        with pytest.raises(SystemExit):
            await daemon._run_view_logs()
    out = capsys.readouterr().out
    assert "out-line" in out
    assert "/STDOUT" not in out


async def test_run_view_logs_stderr(tmp_path, capsys):
    (tmp_path / "MyName" / "0").mkdir(parents=True)
    (tmp_path / "MyName" / "0" / "job.stderr").write_text("err-line\n")
    daemon = CliGbatchDaemonPlain(
        {"workdir": str(tmp_path), "name": "MyName", "view_logs": "stderr"},
        ["cmd"],
    )
    with patch("asyncio.sleep", side_effect=KeyboardInterrupt):
        with pytest.raises(SystemExit):
            await daemon._run_view_logs()
    out = capsys.readouterr().out
    assert "err-line" in out
    assert "/STDERR" not in out


async def test_run_view_logs_all(tmp_path, capsys):
    (tmp_path / "MyName" / "0").mkdir(parents=True)
    (tmp_path / "MyName" / "0" / "job.stdout").write_text("out-line\n")
    (tmp_path / "MyName" / "0" / "job.stderr").write_text("err-line\n")
    daemon = CliGbatchDaemonPlain(
        {"workdir": str(tmp_path), "name": "MyName", "view_logs": "all"},
        ["cmd"],
    )
    with patch("asyncio.sleep", side_effect=KeyboardInterrupt):
        with pytest.raises(SystemExit):
            await daemon._run_view_logs()
    out = capsys.readouterr().out
    assert "/STDOUT out-line" in out
    assert "/STDERR err-line" in out


async def test_run_view_logs_residue(tmp_path, capsys):
    # No trailing newline, so the last line is held as residue until Ctrl-C
    (tmp_path / "MyName" / "0").mkdir(parents=True)
    (tmp_path / "MyName" / "0" / "job.stdout").write_text("no-newline")
    daemon = CliGbatchDaemonPlain(
        {"workdir": str(tmp_path), "name": "MyName", "view_logs": "stdout"},
        ["cmd"],
    )
    with patch("asyncio.sleep", side_effect=KeyboardInterrupt):
        with pytest.raises(SystemExit):
            await daemon._run_view_logs()
    out = capsys.readouterr().out
    assert "no-newline" in out
    # residue is bytes; it must be decoded, not shown as b'no-newline'
    assert "b'no-newline'" not in out

    # with multiple sources, the residue is printed with its source prefix
    (tmp_path / "MyName" / "0" / "job.stderr").write_text("err-line\n")
    daemon = CliGbatchDaemonPlain(
        {"workdir": str(tmp_path), "name": "MyName", "view_logs": "all"},
        ["cmd"],
    )
    with patch("asyncio.sleep", side_effect=KeyboardInterrupt):
        with pytest.raises(SystemExit):
            await daemon._run_view_logs()
    out = capsys.readouterr().out
    assert "/STDOUT no-newline" in out
    assert "/STDERR err-line" in out


async def test_run_version_exits_early():
    daemon = CliGbatchDaemonPlain({"version": True}, ["cmd"])
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock) as m_setup,
        patch.object(daemon, "_show_versions") as m_show_versions,
        patch.object(daemon, "_show_scheduler_opts") as m_show_opts,
        patch.object(daemon, "_run_version") as m_run_version,
        patch.object(daemon, "_run_nowait", new_callable=AsyncMock),
        patch.object(daemon, "_run_view_logs", new_callable=AsyncMock),
        patch.object(daemon, "_run_wait", new_callable=AsyncMock),
    ):
        await daemon.run()
    m_run_version.assert_called_once()
    m_setup.assert_not_awaited()
    m_show_versions.assert_not_called()
    m_show_opts.assert_not_called()

    # the same for the pipeline daemon
    daemon = CliGbatchDaemonPipeline({"version": True}, ["cmd"])
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock) as m_setup,
        patch.object(daemon, "_run_version") as m_run_version,
    ):
        await daemon.run()
    m_run_version.assert_called_once()
    m_setup.assert_not_awaited()


async def test_run_nowait_dispatch():
    daemon = CliGbatchDaemonPlain({"nowait": True}, ["cmd"])
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock),
        patch.object(daemon, "_show_versions"),
        patch.object(daemon, "_show_scheduler_opts"),
        patch.object(daemon, "_run_nowait", new_callable=AsyncMock) as m_run_nowait,
        patch.object(daemon, "_run_view_logs", new_callable=AsyncMock) as m_view_logs,
        patch.object(daemon, "_run_wait", new_callable=AsyncMock) as m_run_wait,
    ):
        await daemon.run()
    m_run_nowait.assert_awaited_once_with()
    m_view_logs.assert_not_awaited()
    m_run_wait.assert_not_awaited()


async def test_run_view_logs_dispatch():
    daemon = CliGbatchDaemonPlain({"view_logs": "all"}, ["cmd"])
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock),
        patch.object(daemon, "_show_versions"),
        patch.object(daemon, "_show_scheduler_opts"),
        patch.object(daemon, "_run_nowait", new_callable=AsyncMock) as m_run_nowait,
        patch.object(daemon, "_run_view_logs", new_callable=AsyncMock) as m_view_logs,
        patch.object(daemon, "_run_wait", new_callable=AsyncMock) as m_run_wait,
    ):
        await daemon.run()
    m_view_logs.assert_awaited_once_with()
    m_run_nowait.assert_not_awaited()
    m_run_wait.assert_not_awaited()


async def test_run_wait_dispatch():
    daemon = CliGbatchDaemonPlain({}, ["cmd"])
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock),
        patch.object(daemon, "_show_versions"),
        patch.object(daemon, "_show_scheduler_opts"),
        patch.object(daemon, "_run_nowait", new_callable=AsyncMock) as m_run_nowait,
        patch.object(daemon, "_run_view_logs", new_callable=AsyncMock) as m_view_logs,
        patch.object(daemon, "_run_wait", new_callable=AsyncMock) as m_run_wait,
    ):
        await daemon.run()
    m_run_wait.assert_awaited_once_with()
    m_run_nowait.assert_not_awaited()
    m_view_logs.assert_not_awaited()


async def test_run_pipeline_wait_dispatch():
    daemon = CliGbatchDaemonPipeline({}, ["cmd"])
    daemon.config["workdir"] = PanPath("gs://bucket/path/workdir")
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock),
        patch.object(daemon, "_show_versions"),
        patch.object(daemon, "_show_scheduler_opts"),
        patch.object(daemon, "_run_wait", new_callable=AsyncMock) as m_run_wait,
    ):
        await daemon.run()
    m_run_wait.assert_awaited_once()
    assert (
        str(m_run_wait.call_args.kwargs["stdout_file"])
        == "gs://bucket/path/workdir/run-latest.log"
    )


async def test_run_pipeline_nowait_dispatch():
    daemon = CliGbatchDaemonPipeline({"nowait": True}, ["cmd"])
    daemon.config["workdir"] = PanPath("gs://bucket/path/workdir")
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock),
        patch.object(daemon, "_show_versions"),
        patch.object(daemon, "_show_scheduler_opts"),
        patch.object(daemon, "_run_nowait", new_callable=AsyncMock) as m_run_nowait,
    ):
        await daemon.run()
    m_run_nowait.assert_awaited_once()
    assert (
        str(m_run_nowait.call_args.kwargs["stdout_file"])
        == "gs://bucket/path/workdir/run-latest.log"
    )


async def test_run_pipeline_view_logs_dispatch():
    daemon = CliGbatchDaemonPipeline({"view_logs": "all"}, ["cmd"])
    daemon.config["workdir"] = PanPath("gs://bucket/path/workdir")
    with (
        patch.object(daemon, "setup", new_callable=AsyncMock),
        patch.object(daemon, "_show_versions"),
        patch.object(daemon, "_show_scheduler_opts"),
        patch.object(daemon, "_run_view_logs", new_callable=AsyncMock) as m_view_logs,
    ):
        await daemon.run()
    m_view_logs.assert_awaited_once_with()


def test_daemon_name_plain_from_config():
    daemon = CliGbatchDaemonPlain({"name": "MyName"}, ["cmd"])
    assert daemon.daemon_name == "MyName"
    assert daemon.config["name"] == "MyName"


async def test_handle_workdir_plain_absolute_non_gs(tmp_path):
    daemon = CliGbatchDaemonPlain({"workdir": str(tmp_path)}, ["cmd"])
    with pytest.raises(ValueError):
        await daemon.handle_workdir()


async def test_jobname_prefix_plain_truncated():
    daemon = CliGbatchDaemonPlain({}, ["a" * 40, "b"])
    prefix = await daemon.jobname_prefix()
    assert re.fullmatch(r"pipen-gbatch-a{28}-[0-9a-f]{6}", prefix)
    assert len(prefix) == 48


async def test_jobname_prefix_plain_invalid_chars():
    daemon = CliGbatchDaemonPlain({"jobname_prefix": "UPPER"}, ["cmd"])
    with pytest.raises(ValueError):
        await daemon.jobname_prefix()


async def test_jobname_prefix_plain_too_long():
    # config-provided prefixes are not truncated
    daemon = CliGbatchDaemonPlain({"jobname_prefix": "a" * 49}, ["cmd"])
    with pytest.raises(ValueError):
        await daemon.jobname_prefix()

    # invalid characters in a config-provided prefix also error
    daemon = CliGbatchDaemonPlain({"jobname_prefix": "UPPER" * 9}, ["cmd"])
    with pytest.raises(ValueError):
        await daemon.jobname_prefix()


async def test_jobname_prefix_pipeline_invalid():
    daemon = CliGbatchDaemonPipeline({"jobname_prefix": "UPPER"}, ["cmd"])
    with pytest.raises(ValueError):
        await daemon.jobname_prefix()


async def test_command_workdir():
    daemon = CliGbatchDaemonPipeline({"mount_as_cwd": "gs://bucket/cwd"}, ["cmd"])
    daemon.config["workdir"] = PanPath("relative/workdir")
    assert str(daemon.command_workdir) == "gs://bucket/cwd/relative/workdir"

    daemon = CliGbatchDaemonPipeline({}, ["cmd"])
    daemon.config["workdir"] = PanPath("gs://bucket/abs/workdir")
    assert str(daemon.command_workdir) == "gs://bucket/abs/workdir"


async def test_command_workdir_cwd():
    daemon = CliGbatchDaemonPipeline(
        {"mount": "gs://bucket/cwd:/mnt/disks/root", "cwd": "/mnt/disks/root/workdir"},
        ["cmd"],
    )
    daemon.config["workdir"] = PanPath("relative/workdir")
    assert str(daemon.command_workdir) == "gs://bucket/cwd/workdir/relative/workdir"


async def test_plain_workdir_cwd():
    daemon = CliGbatchDaemonPlain(
        {"mount": "gs://bucket/cwd:/mnt/disks/root", "cwd": "/mnt/disks/root/workdir"},
        ["cmd"],
    )
    await daemon.handle_workdir()
    assert str(daemon.config.workdir) == "workdir/.pipen"


async def test_plain_workdir_cwd_not_found():
    daemon = CliGbatchDaemonPlain(
        {"mount": "gs://bucket/cwd:/mnt/disks/root", "cwd": "/tmp/notfound"},
        ["cmd"],
    )
    with pytest.raises(ValueError):
        await daemon.handle_workdir()


async def test_command_outdir_cwd():
    daemon = CliGbatchDaemonPipeline(
        {
            "mount": "gs://bucket/cwd:/mnt/disks/root",
            "cwd": "/mnt/disks/root/workdir",
        },
        ["cmd", "--name", "MyJob"],
    )
    await daemon.handle_workdir()
    assert str(daemon.command_workdir) == "gs://bucket/cwd/workdir/.pipen/MyJob"
    assert "/mnt/disks/root/workdir/MyJob-output" in daemon.command
