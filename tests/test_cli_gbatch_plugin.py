from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, call, patch

from panpath import PanPath
from argx import Namespace
from pipen_args.parser_ import _pre_parse
from pipen_cli_gbatch import CliGbatchPlugin
from pipen_cli_gbatch.plugins import XquteCliGbatchPlugin


def make_plugin_with_logfiles(tmp_path, stdout_text=None, stderr_text=None):
    plugin = XquteCliGbatchPlugin(stdout_file=None)
    if stdout_text is not None:
        plugin.stdout_populator.logfile = PanPath(tmp_path / "job.stdout")
        (tmp_path / "job.stdout").write_text(stdout_text)
    if stderr_text is not None:
        plugin.stderr_populator.logfile = PanPath(tmp_path / "job.stderr")
        (tmp_path / "job.stderr").write_text(stderr_text)
    return plugin


def test_clear_residues(caplog):
    plugin = XquteCliGbatchPlugin()
    plugin.stdout_populator.residue = b"line"
    plugin.stderr_populator.residue = b"err"
    plugin._clear_residues()
    # residues are bytes, they must be decoded
    assert "/STDOUT line" in caplog.text
    assert "/STDERR err" in caplog.text
    assert plugin.stdout_populator.residue == ""
    assert plugin.stderr_populator.residue == ""


async def test_on_job_started_no_stdout_file(tmp_path):
    plugin = XquteCliGbatchPlugin(stdout_file=None)
    scheduler = MagicMock()
    scheduler.workdir = PanPath(tmp_path)
    job = MagicMock()
    await plugin.on_job_started(scheduler, job)
    assert (
        str(plugin.stdout_populator.logfile)
        == str(PanPath(tmp_path) / "0" / "job.stdout")
    )
    assert (
        str(plugin.stderr_populator.logfile)
        == str(PanPath(tmp_path) / "0" / "job.stderr")
    )


async def test_on_job_started_existing_stdout_file(tmp_path):
    stdout_file = PanPath(tmp_path / "my.stdout")
    (tmp_path / "my.stdout").write_text("x\n")
    plugin = XquteCliGbatchPlugin(stdout_file=stdout_file)
    scheduler = MagicMock()
    scheduler.workdir = PanPath(tmp_path)
    job = MagicMock()
    await plugin.on_job_started(scheduler, job)
    assert str(plugin.stdout_populator.logfile) == str(stdout_file)
    assert (
        str(plugin.stderr_populator.logfile)
        == str(PanPath(tmp_path) / "0" / "job.stderr")
    )


async def test_on_job_started_existing_stdout_file_symlink(tmp_path):
    target = tmp_path / "real.stdout"
    target.write_text("x\n")
    link = tmp_path / "link.stdout"
    link.symlink_to(target)
    plugin = XquteCliGbatchPlugin(stdout_file=PanPath(link))
    scheduler = MagicMock()
    scheduler.workdir = PanPath(tmp_path)
    job = MagicMock()
    await plugin.on_job_started(scheduler, job)
    # symlink must be resolved to its target
    assert str(plugin.stdout_populator.logfile) == str(target)


async def test_on_job_started_stdout_file_missing_fallback(tmp_path, caplog):
    stdout_file = PanPath(tmp_path / "never.stdout")
    plugin = XquteCliGbatchPlugin(stdout_file=stdout_file)
    scheduler = MagicMock()
    scheduler.workdir = PanPath(tmp_path)
    job = MagicMock()
    with patch("asyncio.sleep", new_callable=AsyncMock):
        await plugin.on_job_started(scheduler, job)
    assert (
        str(plugin.stdout_populator.logfile)
        == str(PanPath(tmp_path) / "0" / "job.stdout")
    )
    assert "Still not found" in caplog.text


async def test_on_job_started_stdout_file_created_later(tmp_path, caplog):
    stdout_file = PanPath(tmp_path / "later.stdout")
    plugin = XquteCliGbatchPlugin(stdout_file=stdout_file)
    scheduler = MagicMock()
    scheduler.workdir = PanPath(tmp_path)
    job = MagicMock()

    def _create(*args, **kwargs):
        (tmp_path / "later.stdout").write_text("x\n")

    with patch("asyncio.sleep", new_callable=AsyncMock, side_effect=_create):
        await plugin.on_job_started(scheduler, job)
    assert str(plugin.stdout_populator.logfile) == str(stdout_file)
    assert "Found the running logs" in caplog.text


async def test_on_job_polling_skips_non_multiple_counter(tmp_path, caplog):
    plugin = make_plugin_with_logfiles(tmp_path, stdout_text="x\n")
    scheduler = MagicMock()
    job = MagicMock()
    await plugin.on_job_polling(scheduler, job, 1)
    assert "/STDOUT" not in caplog.text
    assert plugin.stdout_populator.counter == 0


async def test_on_job_polling_populates_both(tmp_path, caplog):
    plugin = make_plugin_with_logfiles(
        tmp_path, stdout_text="x\n", stderr_text="y\n"
    )
    scheduler = MagicMock()
    job = MagicMock()
    await plugin.on_job_polling(scheduler, job, 5)
    assert "/STDOUT x" in caplog.text
    assert "/STDERR y" in caplog.text
    assert plugin.stdout_populator.counter == 1
    assert plugin.stderr_populator.counter == 1


async def test_on_job_polling_stdout_block_guarded_by_stdout_populator(
    tmp_path, caplog
):
    # pins the fix: the stdout block must be guarded by stdout_populator
    plugin = XquteCliGbatchPlugin()
    plugin.stdout_populator.logfile = PanPath(tmp_path / "job.stdout")
    (tmp_path / "job.stdout").write_text("x\n")
    plugin.stderr_populator = None
    scheduler = MagicMock()
    job = MagicMock()
    await plugin.on_job_polling(scheduler, job, 5)
    assert "/STDOUT x" in caplog.text


async def test_on_job_killed_final_logs_and_residues(tmp_path, caplog):
    # stdout without trailing newline -> held as residue, printed on kill
    plugin = XquteCliGbatchPlugin()
    plugin.stdout_populator.logfile = PanPath(tmp_path / "job.stdout")
    (tmp_path / "job.stdout").write_text("x")
    plugin.stderr_populator.logfile = PanPath(tmp_path / "job.stderr")
    (tmp_path / "job.stderr").write_text("y\n")
    scheduler = MagicMock()
    job = MagicMock()
    await plugin.on_job_killed(scheduler, job)
    assert "/STDERR y" in caplog.text
    # from _clear_residues, decoded
    assert "/STDOUT x" in caplog.text
    assert plugin.stdout_populator.residue == ""


async def test_on_job_failed_suppresses_missing_logfile(caplog):
    plugin = XquteCliGbatchPlugin()
    plugin.stderr_populator.residue = b"tail"
    scheduler = MagicMock()
    job = MagicMock()
    await plugin.on_job_failed(scheduler, job)
    # logfile is None -> AttributeError suppressed, residues still cleared
    assert "/STDERR tail" in caplog.text
    assert plugin.stderr_populator.residue == ""


async def test_on_job_succeeded_suppresses_missing_logfile(caplog):
    plugin = XquteCliGbatchPlugin()
    plugin.stderr_populator.residue = b"tail"
    scheduler = MagicMock()
    job = MagicMock()
    await plugin.on_job_succeeded(scheduler, job)
    assert "/STDERR tail" in caplog.text
    assert plugin.stderr_populator.residue == ""


def test_on_shutdown_cleans_up():
    plugin = XquteCliGbatchPlugin()
    stdout_populator = MagicMock()
    stderr_populator = MagicMock()
    plugin.stdout_populator = stdout_populator
    plugin.stderr_populator = stderr_populator
    with patch("asyncio.create_task") as m_create_task:
        plugin.on_shutdown(None, None)
    m_create_task.assert_has_calls(
        [call(stdout_populator.destroy()), call(stderr_populator.destroy())]
    )
    assert plugin.stdout_populator is None
    assert plugin.stderr_populator is None


async def test_get_defaults_from_config_no_profile(tmp_path):
    conf_file = tmp_path / "conf.toml"
    conf_file.write_text("[default]\nfoo = 'bar'\n")
    defaults = await CliGbatchPlugin._get_defaults_from_config(
        [str(conf_file)], None
    )
    assert defaults == {}


async def test_get_defaults_from_config_profile(tmp_path):
    conf_file = tmp_path / "conf.toml"
    conf_file.write_text(
        "[default]\n"
        "foo = 'bar'\n"
        "[default.scheduler_opts]\n"
        "project = 'proj'\n"
        "[myprofile]\n"
        "baz = 123\n"
        "[myprofile.scheduler_opts]\n"
        "location = 'us-central1'\n"
    )
    defaults = await CliGbatchPlugin._get_defaults_from_config(
        [str(conf_file)], "myprofile"
    )
    # base "default" profile is pre-merged, "myprofile" wins
    assert defaults["foo"] == "bar"
    assert defaults["baz"] == 123
    assert defaults["scheduler_opts"] == {
        "project": "proj",
        "location": "us-central1",
    }


def test_init_sets_up_subparser():
    parser = MagicMock()
    subparser = MagicMock()
    plugin = CliGbatchPlugin(parser, subparser)
    assert plugin.parser is parser
    assert plugin.subparser is subparser
    assert subparser.usage == "pipen gbatch [options] -- <command>"
    assert subparser.pre_parse is _pre_parse
    assert "--mount-as-cwd" in subparser.epilog
    subparser._add_decedents.assert_called_once()
    args = subparser._add_decedents.call_args.args
    assert len(args) == 5
    assert args[2] == []
    assert args[4] == []


async def test_parse_args_strips_dashdash_command():
    plugin = CliGbatchPlugin(MagicMock(), MagicMock())
    ns = Namespace(profile=None, command=["--", "cmd"])
    parsed = await plugin.parse_args(ns, [])
    assert parsed.command == ["cmd"]
    assert parsed._other_opts == {}


async def test_parse_args_no_command_exits():
    plugin = CliGbatchPlugin(MagicMock(), MagicMock())
    ns = Namespace(profile=None)
    with pytest.raises(SystemExit):
        await plugin.parse_args(ns, [])
    plugin.subparser.print_help.assert_called_once()


async def test_parse_args_command_not_after_dashdash():
    plugin = CliGbatchPlugin(MagicMock(), MagicMock())
    ns = Namespace(profile=None, command=["cmd"])
    with pytest.raises(ValueError):
        await plugin.parse_args(ns, [])


async def test_parse_args_profile_defaults_merge(tmp_path):
    conf_file = tmp_path / "conf.toml"
    conf_file.write_text(
        "[myprofile]\n"
        "custom_opt = 'val'\n"
        "[myprofile.scheduler_opts]\n"
        "mount = 'c:d'\n"
        "location = 'us-central1'\n"
    )
    plugin = CliGbatchPlugin(MagicMock(), MagicMock())
    ns = Namespace(
        profile="myprofile",
        command=["--", "cmd"],
        mount=["a:b"],
        location="europe-west1",
    )
    with patch("pipen_cli_gbatch.plugins.CONFIG_FILES", [str(conf_file)]):
        parsed = await plugin.parse_args(ns, [])
    # a single (non-list) mount from config is wrapped and extended
    # with the command line one
    assert parsed.mount == ["c:d", "a:b"]
    # valid command line value is not overridden
    assert parsed.location == "europe-west1"
    # other options are stored for the daemon
    assert parsed._other_opts == {"custom_opt": "val"}


async def test_parse_args_defaults_skip_none_and_command():
    plugin = CliGbatchPlugin(MagicMock(), MagicMock())
    ns = Namespace(profile=None, command=["--", "cmd"])
    with patch.object(
        CliGbatchPlugin,
        "_get_defaults_from_config",
        new=AsyncMock(
            return_value={
                "scheduler_opts": {"foo": None, "command": ["x"], "bar": "x"},
                "other": 1,
            }
        ),
    ):
        parsed = await plugin.parse_args(ns, [])
    assert not hasattr(parsed, "foo")
    assert parsed.command == ["cmd"]
    assert parsed.bar == "x"
    assert parsed._other_opts == {"other": 1}

    # a bool default is considered valid and does not override the
    # command line value
    ns = Namespace(profile=None, command=["--", "cmd"], flag=False)
    with patch.object(
        CliGbatchPlugin,
        "_get_defaults_from_config",
        new=AsyncMock(return_value={"scheduler_opts": {"flag": True}}),
    ):
        parsed = await plugin.parse_args(ns, [])
    assert parsed.flag is False


async def test_parse_args_no_profile_no_defaults(tmp_path):
    conf_file = tmp_path / "nonexist.toml"
    plugin = CliGbatchPlugin(MagicMock(), MagicMock())
    ns = Namespace(profile=None, command=["--", "cmd"])
    with patch("pipen_cli_gbatch.plugins.CONFIG_FILES", [str(conf_file)]):
        parsed = await plugin.parse_args(ns, [])
    assert parsed.command == ["cmd"]
    assert parsed._other_opts == {}


async def test_exec_command():
    plugin = CliGbatchPlugin(MagicMock(), MagicMock())
    with (
        patch(
            "pipen_cli_gbatch.CliGbatchDaemonPlain.run",
            new_callable=AsyncMock,
        ) as m_plain,
        patch(
            "pipen_cli_gbatch.CliGbatchDaemonPipeline.run",
            new_callable=AsyncMock,
        ) as m_pipeline,
    ):
        await plugin.exec_command(Namespace(plain=True, command=["cmd"]))
        await plugin.exec_command(Namespace(plain=False, command=["cmd"]))
    m_plain.assert_awaited_once()
    m_pipeline.assert_awaited_once()
