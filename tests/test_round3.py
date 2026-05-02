"""Tests for cross-platform behavior and CLI rendering:

- ps -o lstart= timestamp parse treats input as local time and
  converts to UTC (PID-reuse defense on macOS without psutil)
- parent log_fd closed after Popen — no fd leak per spawn
- runner module docstring describes the upstream delivery delegation
  rather than claiming inline Telegram dispatch
- CLI shows "delivery failed" (not the older "delivered failed" typo)
"""
from __future__ import annotations

import os
import resource
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ─── ps -o lstart= timezone parsing ─────────────────────────────────────


def test_ps_fallback_treats_lstart_as_local_time(scheduler_module, monkeypatch):
    """The `ps -o lstart=` fallback must parse the timestamp as local
    time and convert to UTC, not tag the parsed naive value as UTC.

    Pre-fix: on a system with non-UTC local TZ, `_process_create_time`
    returned a UTC-tagged datetime that was actually local time, so the
    comparison against the JSON pidfile's UTC `started_at` was always
    off by the local TZ offset — defeating the PID-reuse defense.
    """
    # Force the psutil + /proc paths to fail so we exercise the ps fallback
    monkeypatch.setitem(sys.modules, "psutil", None)

    # Block /proc reads
    real_path_read_text = Path.read_text
    real_path_read_bytes = Path.read_bytes
    def block_proc_text(self, *a, **kw):
        if str(self).startswith("/proc/"):
            raise OSError("blocked for test")
        return real_path_read_text(self, *a, **kw)
    monkeypatch.setattr(Path, "read_text", block_proc_text)

    # Stub `ps -p N -o lstart=` to return a known LOCAL-time string
    # representing some specific moment. Our recorded `started_at` will
    # be the same moment in UTC. If the parser is correct, the two
    # should match exactly.
    fixed_local = datetime(2026, 5, 2, 14, 37, 15)  # naive local
    fixed_local_str = fixed_local.strftime("%a %b %d %H:%M:%S %Y")
    fixed_utc = fixed_local.astimezone().astimezone(timezone.utc)

    def fake_subprocess_run(cmd, **kw):
        if cmd[:1] == ["ps"]:
            r = MagicMock()
            r.returncode = 0
            r.stdout = fixed_local_str + "\n"
            r.stderr = ""
            return r
        raise FileNotFoundError("unexpected cmd")
    monkeypatch.setattr(subprocess, "run", fake_subprocess_run)

    parsed = scheduler_module._process_create_time(99999)
    assert parsed is not None, "ps fallback returned None"
    # parsed and fixed_utc should be equal modulo seconds
    delta = abs((parsed - fixed_utc).total_seconds())
    assert delta < 2, (
        f"ps fallback returned {parsed} but expected {fixed_utc} "
        f"(delta={delta}s) — local→UTC conversion broken"
    )


# ─── log_fd leak prevention ──────────────────────────────────


def test_spawn_does_not_leak_log_fd(scheduler_module, jobs_module, monkeypatch):
    """_spawn_job_subprocess must close the parent's copy of log_fd
    after Popen dups it into the child. Otherwise a long-lived
    scheduler eventually exhausts its fd limit."""
    j = jobs_module.create_job(
        name="fd-test", schedule={"kind": "interval", "interval_s": 60},
    )

    # Stub Popen to return a sentinel proc and capture the stdout fd
    captured_fds = []
    def fake_popen(*args, **kwargs):
        fd = kwargs.get("stdout")
        if hasattr(fd, "fileno"):
            captured_fds.append(fd.fileno())
        proc = MagicMock()
        proc.pid = 11111
        return proc
    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    # Count open fds in /proc/self before + after a batch of spawns
    if not Path("/proc/self/fd").exists():
        pytest.skip("requires /proc on Linux to count fds")

    def count_open_fds() -> int:
        return len(list(Path("/proc/self/fd").iterdir()))

    before = count_open_fds()
    for i in range(20):
        # Need to vary the timestamp to avoid log filename collisions
        # (same job, same name, same second). _safe_log_filename will
        # produce identical names — force a unique name per spawn.
        j["name"] = f"fd-test-{i}"
        scheduler_module._spawn_job_subprocess(j)
    after = count_open_fds()

    # Pre-fix: each spawn leaks 1 fd → +20 fds. Post-fix: ~0.
    leaked = after - before
    assert leaked < 5, (
        f"_spawn_job_subprocess leaked {leaked} fd(s) over 20 spawns; "
        f"parent's log_fd not being closed after Popen dup"
    )


# ─── runner docstring describes upstream delegation ──────────────────────────────


def test_runner_docstring_does_not_claim_inline_telegram(temp_hermes_home):
    """The runner module's docstring used to claim Telegram delivery
    was implemented inline. Delivery delegates to upstream — docstring must reflect that."""
    # Stub minimal Hermes-runtime modules so runner imports cleanly
    cron_pkg = type(sys)("cron")
    cron_scheduler = type(sys)("cron.scheduler")
    cron_scheduler.run_job = MagicMock(return_value=(True, "doc", "response", None))
    cron_pkg.scheduler = cron_scheduler
    sys.modules.setdefault("cron", cron_pkg)
    sys.modules.setdefault("cron.scheduler", cron_scheduler)
    hcli = type(sys)("hermes_cli")
    hcli_env = type(sys)("hermes_cli.env_loader")
    hcli_env.load_hermes_dotenv = lambda: None
    hcli.env_loader = hcli_env
    sys.modules.setdefault("hermes_cli", hcli)
    sys.modules.setdefault("hermes_cli.env_loader", hcli_env)

    import runner
    doc = runner.__doc__ or ""
    # Must NOT claim inline Telegram — docstring used to say:
    # "Telegram delivery is implemented inline using the bot token..."
    assert "implemented inline" not in doc, (
        "runner docstring still claims inline Telegram delivery"
    )
    # SHOULD reference the upstream delegation
    assert "_deliver_result" in doc or "deliver_result" in doc, (
        "runner docstring should mention upstream _deliver_result delegation"
    )
    # SHOULD mention [SILENT] handling
    assert "[SILENT]" in doc or "SILENT" in doc, (
        "runner docstring should mention [SILENT] marker handling"
    )


# ─── CLI delivery-failure rendering ────────────────────────────────────────────


def test_cli_renders_delivery_failed_correctly(temp_hermes_home):
    """CLI list output for a job with delivery error should say
    'delivery failed', not 'delivered failed'."""
    # Spin up a job with a delivery error and check cli's _cmd_list output
    import jobs as jobs_mod
    import cli
    j = jobs_mod.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    jobs_mod.mark_job_run(
        j["id"], success=True,
        delivery_error="telegram api error",
    )

    import io, contextlib, argparse
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        cli._cmd_list(argparse.Namespace())
    out = buf.getvalue()
    assert "delivery failed" in out, (
        f"CLI output missing 'delivery failed' marker: {out!r}"
    )
    assert "delivered failed" not in out, (
        "CLI still has the 'delivered failed' typo"
    )
