"""Tests for scheduler.py — tick logic, lock contention, idempotency.

Critical invariants:
- tick() acquires + releases the file lock cleanly
- tick() advances next_run_at BEFORE spawning (crash safety)
- tick() spawns one Popen per due job (true parallelism)
- tick() skips jobs whose previous run is still alive
- _job_is_running cleans up stale PID files when proc gone
- Tick errors don't poison the ticker thread
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


# ─── tick() basic behavior ────────────────────────────────────────────


def test_tick_no_jobs_due(scheduler_module):
    """No due jobs → empty summary, no Popen calls."""
    summary = scheduler_module.tick()
    assert summary["due"] == 0
    assert summary["spawned"] == 0


def test_tick_spawns_one_subprocess_per_due_job(scheduler_module, jobs_module, monkeypatch):
    """For each due job, tick should call subprocess.Popen exactly once."""
    j1 = jobs_module.create_job(
        name="a", schedule={"kind": "interval", "interval_s": 60},
    )
    j2 = jobs_module.create_job(
        name="b", schedule={"kind": "interval", "interval_s": 60},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j1["id"], {"next_run_at": past})
    jobs_module.update_job(j2["id"], {"next_run_at": past})

    popen_calls = []
    def fake_popen(*args, **kwargs):
        popen_calls.append((args, kwargs))
        proc = MagicMock()
        proc.pid = 12345
        return proc
    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    summary = scheduler_module.tick()
    assert summary["due"] == 2
    assert summary["spawned"] == 2
    assert len(popen_calls) == 2


def test_tick_advances_next_run_before_spawn(scheduler_module, jobs_module, monkeypatch):
    """Critical for crash safety: next_run must be advanced BEFORE
    Popen, so a crash mid-spawn doesn't cause the same job to fire
    again on the next tick."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 600},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"next_run_at": past})

    # Capture next_run_at when subprocess.Popen is invoked
    captured_next_run = []
    def fake_popen(*args, **kwargs):
        # At this point, advance_next_run should have already run
        captured_next_run.append(jobs_module.get_job(j["id"])["next_run_at"])
        proc = MagicMock()
        proc.pid = 1
        return proc
    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    scheduler_module.tick()
    assert len(captured_next_run) == 1
    new_next = datetime.fromisoformat(captured_next_run[0])
    # Should be advanced into the future (past + 600s)
    assert new_next > datetime.now(timezone.utc)


def test_tick_skips_disabled_jobs(scheduler_module, jobs_module, monkeypatch):
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"enabled": False, "next_run_at": past})

    popen_calls = []
    monkeypatch.setattr(subprocess, "Popen",
                        lambda *a, **kw: popen_calls.append(1) or MagicMock(pid=1))

    summary = scheduler_module.tick()
    assert summary["spawned"] == 0
    assert len(popen_calls) == 0


# ─── Idempotency ──────────────────────────


def test_tick_skips_job_with_alive_previous_run(scheduler_module, jobs_module, monkeypatch):
    """If a previous subprocess is still alive (PID file exists +
    process responding to signal 0), tick should skip the job and
    NOT spawn a duplicate."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"next_run_at": past})

    # Patch _job_is_running to claim the previous run is still alive
    monkeypatch.setattr(scheduler_module, "_job_is_running", lambda jid: True)

    popen_calls = []
    monkeypatch.setattr(subprocess, "Popen",
                        lambda *a, **kw: popen_calls.append(1) or MagicMock(pid=1))

    summary = scheduler_module.tick()
    assert summary["due"] == 1
    assert summary["spawned"] == 0
    assert summary["skipped_running"] == 1
    assert len(popen_calls) == 0


def test_job_is_running_returns_false_for_missing_pid_file(scheduler_module):
    assert scheduler_module._job_is_running("never-existed") is False


def test_job_is_running_cleans_up_stale_pid_file(scheduler_module):
    """A PID file pointing at a dead PID should be cleaned up."""
    pid_file = scheduler_module._job_pid_file("dead-job")
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    # Use a PID that's almost certainly dead (very high number)
    pid_file.write_text("999999")
    assert scheduler_module._job_is_running("dead-job") is False
    # Should have been cleaned up
    assert not pid_file.exists()


def test_job_is_running_handles_garbage_pid_file(scheduler_module):
    """Non-numeric PID file content shouldn't crash."""
    pid_file = scheduler_module._job_pid_file("garbage-job")
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text("not a number")
    assert scheduler_module._job_is_running("garbage-job") is False


# ─── File lock contention ─────────────────────────────────────────────


def test_tick_skips_when_lock_held(scheduler_module, jobs_module, monkeypatch):
    """Second concurrent tick should bail out cleanly when the lock
    is already held."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"next_run_at": past})

    # Hold the lock manually
    held = scheduler_module._try_acquire_lock()
    assert held is not None, "expected to acquire fresh lock"

    try:
        # Now tick() from another would-be tick — should fail to acquire
        summary = scheduler_module.tick()
        assert summary.get("skipped_lock_contention") is True
    finally:
        scheduler_module._release_lock(held)


def test_tick_releases_lock_after_completion(scheduler_module, jobs_module):
    """After tick(), the lock should be released so the next tick
    can acquire it."""
    scheduler_module.tick()  # 0 due jobs, fast path

    # Subsequent acquire should succeed
    held = scheduler_module._try_acquire_lock()
    assert held is not None
    scheduler_module._release_lock(held)


def test_tick_releases_lock_on_exception(scheduler_module, jobs_module, monkeypatch):
    """Lock must be released even if processing raises."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"next_run_at": past})

    def crashing_popen(*a, **kw):
        raise RuntimeError("simulated spawn crash")
    monkeypatch.setattr(subprocess, "Popen", crashing_popen)

    # tick() catches exceptions per-job and counts as errors,
    # so it returns normally even though spawn crashed
    summary = scheduler_module.tick()
    assert summary["errors"] >= 1

    # Lock should be released
    held = scheduler_module._try_acquire_lock()
    assert held is not None
    scheduler_module._release_lock(held)


# ─── Subprocess construction ──────────────────────────────────────────


def test_spawn_constructs_correct_subprocess_args(scheduler_module, jobs_module, monkeypatch):
    """The Popen call should reference runner.py with --job-id <id>."""
    j = jobs_module.create_job(
        name="my-job", schedule={"kind": "interval", "interval_s": 60},
    )

    captured = {}
    def fake_popen(cmd, *args, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        proc = MagicMock()
        proc.pid = 7777
        return proc
    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    scheduler_module._spawn_job_subprocess(j)
    assert "runner.py" in captured["cmd"][1]
    assert "--job-id" in captured["cmd"]
    assert j["id"] in captured["cmd"]
    # Env should be inherited (so dotenv-loaded keys propagate)
    assert captured["env"] is not None
    assert "PATH" in captured["env"]


def test_spawn_writes_pid_file(scheduler_module, jobs_module, monkeypatch):
    """Spawn writes the subprocess PID + start time as JSON to the
    per-job pid file (placeholder — real PID + start time get
    overwritten by runner.py on entry). Format is JSON {pid, started_at}
    so PID-reuse can be detected on the next idempotency check."""
    import json as _json
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    monkeypatch.setattr(subprocess, "Popen",
                        lambda *a, **kw: MagicMock(pid=42424))

    scheduler_module._spawn_job_subprocess(j)
    pid_file = scheduler_module._job_pid_file(j["id"])
    assert pid_file.exists()
    rec = _json.loads(pid_file.read_text())
    assert rec["pid"] == 42424
    assert "started_at" in rec  # ISO 8601 timestamp


def test_spawn_disables_inner_ticker(scheduler_module, jobs_module, monkeypatch):
    """The runner subprocess must inherit CRON_PLUS_DISABLED=1 so that
    when Hermes' plugin loader walks register() during agent runtime
    init, _start_ticker_thread bails out instead of spawning a fresh
    daemon ticker inside the short-lived runner process. Without this
    guard, every fired job leaves an inner ticker that competes for the
    tick lock, logs noisy 'ticker started' lines, and (under load) can
    claim+spawn additional due jobs before the runner exits."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    captured = {}
    def fake_popen(cmd, *args, **kwargs):
        captured["env"] = kwargs.get("env")
        proc = MagicMock()
        proc.pid = 9999
        return proc
    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    scheduler_module._spawn_job_subprocess(j)
    assert captured["env"] is not None
    assert captured["env"].get("CRON_PLUS_DISABLED") == "1", (
        "spawn must set CRON_PLUS_DISABLED=1 so the runner subprocess "
        "doesn't start a redundant inner ticker"
    )


# ─── Ticker resilience ────────────────────────────────────────────────


def test_run_ticker_survives_tick_exceptions(scheduler_module, monkeypatch):
    """A buggy tick() that raises must NOT kill the daemon thread.
    Otherwise one bad day takes down the whole scheduler."""
    call_count = [0]
    def buggy_tick():
        call_count[0] += 1
        if call_count[0] == 1:
            raise RuntimeError("first call crashes")
        return {"due": 0, "spawned": 0}

    monkeypatch.setattr(scheduler_module, "tick", buggy_tick)

    # Patch sleep IN the scheduler module's namespace (it uses
    # `import time; time.sleep(...)` so the binding to patch is
    # scheduler.time.sleep, not the global time.sleep).
    sleep_calls = [0]
    def fake_sleep(s):
        sleep_calls[0] += 1
        if sleep_calls[0] >= 2:
            raise SystemExit  # break out of the loop
    monkeypatch.setattr(scheduler_module.time, "sleep", fake_sleep)

    # Should not propagate the RuntimeError; should sleep + retry
    scheduler_module.run_ticker(interval_s=1)
    assert call_count[0] >= 2, "ticker died after first tick exception"


# ─── self-heal: null next_run_at on enabled jobs ──────────────────────


def test_claim_due_jobs_self_heals_null_next_run(jobs_module):
    """Regression: an enabled job with next_run_at=null (e.g. fresh
    deploy from a sanitized jobs.json) should be self-healed by
    claim_due_jobs() — the next_run_at recomputed from its schedule.

    Pre-fix: jobs sat silent forever after a sanitized deploy until an
    external workaround (seed-cron-plus.py) wrote next_run_at, because
    both get_due_jobs() and the original claim loop short-circuit on
    null nra. This test verifies the heal phase inside
    claim_due_jobs() now populates the timestamp atomically.
    """
    job = jobs_module.create_job(
        name="self-heal-target",
        schedule={"kind": "interval", "interval_s": 3600},
    )
    # Force the null-nra state that a sanitized deploy produces
    jobs_module.update_job(job["id"], {"next_run_at": None})
    assert jobs_module.get_job(job["id"])["next_run_at"] is None

    # Claim — heal phase should populate next_run_at even though the
    # job is not yet "due" (the freshly-computed timestamp is in the
    # future, so it's not in `claimed`).
    claimed = jobs_module.claim_due_jobs()

    healed = jobs_module.get_job(job["id"])
    assert healed["next_run_at"] is not None, \
        "claim_due_jobs() must self-heal null next_run_at on enabled jobs"
    # Heal computes the next scheduled time, which is in the future, so
    # the job should NOT have been claimed this tick.
    assert all(c["id"] != job["id"] for c in claimed), \
        "freshly-healed job should not be claimed in the same tick"


def test_claim_due_jobs_does_not_heal_disabled_jobs(jobs_module):
    """Disabled jobs with null next_run_at stay null — heal targets
    only enabled jobs (paused jobs intentionally have null nra)."""
    job = jobs_module.create_job(
        name="paused-target",
        schedule={"kind": "interval", "interval_s": 3600},
    )
    jobs_module.update_job(job["id"], {
        "next_run_at": None,
        "enabled": False,
    })
    jobs_module.claim_due_jobs()
    assert jobs_module.get_job(job["id"])["next_run_at"] is None, \
        "disabled jobs must not be self-healed"


def test_get_due_jobs_does_not_heal(jobs_module):
    """get_due_jobs() must remain read-only — it returns [] for
    null-nra jobs and does NOT mutate the next_run_at field. Heal is
    claim_due_jobs()'s job."""
    job = jobs_module.create_job(
        name="readonly-target",
        schedule={"kind": "interval", "interval_s": 3600},
    )
    jobs_module.update_job(job["id"], {"next_run_at": None})

    due = jobs_module.get_due_jobs()
    assert all(d["id"] != job["id"] for d in due), \
        "null-nra job should not appear in get_due_jobs() result"
    assert jobs_module.get_job(job["id"])["next_run_at"] is None, \
        "get_due_jobs() must NOT mutate next_run_at"
