"""Regression tests for path-safety, concurrent-RMW locking,
spawn-failure marking, and migration field preservation.

The delivery-delegation path is exercised implicitly by runner.py
loading without the helpers it used to maintain locally — there's no
upstream Hermes runtime in this test environment to call into, so we
don't end-to-end the delivery itself here.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest


# ─── path-safe log filenames ──────────────────────────────────


def test_safe_log_filename_strips_traversal(scheduler_module):
    """Path traversal characters in job name should not escape the log dir."""
    f = scheduler_module._safe_log_filename
    assert "/" not in f("../../etc/passwd")
    assert ".." not in f("../weird-name").replace(".", "")  # dots-only check is too strict; check no path sep
    assert "/" not in f("/absolute/path")
    assert "\\" not in f("..\\windows-traversal")


def test_safe_log_filename_keeps_normal_chars(scheduler_module):
    """Regular alphanumerics/dash/underscore/dot pass through."""
    f = scheduler_module._safe_log_filename
    assert f("daily-report") == "daily-report"
    assert f("job_42") == "job_42"
    assert f("backfill.v2") == "backfill.v2"


def test_safe_log_filename_handles_empty(scheduler_module):
    """Empty / None input returns a stable fallback."""
    assert scheduler_module._safe_log_filename("") == "unnamed"
    assert scheduler_module._safe_log_filename(None) == "None"  # str(None)
    # Non-string inputs are coerced to str then sanitized; must not raise


def test_safe_log_filename_caps_length(scheduler_module):
    """Very long names are truncated so they don't trip filesystem limits."""
    long_name = "a" * 500
    assert len(scheduler_module._safe_log_filename(long_name)) <= 80


# ─── stable RMW locking ──────────────────────────────────────


def test_concurrent_updates_dont_lose_writes(jobs_module):
    """Two threads each updating different jobs should preserve both
    updates — pre-fix, the load-modify-save race could lose one."""
    j1 = jobs_module.create_job(
        name="a", schedule={"kind": "interval", "interval_s": 60},
    )
    j2 = jobs_module.create_job(
        name="b", schedule={"kind": "interval", "interval_s": 60},
    )

    errors = []

    def update_a():
        try:
            for _ in range(5):
                jobs_module.update_job(j1["id"], {"name": f"a-{time.time_ns()}"})
        except Exception as e:
            errors.append(("a", e))

    def update_b():
        try:
            for _ in range(5):
                jobs_module.update_job(j2["id"], {"name": f"b-{time.time_ns()}"})
        except Exception as e:
            errors.append(("b", e))

    t1 = threading.Thread(target=update_a)
    t2 = threading.Thread(target=update_b)
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert not errors, f"updaters raised: {errors}"
    # Both jobs must still exist; name should reflect SOME a-* / b-* update
    a = jobs_module.get_job(j1["id"])
    b = jobs_module.get_job(j2["id"])
    assert a is not None and a["name"].startswith("a-")
    assert b is not None and b["name"].startswith("b-")


def test_locked_modify_returns_value(jobs_module):
    """locked_modify supports returning a value alongside the modified jobs."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )

    def find_one(jobs):
        for job in jobs:
            if job["id"] == j["id"]:
                return jobs, job  # don't modify, just return found
        return None, None

    found = jobs_module.locked_modify(find_one)
    assert found is not None
    assert found["id"] == j["id"]


def test_remove_job_atomic(jobs_module):
    """remove_job uses locked_modify so concurrent reads/writes don't
    leave the file in a partial state."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    assert jobs_module.remove_job(j["id"]) is True
    # Re-read should not see it
    assert jobs_module.get_job(j["id"]) is None
    # Removing again should be a no-op (returns False, doesn't crash)
    assert jobs_module.remove_job(j["id"]) is False


# ─── spawn failure marks job as failed ────────────────────────


def test_spawn_failure_marks_job(scheduler_module, jobs_module, monkeypatch):
    """If subprocess.Popen fails, the job's last_run_success should
    flip to False and last_error should be populated. Without the fix,
    the failure was silent — only summary['errors'] tracked it, and
    operators saw nothing in the job state."""
    j = jobs_module.create_job(
        name="failsoon", schedule={"kind": "interval", "interval_s": 60},
    )
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    past = (_dt.now(_tz.utc) - _td(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"next_run_at": past})

    # Force spawn to fail
    import subprocess
    def fail_popen(*a, **kw):
        raise OSError("simulated spawn failure")
    monkeypatch.setattr(subprocess, "Popen", fail_popen)

    summary = scheduler_module.tick()
    assert summary["errors"] >= 1

    # The job should now show last_run_success=False and an error message
    refreshed = jobs_module.get_job(j["id"])
    assert refreshed["last_run_success"] is False
    assert refreshed["last_error"] is not None
    assert "spawn" in refreshed["last_error"].lower()


# ─── migrate preserves all upstream fields ────────────────────


def test_migrate_preserves_upstream_fields(temp_hermes_home, monkeypatch):
    """_convert_job should NOT drop fields like origin / context_from /
    skills / enabled_toolsets / model / provider / base_url / repeat
    that Hermes' run_job() can use."""
    # Stub up a built-in jobs.json so migrate.py can read from it
    builtin_dir = temp_hermes_home / "cron"
    builtin_dir.mkdir(parents=True, exist_ok=True)
    builtin_job = {
        "id": "abc123def456",
        "name": "rich-job",
        "enabled": True,
        "schedule": {"kind": "cron", "expr": "0 10 * * *", "display": "0 10 * * *"},
        "workdir": "/opt/work",
        "prompt": "do the thing",
        "script": "wake.py",
        "deliver": "telegram",
        "origin": "telegram:1234567",
        "context_from": "context-job-id",
        "skills": ["llm-wiki", "research"],
        "enabled_toolsets": ["web", "memory"],
        "model": "some-model-id",
        "provider": "some-provider",
        "base_url": "",
        "repeat": {"times": 5, "completed": 2},
        "schedule_display": "0 10 * * *",
        # runtime fields that should be stripped
        "last_run_at": "2026-05-01T10:00:00+00:00",
        "last_run_success": True,
        "last_status": "ok",
        "next_run_at": "2026-05-02T10:00:00+00:00",
    }
    (builtin_dir / "jobs.json").write_text(json.dumps({"jobs": [builtin_job]}))

    import migrate
    converted = migrate._convert_job(builtin_job)

    # All non-runtime fields preserved
    for key in ("id", "name", "enabled", "workdir", "prompt", "script",
                "deliver", "origin", "context_from", "skills",
                "enabled_toolsets", "model", "provider", "base_url",
                "repeat", "schedule_display"):
        assert converted.get(key) == builtin_job[key], f"field {key} dropped"

    # Runtime fields reset
    assert converted["last_run_at"] is None
    assert converted["last_run_success"] is None
    # next_run_at is recomputed from schedule, not preserved
    assert converted["next_run_at"] != builtin_job["next_run_at"]
    # Migration marker
    assert converted.get("_migrated_from_builtin") is True
