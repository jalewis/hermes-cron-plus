"""Tests for jobs.py — CRUD, schedule eval, due filtering, trigger semantics.

Critical invariants exercised here (the bug fixes documented in
WHY.md and UPSTREAM_HERMES_CRON_BUG_REPORT.md):

- compute_next_run handles cron / interval / once correctly
- create_job populates next_run_at via compute_next_run
- trigger_job sets next_run = now - 1s (avoids trigger-vs-tick race)
- trigger_job refuses if previous subprocess still alive (idempotency)
- get_due_jobs filters by enabled + next_run <= now
- advance_next_run auto-disables one-shot jobs after firing
- File save is atomic (no partial writes corrupt jobs.json)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


def _require_croniter():
    """Skip helper: cron-kind schedule tests need the croniter package.
    Install via pip install -e ".[dev]" — see pyproject.toml."""
    pytest.importorskip(
        "croniter",
        reason='requires croniter — pip install -e ".[dev]"',
    )


# ─── compute_next_run ──────────────────────────────────────────────────


def test_compute_next_run_cron(jobs_module):
    """Cron expression yields ISO timestamp of next firing."""
    _require_croniter()
    anchor = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)  # Sat noon UTC
    nxt = jobs_module.compute_next_run(
        {"kind": "cron", "expr": "0 10 * * 0"},  # Sunday 10:00 UTC
        anchor=anchor,
    )
    parsed = datetime.fromisoformat(nxt)
    assert parsed == datetime(2026, 5, 3, 10, 0, tzinfo=timezone.utc)


def test_compute_next_run_interval(jobs_module):
    """Interval yields anchor + interval_s."""
    anchor = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
    nxt = jobs_module.compute_next_run(
        {"kind": "interval", "interval_s": 600}, anchor=anchor,
    )
    parsed = datetime.fromisoformat(nxt)
    assert parsed == anchor + timedelta(seconds=600)


def test_compute_next_run_once_future(jobs_module):
    """Future one-shot returns the run_at timestamp."""
    anchor = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
    target = datetime(2026, 5, 3, 18, 0, tzinfo=timezone.utc)
    nxt = jobs_module.compute_next_run(
        {"kind": "once", "run_at": target.isoformat()}, anchor=anchor,
    )
    assert datetime.fromisoformat(nxt) == target


def test_compute_next_run_once_past(jobs_module):
    """Past one-shot returns None — won't fire again."""
    anchor = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
    past = (anchor - timedelta(days=1)).isoformat()
    assert jobs_module.compute_next_run({"kind": "once", "run_at": past}, anchor=anchor) is None


def test_compute_next_run_invalid_cron(jobs_module):
    """Invalid cron expr returns None instead of raising."""
    assert jobs_module.compute_next_run({"kind": "cron", "expr": "not a valid expr"}) is None


def test_compute_next_run_unknown_kind(jobs_module):
    """Unknown schedule kind returns None."""
    assert jobs_module.compute_next_run({"kind": "monthly_phase_of_moon"}) is None


# ─── CRUD ──────────────────────────────────────────────────────────────


def test_create_job_populates_next_run(jobs_module):
    """create_job sets next_run_at via compute_next_run."""
    job = jobs_module.create_job(
        name="test", schedule={"kind": "interval", "interval_s": 600},
    )
    assert job["next_run_at"] is not None
    assert job["enabled"] is True
    assert job["last_run_at"] is None
    assert job["created_at"] is not None
    assert len(job["id"]) == 12  # matches Hermes 12-char hex IDs


def test_create_job_id_is_unique(jobs_module):
    """Multiple creates yield distinct IDs."""
    ids = {jobs_module.create_job(
        name=f"j{i}", schedule={"kind": "interval", "interval_s": 60},
    )["id"] for i in range(20)}
    assert len(ids) == 20


def test_get_job_returns_none_for_missing(jobs_module):
    assert jobs_module.get_job("nonexistent") is None


def test_get_job_returns_created(jobs_module):
    created = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    fetched = jobs_module.get_job(created["id"])
    assert fetched is not None
    assert fetched["id"] == created["id"]
    assert fetched["name"] == "t"


def test_update_job_partial(jobs_module):
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    updated = jobs_module.update_job(j["id"], {"name": "renamed"})
    assert updated["name"] == "renamed"
    # Other fields preserved
    assert updated["id"] == j["id"]
    assert updated["enabled"] is True


def test_update_job_recomputes_next_run_when_schedule_changes(jobs_module):
    _require_croniter()
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    original_next = j["next_run_at"]
    updated = jobs_module.update_job(
        j["id"], {"schedule": {"kind": "cron", "expr": "0 0 1 1 *"}},
    )
    assert updated["next_run_at"] != original_next
    # Should be Jan 1 next year
    assert "01-01" in updated["next_run_at"]


def test_remove_job(jobs_module):
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    assert jobs_module.remove_job(j["id"]) is True
    assert jobs_module.get_job(j["id"]) is None
    assert jobs_module.remove_job(j["id"]) is False  # no-op on missing


def test_pause_resume(jobs_module):
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    paused = jobs_module.pause_job(j["id"])
    assert paused["enabled"] is False
    assert paused["paused_at"] is not None

    resumed = jobs_module.resume_job(j["id"])
    assert resumed["enabled"] is True
    assert resumed["paused_at"] is None


def test_resume_recomputes_stale_next_run(jobs_module):
    """Resume should recompute next_run if it's in the past."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 600},
    )
    # Manually corrupt next_run to be 1 hour ago
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    jobs_module.update_job(j["id"], {"enabled": False, "next_run_at": past})
    resumed = jobs_module.resume_job(j["id"])
    new_next = datetime.fromisoformat(resumed["next_run_at"])
    assert new_next > datetime.now(timezone.utc)


# ─── trigger_job semantics ──────────


def test_trigger_sets_next_run_in_past(jobs_module):
    """trigger_job sets next_run = now - 1s, NOT now. Without the
    -1s offset, a tick that samples `now` 1ms before the trigger
    lands would miss the triggered job entirely (race documented in
    docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md)."""
    _require_croniter()
    j = jobs_module.create_job(
        name="t", schedule={"kind": "cron", "expr": "0 0 1 1 *"},
    )
    sample_now = datetime.now(timezone.utc)
    triggered = jobs_module.trigger_job(j["id"])
    next_run = datetime.fromisoformat(triggered["next_run_at"])
    # Must be strictly before sample_now (i.e., in the past relative
    # to any subsequent tick) so get_due_jobs's `next_run <= now`
    # filter includes it.
    assert next_run < sample_now, (
        f"trigger set next_run={next_run} but sample_now={sample_now}; "
        "tick race not protected against — see docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md"
    )


def test_trigger_refuses_if_previous_alive(jobs_module, monkeypatch):
    """trigger_job should refuse to overwrite next_run if a prior
    subprocess is still alive — duplicate-trigger pile-up guard."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 600},
    )
    # First trigger sets next_run to past
    first = jobs_module.trigger_job(j["id"])
    first_next = first["next_run_at"]

    # Now simulate "previous run still alive" — patch _job_is_running
    # in scheduler to return True
    import scheduler  # noqa: PLC0415
    monkeypatch.setattr(scheduler, "_job_is_running", lambda jid: True)

    # Second trigger should be a no-op (returns the existing job state,
    # doesn't touch next_run_at)
    second = jobs_module.trigger_job(j["id"])
    assert second["next_run_at"] == first_next, (
        "trigger_job overwrote next_run while a prior run was still "
        "alive — idempotency guard not working"
    )


def test_trigger_returns_none_for_missing(jobs_module):
    assert jobs_module.trigger_job("nonexistent") is None


# ─── get_due_jobs ──────────────────────────────────────────────────────


def test_get_due_jobs_empty(jobs_module):
    assert jobs_module.get_due_jobs() == []


def test_get_due_jobs_skips_disabled(jobs_module):
    """Disabled jobs never appear in due_jobs even if next_run is past."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"enabled": False, "next_run_at": past})
    assert jobs_module.get_due_jobs() == []


def test_get_due_jobs_skips_future(jobs_module):
    """Future next_run not due."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 86400},  # daily
    )
    # Default next_run is +24h
    assert jobs_module.get_due_jobs() == []


def test_get_due_jobs_returns_past(jobs_module):
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    jobs_module.update_job(j["id"], {"next_run_at": past})
    due = jobs_module.get_due_jobs()
    assert len(due) == 1
    assert due[0]["id"] == j["id"]


def test_get_due_jobs_handles_invalid_next_run(jobs_module):
    """Garbage in next_run_at doesn't crash get_due_jobs."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    jobs_module.update_job(j["id"], {"next_run_at": "not a timestamp"})
    # Should not raise; just skip the job
    due = jobs_module.get_due_jobs()
    assert all(d["id"] != j["id"] for d in due)


# ─── advance_next_run ──────────────────────────────────────────────────


def test_advance_next_run_interval_advances(jobs_module):
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 600},
    )
    original = j["next_run_at"]
    assert jobs_module.advance_next_run(j["id"]) is True
    refreshed = jobs_module.get_job(j["id"])
    assert refreshed["next_run_at"] != original
    assert refreshed["last_run_at"] is not None


def test_advance_next_run_once_auto_disables(jobs_module):
    """One-shot jobs auto-disable after advance — they don't re-fire."""
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    j = jobs_module.create_job(
        name="oneshot", schedule={"kind": "once", "run_at": future},
    )
    assert jobs_module.advance_next_run(j["id"]) is True
    refreshed = jobs_module.get_job(j["id"])
    assert refreshed["enabled"] is False
    assert refreshed["next_run_at"] is None


def test_advance_next_run_missing_returns_false(jobs_module):
    assert jobs_module.advance_next_run("nonexistent") is False


def test_mark_job_run_records_outcome(jobs_module):
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    jobs_module.mark_job_run(j["id"], success=True)
    assert jobs_module.get_job(j["id"])["last_run_success"] is True

    jobs_module.mark_job_run(j["id"], success=False, error="boom")
    refreshed = jobs_module.get_job(j["id"])
    assert refreshed["last_run_success"] is False
    assert refreshed["last_error"] == "boom"


# ─── Storage atomicity ────────────────────────────────────────────────


def test_save_jobs_atomic(jobs_module, monkeypatch):
    """Save uses tempfile + atomic rename so a crash mid-write
    doesn't corrupt jobs.json."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "interval", "interval_s": 60},
    )
    # Capture jobs.json content before
    before_content = jobs_module.JOBS_FILE.read_text()
    assert "t" in before_content

    # Inject a save that crashes — should not corrupt the file
    real_replace = os.replace
    def crash_on_replace(*a, **kw):
        raise RuntimeError("simulated crash mid-rename")
    monkeypatch.setattr(os, "replace", crash_on_replace)

    with pytest.raises(RuntimeError):
        jobs_module.save_jobs([])  # would zero out the file if not atomic

    # Original file should be intact
    after_content = jobs_module.JOBS_FILE.read_text()
    assert after_content == before_content
    monkeypatch.setattr(os, "replace", real_replace)


def test_load_jobs_handles_missing_file(jobs_module):
    """First-time load creates empty jobs.json."""
    jobs_module.JOBS_FILE.unlink(missing_ok=True)
    assert jobs_module.load_jobs() == []
    assert jobs_module.JOBS_FILE.exists()
