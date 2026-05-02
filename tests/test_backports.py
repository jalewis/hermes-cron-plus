"""Regression tests for v0.1.1 upstream-Hermes backports.

Originally three backports were planned; after consolidating delivery
through upstream `_deliver_result`, only ONE remains as cron-plus code:

- compute_next_run uses last_run_at as croniter base   (e0c016742)

The other two were superseded by the HIGH 2 review fix that delegates
delivery entirely to upstream Hermes' `cron.scheduler._deliver_result`:

- normalize list-form deliver values (398945e7b)  → upstream handles
- parse telegram:<chat>:<thread> per-job targets (6ce796b49) → upstream handles

Removing the locally-reimplemented helpers eliminated drift risk and
~80 LOC of duplicated logic.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

# Every test in this file exercises cron-kind schedules and needs croniter.
# Skip the whole module rather than fail when the optional dep is absent;
# `pip install -e ".[dev]"` (per pyproject.toml) installs it.
pytest.importorskip(
    "croniter",
    reason='cron schedule tests require croniter — install via pip install -e ".[dev]"',
)


# ─── Backport 1: compute_next_run uses last_run_at as base ─────────────


def test_compute_next_run_uses_last_run_at_for_cron(jobs_module):
    """For cron schedules, last_run_at takes precedence over anchor —
    prevents schedule drift across restarts."""
    last_run = datetime(2026, 5, 2, 10, 5, tzinfo=timezone.utc).isoformat()
    anchor_now = datetime(2026, 5, 2, 15, 0, tzinfo=timezone.utc)

    nxt = jobs_module.compute_next_run(
        {"kind": "cron", "expr": "0 10 * * *"},
        anchor=anchor_now,
        last_run_at=last_run,
    )
    parsed = datetime.fromisoformat(nxt)
    assert parsed == datetime(2026, 5, 3, 10, 0, tzinfo=timezone.utc)


def test_compute_next_run_falls_back_to_anchor_without_last_run(jobs_module):
    """When last_run_at is missing, croniter base falls back to anchor."""
    anchor = datetime(2026, 5, 2, 15, 0, tzinfo=timezone.utc)
    nxt = jobs_module.compute_next_run(
        {"kind": "cron", "expr": "0 10 * * *"}, anchor=anchor,
    )
    assert datetime.fromisoformat(nxt) == datetime(2026, 5, 3, 10, 0, tzinfo=timezone.utc)


def test_compute_next_run_invalid_last_run_at_falls_back(jobs_module):
    """Garbage last_run_at doesn't crash; falls back to anchor."""
    anchor = datetime(2026, 5, 2, 15, 0, tzinfo=timezone.utc)
    nxt = jobs_module.compute_next_run(
        {"kind": "cron", "expr": "0 10 * * *"},
        anchor=anchor,
        last_run_at="not a real timestamp",
    )
    assert datetime.fromisoformat(nxt) > anchor


def test_advance_next_run_threads_last_run_at_through(jobs_module):
    """advance_next_run should pass last_run_at to compute_next_run for
    cron schedules. End-to-end check."""
    j = jobs_module.create_job(
        name="t", schedule={"kind": "cron", "expr": "0 10 * * *"},
    )
    jobs_module.advance_next_run(j["id"])
    refreshed = jobs_module.get_job(j["id"])
    assert refreshed["last_run_at"] is not None
    assert refreshed["next_run_at"] is not None
    next_run = datetime.fromisoformat(refreshed["next_run_at"])
    assert next_run.hour == 10 and next_run.minute == 0
