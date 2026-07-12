"""okengine#197 regressions: root writes must not poison store ownership, and an unreadable
store must scream (sentinel), never silently idle.

A root `docker exec` rewriting jobs.json re-owned the new inode root:root; the uid-service
scheduler then couldn't read it and every lane silently stopped (a 44-minute live all-lane
outage, discovered only by manual inspection — the ticker kept touching .tick.lock, so it
looked alive)."""
from __future__ import annotations

import importlib
import json
import os
import sys


def _mods(temp_hermes_home):
    import jobs
    import scheduler
    return jobs, scheduler


def test_root_write_chowns_back_to_service_owner(temp_hermes_home, monkeypatch):
    """Simulated-root writer + HERMES_UID env -> the store is chowned to the service uid after
    every atomic write. (Real chown needs root; we capture the call.)"""
    jobs, _ = _mods(temp_hermes_home)
    calls = []
    monkeypatch.setattr(jobs.os, "geteuid", lambda: 0)
    monkeypatch.setattr(jobs.os, "chown", lambda path, uid, gid: calls.append((str(path), uid, gid)))
    monkeypatch.setenv("HERMES_UID", "1003")
    monkeypatch.setenv("HERMES_GID", "1003")
    jobs.save_jobs([{"id": "abc123", "name": "j"}])
    assert calls and calls[-1][1:] == (1003, 1003), calls
    assert calls[-1][0].endswith("jobs.json")


def test_nonroot_write_never_chowns(temp_hermes_home, monkeypatch):
    jobs, _ = _mods(temp_hermes_home)
    calls = []
    monkeypatch.setattr(jobs.os, "chown", lambda *a: calls.append(a))
    jobs.save_jobs([{"id": "abc123", "name": "j"}])   # we run as a normal uid
    assert calls == []


def test_service_owner_ignores_root_references(temp_hermes_home, monkeypatch):
    """A root-owned prior file / store dir is the POISON — it must never be chosen as the target
    owner (that would re-chown to root and perpetuate the outage)."""
    jobs, _ = _mods(temp_hermes_home)
    monkeypatch.delenv("HERMES_UID", raising=False)
    assert jobs._service_owner(prior=(0, 0)) != (0, 0)


def test_unreadable_store_drops_sentinel_and_recovery_clears_it(temp_hermes_home):
    jobs, _ = _mods(temp_hermes_home)
    jobs.save_jobs([{"id": "abc123", "name": "j"}])
    # poison: make the store unreadable to us (mode 000 stands in for root-owned)
    os.chmod(jobs.JOBS_FILE, 0)
    try:
        assert jobs.load_jobs() == []
        assert jobs.STALLED_SENTINEL.is_file(), "stall must leave a machine-readable sentinel"
        rec = json.loads(jobs.STALLED_SENTINEL.read_text())
        assert rec.get("error") and rec.get("at")
    finally:
        os.chmod(jobs.JOBS_FILE, 0o644)
    # recovery: a successful load clears the alarm
    assert jobs.load_jobs() == [{"id": "abc123", "name": "j"}]
    assert not jobs.STALLED_SENTINEL.exists()
