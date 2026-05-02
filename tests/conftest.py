"""Shared pytest fixtures for cron-plus tests."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Make the plugin's flat-layout modules importable. Tests live at
# tests/, plugin source lives at the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))


@pytest.fixture
def temp_hermes_home(tmp_path, monkeypatch):
    """Each test gets its own ~/.hermes equivalent so we never touch
    the real one. Returns the temp path. Re-imports jobs.py and
    scheduler.py with the new HERMES_HOME so they pick up the override."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    # Force re-import so module-level Path constants pick up the new env
    for mod_name in ("jobs", "scheduler", "runner", "cli", "migrate"):
        if mod_name in sys.modules:
            del sys.modules[mod_name]

    yield home

    # Cleanup module cache so next test gets fresh modules
    for mod_name in ("jobs", "scheduler", "runner", "cli", "migrate"):
        if mod_name in sys.modules:
            del sys.modules[mod_name]


@pytest.fixture
def jobs_module(temp_hermes_home):
    """Import jobs.py with isolated HERMES_HOME."""
    import jobs as jobs_mod  # noqa: PLC0415
    return jobs_mod


@pytest.fixture
def scheduler_module(temp_hermes_home):
    """Import scheduler.py with isolated HERMES_HOME."""
    import scheduler as scheduler_mod  # noqa: PLC0415
    return scheduler_mod
