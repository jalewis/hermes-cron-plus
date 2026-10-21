"""Contracts OKEngine relies on, read from the shipped source.

runner.py runs as a subprocess script and cli.py formats for a terminal, so these pin the exact
wiring a consumer depends on: a rename here would otherwise surface only as a silent behavior
change in a deployment.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _src(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def test_runner_publishes_the_run_deadline_to_the_lane():
    runner = _src("runner.py")
    for var in ("OKENGINE_RUN_DEADLINE_EPOCH", "OKENGINE_RUN_TIMEOUT_SECONDS"):
        assert var in runner, f"runner.py no longer publishes {var}"


def test_runner_enforces_the_slug_guard_after_failed_and_successful_runs():
    runner = _src("runner.py")
    assert "if slug_snapshot is not None:" in runner
    assert "if success and slug_snapshot is not None:" not in runner


def test_runner_hands_the_slug_guard_its_run_records_and_output():
    runner = _src("runner.py")
    assert 'runs_root=jobs_mod.CRON_PLUS_HOME / "runs"' in runner
    assert "response=response" in runner


def test_manual_run_output_is_null_safe():
    cli = _src("cli.py")
    assert 'j.get("next_run_at")' in cli
    assert "pending scheduler reconciliation" in cli


def test_the_job_store_locks_through_the_sidecar_for_load_save_and_modify():
    jobs = _src("jobs.py")
    assert 'JOBS_LOCK_FILE = CRON_PLUS_HOME / "jobs.json.lock"' in jobs
    assert jobs.count("with _jobs_lock(fcntl.LOCK_") == 3, "load, save and locked_modify all lock it"
