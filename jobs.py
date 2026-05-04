"""cron-plus job storage — CRUD + due-job filtering + schedule eval.

Stores jobs at ~/.hermes/cron-plus/jobs.json. Disjoint from Hermes'
built-in cron at ~/.hermes/cron/jobs.json so both can coexist.

Schedule kinds:
- cron:     {"kind": "cron", "expr": "0 10 * * 0"}  — 5-field cron, UTC
- interval: {"kind": "interval", "interval_s": 600} — every N seconds
- once:     {"kind": "once", "run_at": "2026-05-03T18:00:00+00:00"}
            — fires once at the given UTC timestamp, then auto-disables
"""
from __future__ import annotations

import fcntl
import json
import logging
import os
import secrets
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

HERMES_HOME = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
CRON_PLUS_HOME = HERMES_HOME / "cron-plus"
JOBS_FILE = CRON_PLUS_HOME / "jobs.json"

# croniter is part of the Hermes venv (the built-in scheduler uses it).
try:
    from croniter import croniter
    _HAS_CRONITER = True
except ImportError:
    croniter = None  # type: ignore[assignment]
    _HAS_CRONITER = False


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_jobs_file() -> None:
    CRON_PLUS_HOME.mkdir(parents=True, exist_ok=True)
    if not JOBS_FILE.exists():
        JOBS_FILE.write_text(json.dumps({"jobs": []}, indent=2) + "\n")


def _new_id() -> str:
    """Match Hermes' built-in cron 12-char hex IDs."""
    return secrets.token_hex(6)


def load_jobs() -> list[dict]:
    _ensure_jobs_file()
    try:
        with open(JOBS_FILE, "r") as fd:
            fcntl.flock(fd, fcntl.LOCK_SH)
            try:
                data = json.load(fd)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
    except (OSError, json.JSONDecodeError) as e:
        logger.error("cron-plus: failed to load %s: %s", JOBS_FILE, e)
        return []
    return list(data.get("jobs", []))


def save_jobs(jobs: list[dict]) -> None:
    """Atomic write under exclusive lock. For RMW workflows use
    ``locked_modify()`` instead so the load+modify+save runs under a
    single lock — otherwise two concurrent updaters can lose writes."""
    _ensure_jobs_file()
    fd = os.open(str(JOBS_FILE), os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        _write_jobs_atomic(jobs)
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _write_jobs_atomic(jobs: list[dict]) -> None:
    """Write {jobs} to JOBS_FILE atomically (tempfile + rename).

    Caller is responsible for holding the exclusive lock on JOBS_FILE.
    """
    tmp = tempfile.NamedTemporaryFile(
        mode="w", dir=str(CRON_PLUS_HOME),
        prefix=".jobs.", suffix=".tmp", delete=False,
    )
    try:
        json.dump({"jobs": jobs}, tmp, indent=2)
        tmp.write("\n")
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp.close()
        os.replace(tmp.name, str(JOBS_FILE))
    except Exception:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        raise


def locked_modify(fn):
    """Run ``fn(jobs)`` under a single exclusive lock on JOBS_FILE.

    ``fn`` receives the current job list (mutable) and returns either:
        - the (possibly mutated) list to persist, OR
        - a tuple (jobs_to_persist, return_value) — common for CRUD
          mutators that need to surface the updated job to the caller

    Without this, mutators that load → modify → save under separate locks
    lose writes when two updaters race (load A, load B, modify both,
    save A, save B — A's changes lost).
    """
    _ensure_jobs_file()
    fd = os.open(str(JOBS_FILE), os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        # Re-read inside the lock so we see the latest state
        try:
            with open(JOBS_FILE, "r") as f:
                data = json.load(f)
            jobs = list(data.get("jobs", []))
        except (OSError, json.JSONDecodeError) as e:
            logger.error("cron-plus locked_modify: failed to read %s: %s", JOBS_FILE, e)
            jobs = []

        result = fn(jobs)
        if isinstance(result, tuple):
            jobs_to_save, return_value = result
        else:
            jobs_to_save, return_value = result, None

        if jobs_to_save is not None:
            _write_jobs_atomic(jobs_to_save)
        return return_value
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


# ─── Schedule eval ────────────────────────────────────────────────────


def compute_next_run(
    schedule: dict,
    *,
    anchor: Optional[datetime] = None,
    last_run_at: Optional[str] = None,
) -> Optional[str]:
    """Compute the next firing time for a schedule, returning ISO 8601.

    `anchor` is the timestamp to compute "next after" from. Defaults to now.
    `last_run_at`, when provided, is used as the croniter base for cron-kind
    schedules instead of `anchor`. This prevents schedule drift across
    gateway restarts: a daily-at-10:00 job restarting at 03:00 would
    otherwise compute the next run from 03:00 + cron-pattern, shifting
    the canonical 10:00 fire window. Backported from upstream Hermes
    fix `e0c016742` (`fix(cron): use last_run_at as croniter base`).

    Returns None if no future run exists (e.g., one-shot in the past).
    """
    anchor = anchor or _utc_now()
    kind = schedule.get("kind")

    if kind == "cron":
        expr = schedule.get("expr")
        if not expr:
            return None
        if not _HAS_CRONITER:
            logger.error("cron-plus: croniter not installed; cron schedules unusable")
            return None
        # Use last_run_at as the croniter base when available — see docstring.
        cron_base = anchor
        if last_run_at:
            try:
                cron_base = datetime.fromisoformat(last_run_at)
                if cron_base.tzinfo is None:
                    cron_base = cron_base.replace(tzinfo=timezone.utc)
            except ValueError:
                pass  # fall back to anchor
        try:
            it = croniter(expr, cron_base)
            return it.get_next(datetime).astimezone(timezone.utc).isoformat()
        except Exception as e:
            logger.error("cron-plus: invalid cron expr %r: %s", expr, e)
            return None

    if kind == "interval":
        try:
            interval_s = int(schedule.get("interval_s", 0))
        except (ValueError, TypeError):
            return None
        if interval_s <= 0:
            return None
        return (anchor + timedelta(seconds=interval_s)).isoformat()

    if kind == "once":
        run_at = schedule.get("run_at")
        if not run_at:
            return None
        try:
            ts = datetime.fromisoformat(run_at)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts <= anchor:
                return None
            return ts.isoformat()
        except ValueError:
            return None

    return None


def _is_one_shot(job: dict) -> bool:
    return (job.get("schedule") or {}).get("kind") == "once"


# ─── CRUD ──────────────────────────────────────────────────────────────


def get_job(job_id: str) -> Optional[dict]:
    for j in load_jobs():
        if j.get("id") == job_id:
            return j
    return None


def list_jobs(*, include_disabled: bool = True) -> list[dict]:
    out = load_jobs()
    if not include_disabled:
        out = [j for j in out if j.get("enabled", True)]
    return out


def create_job(
    *,
    name: str,
    schedule: dict,
    workdir: Optional[str] = None,
    prompt: Optional[str] = None,
    script: Optional[str] = None,
    deliver: str = "local",
    enabled: bool = True,
) -> dict:
    """Create a new job. Returns the created dict."""
    job_id = _new_id()
    next_run = compute_next_run(schedule)
    job = {
        "id": job_id,
        "name": name,
        "enabled": enabled,
        "schedule": schedule,
        "workdir": workdir,
        "prompt": prompt,
        "script": script,
        "deliver": deliver,
        "next_run_at": next_run,
        "last_run_at": None,
        "created_at": _utc_now().isoformat(),
    }
    def _do(existing: list[dict]):
        existing.append(job)
        return existing, None
    locked_modify(_do)
    logger.info("cron-plus: created job %s (id=%s, next=%s)", name, job_id, next_run)
    return job


def update_job(job_id: str, updates: dict) -> Optional[dict]:
    """Apply a partial update to a job. Returns the updated dict or None.

    Runs under a single exclusive lock so concurrent updates can't lose
    changes.
    """
    def _do(jobs: list[dict]):
        for job in jobs:
            if job.get("id") != job_id:
                continue
            for k, v in updates.items():
                job[k] = v
            if "schedule" in updates and "next_run_at" not in updates:
                job["next_run_at"] = compute_next_run(job["schedule"])
            return jobs, job
        return None, None  # not found — don't write
    return locked_modify(_do)


def remove_job(job_id: str) -> bool:
    """Remove a job. """
    def _do(jobs: list[dict]):
        new = [j for j in jobs if j.get("id") != job_id]
        if len(new) == len(jobs):
            return None, False  # nothing to write
        return new, True
    result = locked_modify(_do)
    return bool(result)


def pause_job(job_id: str) -> Optional[dict]:
    return update_job(job_id, {"enabled": False, "paused_at": _utc_now().isoformat()})


def resume_job(job_id: str) -> Optional[dict]:
    job = get_job(job_id)
    if not job:
        return None
    updates = {"enabled": True, "paused_at": None}
    # Recompute next_run if it's stale or null
    nra = job.get("next_run_at")
    needs_recompute = not nra
    if nra:
        try:
            ts = datetime.fromisoformat(nra)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts < _utc_now():
                needs_recompute = True
        except ValueError:
            needs_recompute = True
    if needs_recompute:
        updates["next_run_at"] = compute_next_run(job.get("schedule") or {})
    return update_job(job_id, updates)


def trigger_job(job_id: str) -> Optional[dict]:
    """Schedule a job to run on the very next tick.

    Sets next_run_at = now - 1s (NOT now + 0) to avoid the
    trigger-vs-tick race documented in UPSTREAM_HERMES_CRON_BUG_REPORT.md
    The 1s offset guarantees the next tick that samples `now`
    sees the job as due, even if the tick samples a few ms before this
    trigger lands.

    Idempotency: refuses to re-trigger if the job's most recent fire
    is still active (PID-tracked by scheduler.py). Logs and returns
    the existing job state.
    """
    job = get_job(job_id)
    if not job:
        return None

    # Idempotency check. Import locally to avoid module-load circularity.
    # Handle both package context (hermes_plugins.cron_plus) and flat
    # module context (when invoked via sys.path.insert in runner.py / tests).
    try:
        from . import scheduler  # type: ignore[import]
    except ImportError:
        import scheduler  # type: ignore[import]
    if scheduler._job_is_running(job_id):
        logger.info(
            "cron-plus: trigger_job(%s) ignored — previous run still active",
            job_id,
        )
        return job

    triggered_at = _utc_now() - timedelta(seconds=1)
    return update_job(job_id, {
        "enabled": True,
        "next_run_at": triggered_at.isoformat(),
    })


# ─── Due-job filtering ─────────────────────────────────────────────────


def get_due_jobs() -> list[dict]:
    """Return jobs whose next_run_at is in the past and that are enabled.

    Read-only — does NOT mutate job state. Use claim_due_jobs() instead
    if you intend to spawn the returned jobs (it advances next_run_at
    atomically as part of the claim).

    Note: jobs with null `next_run_at` are skipped here (they're not
    "in the past" — they're "no decision yet"). claim_due_jobs() heals
    null entries as part of its lock-protected claim phase; this
    function deliberately does not, to preserve its read-only contract.
    Callers using this for inspection (e.g. CLI list output) will see
    an empty result for newly-deployed jobs until the first tick claim
    heals them — which is informational-only, not load-bearing.
    """
    now = _utc_now()
    due: list[dict] = []
    for job in load_jobs():
        if not job.get("enabled", True):
            continue
        nra = job.get("next_run_at")
        if not nra:
            continue
        try:
            next_run = datetime.fromisoformat(nra)
        except ValueError:
            logger.warning("cron-plus: job %s has invalid next_run_at: %r", job.get("id"), nra)
            continue
        if next_run.tzinfo is None:
            next_run = next_run.replace(tzinfo=timezone.utc)
        if next_run <= now:
            due.append(job)
    return due


def claim_due_jobs(is_running_check=None) -> list[dict]:
    """Atomic claim: under a single exclusive lock, identify due jobs,
    skip any that have been removed/paused/are still running since the
    last snapshot, advance their next_run_at, and return the snapshot.

    The caller is then SAFE to spawn each returned job — there is no
    longer a race window where a removal/pause could land between the
    "is it due?" check and the spawn.

    `is_running_check(job_id) -> bool` is an optional callback used to
    skip jobs whose previous subprocess is still alive (idempotency).

    Self-heals null `next_run_at` on enabled jobs as part of the same
    lock acquisition (see comment inside `_do`). Without that heal, a
    sanitized jobs.json (typical disaster-recovery deploy artifact —
    runtime fields stripped) leaves every enabled job with
    next_run_at=null and the original due-check below skipped them
    forever.

    HIGH 1 fix.
    """
    now = _utc_now()
    claimed: list[dict] = []

    def _do(jobs: list[dict]):
        nonlocal claimed
        # ── Phase 1: self-heal null next_run_at on enabled jobs ──────
        # A freshly-deployed jobs.json from a sanitized source-of-truth
        # has no runtime state — every enabled job arrives with
        # next_run_at=null. The Phase-2 due-check below short-circuits
        # those entries (`if not nra: continue`), so without this heal
        # they sit silent forever until something pokes next_run_at to
        # a real timestamp. This bug was the same as gotcha #11 in the
        # legacy Hermes built-in cron and required external workaround
        # scripts (e.g. seed-cron-plus.py) to recover from each deploy.
        #
        # The heal is cheap: compute_next_run() is pure-function over
        # the schedule + now. We do it under the same exclusive lock as
        # the claim itself so a concurrent tick can't see a half-healed
        # state. The freshly-computed timestamp is almost always in the
        # future (the *next* scheduled tick), so the just-healed job
        # falls through Phase 2 unclaimed this tick — but is correctly
        # picked up on the tick at-or-after the new next_run_at.
        for job in jobs:
            if not job.get("enabled", True):
                continue
            if job.get("next_run_at"):
                continue
            schedule = job.get("schedule") or {}
            healed = compute_next_run(schedule)
            if healed:
                job["next_run_at"] = healed
                logger.info(
                    "cron-plus: self-healed null next_run_at for job "
                    "'%s' (id=%s) → %s",
                    job.get("name", job["id"]), job["id"], healed,
                )

        # ── Phase 2: claim due jobs ──────────────────────────────────
        for job in jobs:
            if not job.get("enabled", True):
                continue
            nra = job.get("next_run_at")
            if not nra:
                continue
            try:
                next_run = datetime.fromisoformat(nra)
            except ValueError:
                logger.warning("cron-plus: job %s has invalid next_run_at: %r",
                               job.get("id"), nra)
                continue
            if next_run.tzinfo is None:
                next_run = next_run.replace(tzinfo=timezone.utc)
            if next_run > now:
                continue
            # Idempotency check (still alive subprocess?)
            if is_running_check is not None and is_running_check(job["id"]):
                logger.warning(
                    "cron-plus: job '%s' (id=%s) skipped — previous run still active",
                    job.get("name", job["id"]), job["id"],
                )
                continue
            # Claim: advance next_run_at INSIDE the lock so a concurrent
            # tick can't claim the same job. This is the same logic as
            # advance_next_run but inlined to stay under one lock.
            if _is_one_shot(job):
                job["next_run_at"] = None
                job["enabled"] = False
                job["last_run_at"] = now.isoformat()
            else:
                schedule = job.get("schedule") or {}
                now_iso = now.isoformat()
                job["next_run_at"] = compute_next_run(schedule, last_run_at=now_iso)
                job["last_run_at"] = now_iso
            # Snapshot a deep-ish copy so the caller's mutations don't
            # affect the persisted dict
            import copy
            claimed.append(copy.deepcopy(job))
        return jobs, None

    locked_modify(_do)
    return claimed


def advance_next_run(job_id: str) -> bool:
    """Advance next_run_at to the next firing per the schedule.

    For one-shot jobs, sets next_run_at=null and disables the job
    (auto-disable after firing). Returns True if state changed.

    
    """
    def _do(jobs: list[dict]):
        for job in jobs:
            if job.get("id") != job_id:
                continue
            if _is_one_shot(job):
                job["next_run_at"] = None
                job["enabled"] = False
                job["last_run_at"] = _utc_now().isoformat()
            else:
                schedule = job.get("schedule") or {}
                # Pass the last_run_at we are about to set so cron schedules
                # anchor on actual execution time, not on now() — avoids
                # drift after restarts. (Backport: Hermes upstream e0c016742.)
                now_iso = _utc_now().isoformat()
                new_next = compute_next_run(schedule, last_run_at=now_iso)
                job["next_run_at"] = new_next
                job["last_run_at"] = now_iso
            return jobs, True
        return None, False
    return bool(locked_modify(_do))


def mark_job_run(
    job_id: str,
    *,
    success: bool,
    error: Optional[str] = None,
    delivery_error: Optional[str] = None,
) -> None:
    """Record the outcome of the most recent run.

    `success` reflects whether the AGENT itself ran successfully. A
    delivery failure leaves success=True but populates delivery_error
    so the CLI can render the distinction. 
    """
    update_job(job_id, {
        "last_run_success": success,
        "last_error": error,
        "last_delivery_error": delivery_error,
    })
