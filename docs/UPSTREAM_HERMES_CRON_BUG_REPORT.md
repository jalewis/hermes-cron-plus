# Hermes-Agent Cron Scheduler — 4 Related Issues + Architectural Concern

**Status**: Drafted, NOT FILED. For review before deciding to submit upstream at NousResearch/hermes-agent.

**Author context**: Operator running 17 cron jobs (5-25 min runtimes each) in a fork of hermes-agent. All 4 issues independently observed in production over a single 90-minute debugging session. Diagnosed against `cron/scheduler.py` and `cron/jobs.py` source.

---

## TL;DR

The cron scheduler has 4 separable defects that compound into "triggered jobs silently fail to fire under contention":

| # | Issue | Severity | Fix size |
|---|---|---|---|
| 1 | Workdir-bearing jobs serialize within a tick because of `os.environ` mutation; tick holds file lock for the entire serial duration | High (architectural) | ~250 LOC |
| 2 | `cron run` sets `next_run_at = now`, but the tick that fires immediately after may have just checked due-jobs at `now - 1ms` — triggered job is missed by milliseconds | Medium | 1 LOC |
| 3 | `verbose=False` hardcoded in `gateway/run.py` makes scheduler observability invisible without a fork | Low (observability) | ~5 LOC |
| 4 | `cron run` doesn't dedupe — repeated triggers on a pending or in-flight job pile up and fire as duplicate sessions | Medium | ~10 LOC |

**Compounded failure mode**: triggered job sits unfired for 60+ minutes, then fires multiple times back-to-back as duplicates (observed: 2 concurrent agent sessions for the same cron job ID).

---

## Environment

- **Hermes-Agent**: ~v0.11 era fork
- **Deployment**: Docker, single gateway container; `~/.hermes` mounted into the container; per-job workdir set
- **Python**: 3.11 (per Hermes venv)
- **Scheduler config**: `interval=60s` (default), `cron.script_timeout_seconds=240` (custom; default 120), `HERMES_CRON_MAX_PARALLEL` unset (unbounded)
- **Job count**: 17 total, 11 with workdir, runtimes 5-25 min average, ~1 noop wake-gate-skip job, ~6 frequent wake-gate skips
- **Key files referenced**: `cron/scheduler.py`, `cron/jobs.py`, `gateway/run.py`

---

## Bug 1 — Workdir-bearing jobs serialize within a tick due to `os.environ` mutation

### Root cause

`cron/scheduler.py` lines 1313-1314:

```python
# Partition due jobs: those with a per-job workdir mutate
# os.environ["TERMINAL_CWD"] inside run_job, which is process-global —
# so they MUST run sequentially to avoid corrupting each other.  Jobs
# without a workdir leave env untouched and stay parallel-safe.
workdir_jobs = [j for j in due_jobs if (j.get("workdir") or "").strip()]
parallel_jobs = [j for j in due_jobs if not (j.get("workdir") or "").strip()]
```

The serialization is enforced because `_process_job` (and the agent code it calls into) mutates `os.environ["TERMINAL_CWD"]` to bind the agent's terminal/file/code_exec tools to the job's workdir. Since `os.environ` is process-global, two workdir jobs running concurrently would corrupt each other's CWD.

The sequential workdir loop runs to completion *before* the file lock at the top of `tick()` is released:

```python
# cron/scheduler.py:1218-1223
lock_fd = open(_LOCK_FILE, "w")
if fcntl:
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
elif msvcrt:
    msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
```

So the lock is held for `sum(runtime of all due workdir jobs in this tick)`.

### Repro

Minimal:

```bash
# Create two long-running workdir jobs
hermes cron create '* * * * *' \
  --name long-job-a --workdir /tmp/a \
  'sleep 600 && echo done-a'

hermes cron create '* * * * *' \
  --name long-job-b --workdir /tmp/b \
  'sleep 600 && echo done-b'

# Wait one tick. Both are due.
# Observe: job-b doesn't start until job-a completes 10 min later.
# Observe: file lock held the entire 20 min combined duration.
# Observe: any third triggered job during this window is queued and
#          will not fire until both finish.
```

Real-world repro (operator's): with 11 workdir jobs and 60s tick interval, a tick that picks up 4 due jobs (each 5-15 min) holds the lock for 30-60 min straight. During that window:

- `cron run <id>` triggers don't fire (file lock blocks new ticks)
- New scheduled jobs miss their natural firing windows and may be marked "stale" by `get_due_jobs()` (see `cron/jobs.py:824`) and fast-forwarded past their intended run

### Impact

- Triggered jobs starve indefinitely under contention
- Compounding: as the deployment's job portfolio grows, more workdir jobs → longer serial tick durations → worse starvation
- Effective throughput ceiling: ~24h / sum(per-job runtime) jobs per day
- Operator currently running 17 jobs averaging 8 min each = ~136 min of serial work per "round" if all are due simultaneously, vs the 60s tick interval — chronic queue backup

### Proposed fix

**Subprocess-per-job for workdir jobs** (~250 LOC):

```python
# cron/scheduler.py
def _spawn_workdir_job_subprocess(job: dict) -> subprocess.Popen:
    """Spawn a workdir job in its own subprocess. Each subprocess has
    its own os.environ, so TERMINAL_CWD pollution is contained.
    Fire-and-forget — caller does not wait."""
    log_path = HERMES_HOME / "logs" / "cron" / f"{job['id']}-{ts}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        [sys.executable, "-m", "cron.run_job_subprocess", "--job-id", job["id"]],
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

# In tick():
for job in workdir_jobs:
    advance_next_run(job["id"])  # at-most-once preserved
    _spawn_workdir_job_subprocess(job)
# Fall through immediately — tick returns in <1s instead of minutes
```

New module `cron/run_job_subprocess.py`:

```python
"""Subprocess entry point for a single cron job.

Loads Hermes runtime, fetches the job by ID, runs the wake-gate,
invokes the agent if needed, persists session to state.db, and
delivers via Telegram/etc. Exits when done.

Each subprocess has its own os.environ, so TERMINAL_CWD mutation
is contained and parallel execution is safe.
"""
import argparse
import sys
from cron.scheduler import run_job
from cron.jobs import get_job, mark_job_run

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    job = get_job(args.job_id)
    if not job:
        sys.exit(2)
    success, output, response, err = run_job(job)
    mark_job_run(job["id"], success, err)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
```

### Trade-offs

- **Memory**: ~100MB Hermes runtime per concurrent subprocess. At 17 max-concurrent: ~1.7GB peak. Acceptable for typical deployments.
- **Startup latency**: 2-3s per subprocess (Python import). Negligible vs 5-25 min job runtimes.
- **Telegram delivery**: subprocess can deliver directly using bot token from `~/.hermes/.env` (no need to share the gateway's polling adapter)
- **Live adapter integration**: subprocess sends one-shot messages, doesn't need to share polling state
- **Observability**: each subprocess gets its own log file, easier to debug than interleaved in-process logs
- **Process accounting**: can `ps` and see per-job processes; can `kill` an individual stuck job

### Test case

```python
# tests/cron/test_workdir_parallelism.py
def test_workdir_jobs_run_concurrently():
    """Two workdir jobs with different workdirs should run in parallel,
    not serialize on os.environ['TERMINAL_CWD']."""
    job_a = create_job(
        name="parallel-a",
        workdir="/tmp/wd-a",
        prompt="run pwd; sleep 5; run pwd",
    )
    job_b = create_job(
        name="parallel-b",
        workdir="/tmp/wd-b",
        prompt="run pwd; sleep 5; run pwd",
    )
    trigger_job(job_a["id"])
    trigger_job(job_b["id"])

    start = time.time()
    tick(verbose=True)
    elapsed = time.time() - start

    # If serial: ~10s. If parallel: ~5s.
    assert elapsed < 7, f"Jobs serialized (elapsed {elapsed}s, expected <7s)"

    # Each job's pwd output should reflect its own workdir, not the other's
    a_session = latest_session(job_a["id"])
    b_session = latest_session(job_b["id"])
    assert "/tmp/wd-a" in a_session.tool_outputs[0]
    assert "/tmp/wd-b" in b_session.tool_outputs[0]
```

---

## Bug 2 — `cron run` trigger-vs-tick race

### Root cause

`cron/jobs.py:641-655`:

```python
def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Schedule a job to run on the next scheduler tick."""
    job = get_job(job_id)
    if not job:
        return None
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": _hermes_now().isoformat(),
        },
    )
```

`next_run_at` is set to `_hermes_now()` at trigger time. But the scheduler tick that runs *immediately after* may have just sampled `now` 1-100 ms before the trigger, and its `get_due_jobs()` filter does `next_run_dt <= now` — so the triggered job is excluded by milliseconds.

Observed example (this debugging session):

```
17:33:49.633 — cron run sets next_run_at=17:33:49.633
17:33:40.841 — tick had already started (sampled now=17:33:40.841)
              get_due_jobs sees next_run=null (M&A wasn't triggered yet)
              → excluded
17:34:57.067 — next tick (60s later)
              get_due_jobs sees next_run=17:33:49.633 ≤ now=17:34:57.067
              → included → finally fires
```

In high-contention windows (long ticks), the gap between trigger and effective fire can be 60s + (in-flight tick duration) = 30-60+ minutes.

### Repro

```bash
# Time the trigger to land just after a tick samples
hermes cron create '*/1 * * * *' --name trace 'echo tick'

# Run tick manually right before triggering to observe race
python -c "from cron.scheduler import tick; tick(verbose=True)" &
sleep 0.05  # land trigger 50ms after tick started
hermes cron run <trace-id>

# Observe: trigger sets next_run, but the running tick already
# captured due_jobs and won't fire the triggered run. Triggered
# fire happens on the *next* tick (60s away).
```

### Impact

- Operator UX: `cron run` "succeeds" but the job doesn't actually run for up to 60s + in-flight-tick-duration
- Combined with Bug 1: triggered jobs can be delayed by 30-60 minutes
- Confusing diagnostics: `hermes cron list` shows `Next run: <past timestamp>` indefinitely until the next idle tick

### Proposed fix

Set `next_run_at = now - 1s` so the next tick that samples `now` is guaranteed to see it as due:

```python
# cron/jobs.py:trigger_job
def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    job = get_job(job_id)
    if not job:
        return None
    # Subtract 1s so triggered jobs are guaranteed to be due
    # on the next tick, even if it samples `now` slightly before
    # this trigger lands.
    triggered_at = _hermes_now() - timedelta(seconds=1)
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": triggered_at.isoformat(),
        },
    )
```

Alternative (more invasive): **trigger executes inline** instead of queuing for the next tick:

```python
def trigger_job(job_id: str, *, run_inline: bool = True) -> ...:
    if run_inline:
        # Acquire the file lock, run this single job, release.
        # Same locking discipline as tick() but only for one job.
        ...
```

### Test case

```python
def test_trigger_fires_on_immediate_next_tick():
    """A job triggered just after a tick samples should still fire
    on the very next tick, not skip a cycle."""
    job = create_job(schedule="0 0 1 1 *", prompt="echo")  # natural: Jan 1

    # Tick samples `now` first
    sampled_now = _hermes_now()
    # Then trigger lands
    trigger_job(job["id"])
    # Verify next_run is BEFORE sampled_now (so tick will see it as due)
    refreshed = get_job(job["id"])
    next_run = datetime.fromisoformat(refreshed["next_run_at"])
    assert next_run < sampled_now, \
        f"Trigger set next_run={next_run} not before now={sampled_now}; will miss this tick"
```

---

## Bug 3 — Verbose tick logging hardcoded off; observability requires fork

### Root cause

`gateway/run.py:11288`:

```python
while not stop_event.is_set():
    try:
        cron_tick(verbose=False, adapters=adapters, loop=loop)
```

The scheduler's per-tick INFO logs (`tick(verbose=True)` emits "X jobs due", "Running N in parallel", etc.) are unreachable from the gateway because the call site hardcodes `verbose=False`.

The only way to enable verbose ticks is patching `gateway/run.py` and rebuilding the image — operators can't toggle this for diagnostics.

### Repro

```bash
# Operator hits a "triggered job not firing" issue.
# Tries to enable scheduler debug:
hermes config get logging.level  # → INFO
# But this only affects the agent.log handler level, NOT the
# scheduler's verbose tick logging. The latter is gated on
# tick(verbose=True), which is never called from the gateway.

# So operator has zero visibility into:
# - "Was the job in due_jobs this tick?"
# - "How many jobs were due?"
# - "Was the file lock contended?"
# - "Was the job fast-forwarded as stale?"
```

### Impact

- Operators debugging cron issues have no scheduler visibility without forking
- Adoption friction: enabling verbose requires rebuild + restart, which loses the original failing state

### Proposed fix

Add a config flag and/or env var:

```python
# gateway/run.py:11288
import os
_verbose = os.getenv("HERMES_CRON_VERBOSE", "").lower() in ("1", "true", "yes")
# Or read from config.yaml: cron.verbose_ticks
while not stop_event.is_set():
    try:
        cron_tick(verbose=_verbose, adapters=adapters, loop=loop)
```

And document in `~/.hermes/config.yaml.example`:

```yaml
cron:
  # Emit per-tick INFO logs ("X jobs due", "Running N in parallel").
  # Useful for debugging triggered-job starvation. Default: false.
  verbose_ticks: true
```

### Test case

```python
def test_verbose_ticks_env_var(caplog):
    """HERMES_CRON_VERBOSE=1 should enable per-tick INFO logs."""
    with mock.patch.dict(os.environ, {"HERMES_CRON_VERBOSE": "1"}):
        with caplog.at_level(logging.INFO, logger="cron.scheduler"):
            tick_via_gateway_path()  # the path gateway/run.py uses
    assert any("job(s) due" in rec.message for rec in caplog.records)
```

---

## Bug 4 — `cron run` doesn't dedupe; pile-up triggers spawn duplicate sessions

### Root cause

`cron/jobs.py:trigger_job` (above) overwrites `next_run_at` unconditionally. If the operator calls `cron run <id>` multiple times (because the previous trigger appeared not to fire — see Bugs 1 + 2), each call resets `next_run_at = now` again.

Once contention clears, multiple subsequent ticks each see the job as due, advance `next_run_at` to its natural schedule, fire a session, and (because the operator triggered N times) N sessions can fire back-to-back across N ticks.

Observed example (this session):

```
15:12:40 — first trigger (didn't fire — Bug 2)
15:50:30 — second trigger (didn't fire — Bug 1, blocked by long workdir job)
16:51:48 — third trigger (didn't fire — Bug 2 again)
17:33:49 — fourth trigger
17:33:47 — tick fires the M&A digest (probably from third or earlier trigger)
17:35:03 — next tick fires another M&A digest (from fourth trigger)
        → TWO concurrent agent sessions for the same job ID
```

State.db shows two concurrent sessions:

```
cron_<job-id>_<ts1> 17:33:48->RUNNING msgs=0
cron_<job-id>_<ts2> 17:35:04->RUNNING msgs=0
```

Both then deliver to whatever the job's `deliver:` target is (Telegram, file, etc.) → operator gets duplicate output. Worse: both write to state.db, both perform the job's side effects, both burn API spend.

### Impact

- Duplicate output deliveries (operator-visible)
- Duplicate state.db sessions (data quality)
- Wasted API spend (duplicate agent runs)
- Race conditions on any shared resource the job writes to (potential data corruption — neither concurrent run saw the other's state)

### Proposed fix

Add an idempotency guard at trigger time:

```python
# cron/jobs.py:trigger_job
def trigger_job(job_id: str) -> Optional[Dict[str, Any]]:
    job = get_job(job_id)
    if not job:
        return None

    # Idempotency: refuse re-trigger if already scheduled or running
    state = job.get("state", "idle")
    if state in ("scheduled", "running"):
        existing_next_run = job.get("next_run_at")
        logger.info(
            "trigger_job(%s) ignored — already %s (next_run=%s)",
            job_id, state, existing_next_run,
        )
        return job  # no-op, return existing state

    triggered_at = _hermes_now() - timedelta(seconds=1)  # also fixes Bug 2
    return update_job(
        job_id,
        {
            "enabled": True,
            "state": "scheduled",
            "paused_at": None,
            "paused_reason": None,
            "next_run_at": triggered_at.isoformat(),
        },
    )
```

Plus tick-time check: when picking up a job from `due_jobs`, refuse if a session for that job_id is already active in `state.db`:

```python
# cron/scheduler.py:tick (inside the workdir loop)
if has_active_session(job["id"]):
    logger.warning(
        "Job '%s' (ID: %s): skipping — previous session still running",
        job["name"], job["id"],
    )
    continue
```

### Test case

```python
def test_trigger_dedupes_when_already_scheduled():
    """Re-triggering a job that's already scheduled should be a no-op,
    not pile up extra runs."""
    job = create_job(schedule="0 0 1 1 *", prompt="echo")
    trigger_job(job["id"])
    next_run_first = get_job(job["id"])["next_run_at"]

    time.sleep(0.5)
    trigger_job(job["id"])  # second trigger before first fires
    next_run_second = get_job(job["id"])["next_run_at"]

    assert next_run_first == next_run_second, \
        "Re-trigger should not move next_run forward when job is already scheduled"

def test_concurrent_session_prevention():
    """Tick should refuse to start a job if a previous session is still running."""
    job = create_job(prompt="sleep 60; echo done", workdir="/tmp")
    trigger_job(job["id"])
    tick()
    assert latest_session(job["id"]).status == "running"

    trigger_job(job["id"])
    tick()
    sessions = sessions_for_job(job["id"])
    assert sum(1 for s in sessions if s.status == "running") == 1, \
        "Should not have two concurrent sessions for the same job"
```

---

## Combined repro: full failure mode

The 4 bugs compound into the worst-case operator experience:

```bash
# 1. Operator has 11 workdir-bearing cron jobs averaging 8 min each
# 2. A tick fires with 4 of them due simultaneously
#    → Bug 1: serial 32 min lock held
# 3. Operator wants to run a one-off urgent job and triggers it
#    hermes cron run urgent-job
#    → Bug 2: trigger landed 1ms after the in-flight tick sampled now
#    → trigger has no effect on this tick
#    → Bug 3: operator can't see why (no verbose logs)
# 4. After 30 seconds, operator triggers again
# 5. After 5 minutes, operator triggers again
# 6. After 30 minutes, operator triggers a fourth time
#    (each one resets next_run_at — Bug 4)
# 7. The big tick eventually completes
# 8. Next tick fires; sees urgent-job as due; spawns session A
# 9. Within 60s, the FOURTH trigger lands as due again (because
#    Bug 4 doesn't dedupe), next tick spawns session B
#    → Two concurrent sessions for the same job
#    → Duplicate output delivery
#    → Wasted API spend
#    → Possible write race against any resource the job touches
```

Diagnosing this from operator logs is impossible without `verbose=True` (Bug 3). Diagnosing it from state.db requires noticing the duplicate sessions hours later.

---

## Test plan if upstream accepts

- Unit tests for each individual bug (above)
- Integration test: spin up a gateway with two long-running workdir jobs, trigger both, assert they execute concurrently (Bug 1 fix verification)
- Soak test: 1-hour run with 17 jobs of varying durations, monitor for any starvation or duplicates
- Backwards-compat test: ensure existing single-process workdir behavior still works when subprocess-per-job is disabled via config

---

## What we did locally

In our deployment we have applied:

1. **Local patch — Bug 3 only**: `gateway/run.py:11288` `verbose=False → verbose=True`. One-line change to get scheduler visibility; the smallest possible fix.

2. **Workarounds for Bugs 1 + 2 + 4**: tighten cron schedules to spread load across the hour, increase `HERMES_CRON_SCRIPT_TIMEOUT` to 240s for noisy jobs, accept that triggered jobs may take 5-30 min to fire under contention, manually verify no duplicate sessions before re-triggering.

We have NOT applied the architectural fix (Bug 1: subprocess-per-job) because:
- It's invasive (~250 LOC)
- Maintaining a substantial scheduler-core fork creates merge friction on every upstream sync
- The workarounds are tolerable for our portfolio size today

We'd rather see this fixed upstream.

---

## References

- `cron/scheduler.py` — tick logic, file lock, sequential workdir loop, parallel non-workdir pool
- `cron/jobs.py` — `trigger_job`, `get_due_jobs`, `advance_next_run`, staleness/grace logic
- `gateway/run.py` — `_start_cron_ticker`, in-process tick driver
