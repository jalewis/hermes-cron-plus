# Why cron-plus exists

This plugin was extracted from a production [Hermes Agent](https://github.com/NousResearch/hermes-agent) deployment after an extended debugging session uncovered four interacting defects in Hermes' built-in cron scheduler. None of the defects are individually catastrophic; together, they make `hermes cron run <id>` unreliable for any deployment running multiple long-running scheduled jobs.

This document is the long-form record of *why* a replacement scheduler was needed. The README covers *what* cron-plus does and *how* to use it; this file is for the operator deciding whether their deployment hits the same pathology.

---

## The deployment that found this

A single Hermes gateway running:

- **17 cron jobs**, of which 11 carry a `workdir` setting (the data-store-touching jobs)
- **Average per-job runtime**: 5-25 minutes (Hermes' standard tool-using agent runtime)
- **Tick interval**: 60s (default)
- **Mix of natural schedules + manual triggers** via `hermes cron run <id>`

Workload pattern: most jobs fire on natural cron schedules (hourly/daily/weekly). Operators occasionally need to trigger a specific job out-of-band — to verify a fresh deploy, to refresh output after editing a prompt, to test a new wake-gate.

---

## What we observed

Symptom: **`hermes cron run <id>` would silently fail to fire the job for 60+ minutes** under load. Operators retried, often multiple times. Eventually the job would run — sometimes twice or three times back-to-back, producing duplicate downstream side effects (delivered messages, file writes, etc.).

A representative timeline from the production session (job names anonymized, durations real):

```
T+00:00 — operator triggers job-X via `hermes cron run <id>`
          cron list shows next_run_at = T+00:00 (looks correct)

T+00:01 — job-A starts running   (8 min daily report)
T+00:09 — job-B starts running   (8 min ingest)
T+00:17 — job-C starts running   (2 min external API poll)
T+00:21 — job-D starts running   (4 min watchlist scan)
T+00:25 — job-E starts running   (21 min raw-data ingest)
T+00:38 — operator triggers job-X AGAIN (still hadn't fired)
T+00:46 — job-F starts running   (31 min source-quality scoring)
T+01:18 — job-G starts running   (7 min RSS digest)
T+01:25 — job-H starts running   (11 min synthesis)
T+01:36 — job-I starts running   (audit pass)
T+01:39 — operator restarts gateway (out of patience)
T+01:39 — operator triggers job-X AGAIN
T+02:21 — job-X FINALLY fires (one of the earlier triggers)
T+02:23 — job-X fires AGAIN (another trigger from the pile)
          → two concurrent agent sessions for the same job ID
          → duplicate delivery
          → wasted compute on both
```

Total operator wait between first trigger and first fire: **~2.5 hours**. Total *productive* triggers to get the job to run: **4**. Net result: 2 concurrent sessions doing the same work.

---

## The 4 underlying defects

We diagnosed against `cron/scheduler.py` and `cron/jobs.py` source. Four separable defects compound:

### 1. Workdir-bearing jobs serialize within a tick

`cron/scheduler.py:1313-1314`:

```python
# Partition due jobs: those with a per-job workdir mutate
# os.environ["TERMINAL_CWD"] inside run_job, which is process-global —
# so they MUST run sequentially to avoid corrupting each other.
workdir_jobs = [j for j in due_jobs if (j.get("workdir") or "").strip()]
parallel_jobs = [j for j in due_jobs if not (j.get("workdir") or "").strip()]
```

The serialization is necessary *given the architecture* — the agent's terminal/file/code_exec tools read `TERMINAL_CWD` from `os.environ`, which is process-global. Two workdir jobs in the same Python process would corrupt each other's CWD.

The cost: the tick's file lock (`_LOCK_FILE`) is held for `sum(per-job runtime)`. With our portfolio that's 30-60+ minutes per "round" of due jobs. During that lock-held window, **no new tick fires**, so no triggered job picked up after the lock acquires can execute until the entire serial pass finishes.

### 2. Trigger-vs-tick race

`cron/jobs.py:trigger_job` sets `next_run_at = _hermes_now()`. The tick that fires immediately after may have already sampled `now` 1-100 ms before the trigger landed, and its `get_due_jobs()` filter uses `next_run_dt <= now` — so the triggered job is excluded by milliseconds. It then has to wait until *the next* tick (60s away) that re-samples `now`.

Combined with #1: if the next-but-one tick is blocked behind a long workdir-job loop, "60s away" becomes "30-60 minutes away."

### 3. Verbose tick logging hardcoded off

`gateway/run.py:11288` calls `cron_tick(verbose=False)`. The scheduler's per-tick INFO logs ("X jobs due", "Running N in parallel", "Job Y skipped — wakeAgent=false") are unreachable without forking the gateway. Operators debugging "why isn't my triggered job firing" have no visibility into whether the scheduler is even seeing it as due.

### 4. `cron run` doesn't dedupe

`trigger_job()` overwrites `next_run_at` unconditionally. If the operator triggers N times (because earlier triggers appeared not to fire), each trigger resets `next_run_at = now` again. Once contention clears, multiple subsequent ticks each see the job as due, advance `next_run_at` to the natural schedule, fire a session, and (because the operator triggered N times) **N sessions can fire back-to-back as duplicates**.

There's no idempotency guard: nothing checks whether a previous session for this job is still running before spawning a new one.

---

## Why a plugin (not an upstream patch)

After diagnosis, three paths were on the table:

### Option A: Patch upstream Hermes

Fix the four defects in the Hermes core. Submit upstream, wait for review.

**Pros**: every Hermes user benefits; idiomatic fix for #2 (one-line change), #3 (env-var gate), #4 (idempotency check).

**Cons**:
- The architectural fix (#1) requires ~250 LOC restructuring of the scheduler. Upstream review for a structural change of that size has a long tail.
- Maintaining a patched fork during the review window means merge conflicts on every `git fetch upstream`.
- For a project that needs the fix today, this isn't actionable.

### Option B: Local downstream patches

Apply minimal patches to `gateway/run.py` + `cron/scheduler.py` in our fork.

**Pros**: smallest diff. We did this for #3 (one-line `verbose=True` flip).

**Cons**:
- Same merge-friction problem as A but without the upstream benefit.
- For #1 (subprocess execution), the patch would touch large parts of the scheduler — high merge risk on every upstream sync.
- "We forked Hermes to fix cron" is a worse onboarding story than "we have a Hermes plugin."

### Option C: Ship a plugin (this approach)

Build a separate scheduler that lives at `~/.hermes/plugins/cron-plus/`, opt-in via `plugins.enabled` in `config.yaml`. Coexists with the built-in scheduler (disjoint job storage). Zero modification of Hermes core.

**Pros**:
- Survives `git fetch upstream` cleanly — pure additive change at the user layer.
- Same distribution pattern as [hermes-web-search-plus](https://github.com/robbyczgw-cla/hermes-web-search-plus) — Hermes users already know how to install plugins.
- Incremental adoption: migrate jobs one-by-one, roll back per-job by re-enabling in built-in cron.
- Solves all 4 defects in one design (subprocess isolation eliminates #1 and #4 by construction; #2 and #3 fixed in the plugin's own code paths).

**Cons**:
- ~600 LOC of net-new code to maintain, vs ~10 LOC if we'd just patched upstream.
- Two schedulers running side-by-side during migration is operationally heavier than one.
- Reuses Hermes' `cron.scheduler.run_job()` for the actual agent invocation — so we have a soft dependency on that internal API not changing.

We picked C because the alternatives all required substantial commitment from upstream maintainers we don't control. The plugin path lets us own the scheduling layer end-to-end while staying ecosystem-compatible.

---

## What we didn't do

A few alternatives considered and rejected:

### Run all cron jobs through `at` / system cron / systemd timers

Possible, but loses Hermes' integration: per-job credential pools, session persistence to `state.db`, `session_search` discoverability, the `hermes cron list/run/pause` UX, the wake-gate `--script` mechanism. Moving to system cron means rebuilding all of that or accepting blind execution.

### Tighten `HERMES_CRON_MAX_PARALLEL` and set per-job timeouts more aggressively

Treats the symptom, not the cause. Workdir jobs still serialize because the underlying `os.environ` mutation is structural, not configurable.

### Split the workload across multiple gateway containers

Could work — give each gateway a subset of the jobs. But coordinating 17 jobs across 3-4 gateways is operationally complex, and Hermes wasn't designed for horizontal scheduling. The shared `~/.hermes/cron/jobs.json` would also need to become per-gateway.

### Wait for upstream

The originating project couldn't wait. cron-plus shipped in two days; upstream bug fix + review for a 250-LOC scheduler restructure is multi-week-to-multi-month optimistic.

---

## Production data after switching

(To be filled in once we have a week of data running on cron-plus.)

Expected:
- Triggered jobs fire within 60s reliably (no more 30-60 min starvation)
- Zero duplicate sessions per job
- Per-job log files at `~/.hermes/logs/cron-plus/<job>-<ts>.log` make debugging easier
- Memory: ~1-1.5 GB peak when 5-10 jobs concurrent (Hermes runtime per subprocess)

---

## Should you use cron-plus?

Probably **yes** if your deployment matches:

- ≥ 5 cron jobs with `workdir` set
- Per-job runtimes regularly exceed 5 minutes
- You use `hermes cron run <id>` for ad-hoc triggers
- You've ever waited >30 seconds for a triggered job to fire

Probably **no** if:

- You have 1-3 cron jobs total
- All jobs are short (<60s)
- You never trigger jobs manually
- You're concerned about the operational complexity of two schedulers (during migration) or the ~100MB-per-concurrent-job memory cost

Probably **maybe** if:

- You want the per-job log file separation for debugging
- You're hitting the trigger-vs-tick race occasionally but it's not blocking you
- You'd rather fix the upstream defects yourself — file an RFC at NousResearch/hermes-agent and use cron-plus as the reference implementation

---

## Related reading

- The full upstream bug report (4 defects + reproduction steps + suggested fixes + test cases) is in this repo as [`docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md`](./docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md). It was drafted to be filed at NousResearch/hermes-agent but is not currently filed.
- Hermes' [plugin developer guide](https://hermes-agent.nousresearch.com/docs/guides/build-a-hermes-plugin) — the framework cron-plus is built against.
- [hermes-web-search-plus](https://github.com/robbyczgw-cla/hermes-web-search-plus) — the plugin we modeled this repo's distribution conventions on.
