# Changelog

## [v0.1.0] — 2026-05-02

Initial release.

A subprocess-per-job cron scheduler plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent). Designed as an alternative to the built-in scheduler for projects where workdir-bearing job serialization causes triggered-job starvation. See [`README.md`](./README.md) for install + usage and [`WHY.md`](./WHY.md) for the production-side rationale.

### Features

**Scheduling**
- Daemon ticker thread spawned at gateway boot via `register(ctx)`. Default 60s tick interval, configurable via `CRON_PLUS_TICK_INTERVAL_S` (clamped to `[5, 3600]`).
- Three schedule kinds: standard 5-field cron expressions (croniter), interval shorthand (`30m`, `2h`, `1d`), and one-shot (`once:<ISO>`) with auto-disable after firing.
- `cron-plus` CLI: `list`, `show`, `run`, `pause`, `resume`, `rm`, `tick`, `create`. Available as `hermes cron-plus <sub>` inside agent contexts; for clean-shell access run the CLI module directly.

**Execution**
- Each due workdir-bearing job runs in its own Python subprocess via `Popen` (fire-and-forget). True parallelism — scheduler tick returns in <1 second regardless of how many or how long the spawned jobs run.
- Each subprocess has its own `os.environ`, so the `TERMINAL_CWD` pollution problem that forces Hermes' built-in scheduler to serialize workdir jobs is structurally absent.
- Atomic claim under a single jobs lock (re-verify enabled + not running, advance `next_run_at`, return claimed list) eliminates the snapshot-then-spawn race for jobs removed or paused mid-tick.
- Read-modify-write operations on the job store run under a single exclusive lock (`locked_modify(fn)` helper) so concurrent CLI / tick / runner updates can't lose changes.

**Idempotency**
- PID file per running job, JSON `{pid, started_at}`. PID-reuse defense compares OS-reported process create_time against the recorded `started_at`. Cross-platform via `psutil` (preferred) → Linux `/proc` → `ps -o lstart=` fallback chain.
- `cron run <id>` triggers set `next_run_at = now - 1s` so the next tick that samples `now` reliably picks up the triggered job (avoids the millisecond-scale race the built-in scheduler suffers from).
- `trigger_job()` refuses to re-trigger if the job's previous subprocess is still alive — duplicate-trigger pile-ups don't fire as concurrent sessions.
- If subprocess spawn fails, the runner kills the just-spawned child if PID file write fails (so the next tick can re-spawn cleanly).

**Delivery**
- Hands delivery off to upstream Hermes' `cron.scheduler._deliver_result`. Inherits every platform Hermes supports (Telegram with topic targets, Slack, Discord, Matrix), `origin`-chat delivery, per-job target syntax (`telegram:<chat>:<thread>`), comma-separated multi-target dispatch, MEDIA: attachment extraction, and config-driven response wrapping.
- Honors the `[SILENT]` marker — agent suppresses delivery by responding with exactly `[SILENT]`.
- Agent-success and delivery-success tracked separately. `last_run_success` reflects only the agent run; `last_delivery_error` is set when delivery fails. CLI renders `⚠ delivery failed (…)` distinctly from `✓`.

**Compatibility**
- Compatibility check at plugin load — verifies required Hermes internals (`cron.scheduler.run_job`, `cron.scheduler._deliver_result`) are importable. Logs `cron-plus DISABLED: <missing symbols>` and refuses to start the ticker if upstream renames or removes either.
- Tested against Hermes Agent ≥ v0.11.

**Operational**
- Per-job log file at `~/.hermes/logs/cron-plus/<job>-<ts>.log` — full subprocess stdout+stderr.
- Output archive at `~/.hermes/cron-plus/output/<id>/<ts>.md` for every run, regardless of delivery target — recoverable trail when delivery fails.
- Path-safe log filenames: job names with `/` or `..` are sanitized to `[A-Za-z0-9._-]` with length cap; resolved log path verified inside `LOG_DIR`.
- Gateway restart leaves running runner subprocesses orphaned (`start_new_session=True`) — they survive to completion and clean up their own PID files.
- Migration script (`migrate.py`) copies jobs from `~/.hermes/cron/jobs.json` to `~/.hermes/cron-plus/jobs.json`, preserving job IDs and all upstream fields (`origin`, `context_from`, `skills`, `enabled_toolsets`, `model`, `provider`, `base_url`, `repeat`, `schedule_display`).

### Tests

- 60 tests across `tests/test_jobs.py`, `tests/test_scheduler.py`, `tests/test_backports.py`, `tests/test_review_fixes.py`, `tests/test_round3.py`.
- Cron-schedule tests `pytest.importorskip("croniter")` so a bare `pytest -q` shows skips (with reason) rather than failures when `croniter` is missing. `pip install -e ".[dev]"` installs `croniter` + `PyYAML` + `pytest`.

### Documentation

- [`README.md`](./README.md) — install, CLI, migration, environment variables, architecture, troubleshooting, operational notes, trust & security model, manual end-to-end smoke test.
- [`WHY.md`](./WHY.md) — production-side rationale, the four underlying defects in Hermes' built-in scheduler that motivated this plugin, alternatives considered, "should you use this?" decision section.
- [`docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md`](./docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md) — bug-report-shape detail with line-numbered root causes, reproduction steps, suggested upstream fixes, and test cases. Drafted to be filed at NousResearch/hermes-agent.
