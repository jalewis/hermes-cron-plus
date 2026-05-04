# cron-plus — Subprocess-per-job scheduler for Hermes Agent

A drop-in alternative to Hermes' built-in cron scheduler. Each due job runs in its own Python subprocess (fire-and-forget) so workdir-bearing jobs no longer serialize on `os.environ["TERMINAL_CWD"]`. True parallelism, true cron semantics, no fork drift on Hermes core.

Coexists with the built-in scheduler — uses disjoint storage at `~/.hermes/cron-plus/jobs.json` for incremental adoption.

> Plugin for [NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent).
>
> Inspired by — and following the same plugin-distribution conventions as — [hermes-web-search-plus](https://github.com/robbyczgw-cla/hermes-web-search-plus).

---

## Why

Hermes' built-in scheduler runs in-process and serializes all workdir-bearing jobs because `os.environ["TERMINAL_CWD"]` is process-global (`cron/scheduler.py:1313`). For projects with several long-running cron jobs, the consequences compound:

- A tick with N due workdir jobs holds the scheduler's file lock for `sum(per-job runtimes)` — easily 30-60 minutes
- During that window, **all newly triggered jobs starve** (the lock blocks the next tick)
- `hermes cron run <id>` triggers can be missed entirely by milliseconds (`next_run = now + ε` vs `tick samples now`)
- Repeated triggers pile up and fire as **duplicate concurrent sessions** once contention clears

cron-plus avoids all of this by spawning each due job as a separate Python subprocess. The ticker tick returns in <1 second regardless of how many or how long the spawned jobs run.

This plugin was extracted from a real production deployment after an extended debugging session uncovered four interacting defects in the built-in scheduler. **If you're considering adopting cron-plus, read [`WHY.md`](./WHY.md) first** — it covers the production data, the full diagnostic timeline, the four underlying defects, why a plugin was the right fix vs upstream patches, and a "should you use this?" decision section. The companion document [`docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md`](./docs/UPSTREAM_HERMES_CRON_BUG_REPORT.md) has the technical bug-report-shape detail (reproduction steps, line-numbered root causes, suggested upstream fixes, test cases) for anyone who'd rather drive the upstream fix than adopt a plugin.

---

## Quick start

```bash
git clone https://github.com/jalewis/hermes-cron-plus.git ~/.hermes/plugins/cron-plus
```

Enable in `~/.hermes/config.yaml`:

```yaml
plugins:
  enabled:
    - cron-plus
```

Restart Hermes (or restart the gateway container if running dockerized). The ticker thread starts automatically on plugin load.

Verify the plugin loaded:

```bash
hermes plugins list | grep cron-plus
```

If the plugin shows as enabled but the ticker doesn't start, the most
common cause is a Hermes version mismatch — cron-plus does a
compatibility check at load time and logs `cron-plus DISABLED: ...` to
`~/.hermes/logs/agent.log` with the missing symbols. See
[Compatibility](#compatibility) below.

---

## Running the tests

cron-plus depends on `croniter` and `PyYAML` at runtime. The plugin's
`pyproject.toml` declares both as runtime deps and `pytest` as a dev
dep, so the canonical setup is:

```bash
cd ~/.hermes/plugins/cron-plus
pip install -e ".[dev]"     # installs croniter + PyYAML + pytest
pytest -q                   # 60 tests collected; 4 require croniter
                            # (skipped, not failed, when missing)
```

Without `pip install`, bare `pytest` will SKIP (not fail) the
cron-schedule tests — they require `croniter`, which isn't in the
stdlib. The non-cron tests still run.

---

## Compatibility

cron-plus is built against and tested with **Hermes Agent ≥ v0.11**
(the era when `cron.scheduler` exposes both `run_job` and
`_deliver_result`). The latter is a private symbol (leading
underscore), so this is a soft dependency on internals. If upstream
renames or removes either, the plugin will refuse to start the ticker
and log a clear `cron-plus DISABLED` message naming the missing
symbols rather than failing per-job at runtime.

If you hit such a message, check upstream Hermes for the rename and
file an issue at
https://github.com/jalewis/hermes-cron-plus/issues so the symbol list
in `__init__.py:_REQUIRED_HERMES_SYMBOLS` can be updated.

---

## CLI usage

The plugin registers `hermes cron-plus <subcmd>` for use within agent contexts (`hermes chat`, the gateway's interactive mode, etc.). For clean-shell access from outside any agent invocation, run the CLI module directly:

```bash
python ~/.hermes/plugins/cron-plus/cli.py list
python ~/.hermes/plugins/cron-plus/cli.py show <id>
python ~/.hermes/plugins/cron-plus/cli.py run <id>          # trigger immediate run
python ~/.hermes/plugins/cron-plus/cli.py pause <id>
python ~/.hermes/plugins/cron-plus/cli.py resume <id>
python ~/.hermes/plugins/cron-plus/cli.py rm <id>           # asks for confirmation
python ~/.hermes/plugins/cron-plus/cli.py rm <id> -f        # skip confirmation
python ~/.hermes/plugins/cron-plus/cli.py tick              # manually fire a tick (testing)

python ~/.hermes/plugins/cron-plus/cli.py create '0 10 * * 0' --name foo \
    --workdir /opt/data/work \
    --prompt 'Daily brief...' \
    --deliver telegram

python ~/.hermes/plugins/cron-plus/cli.py create '30m' --name bar --prompt '...'
python ~/.hermes/plugins/cron-plus/cli.py create 'once:2026-05-03T18:00:00+00:00' --name one-shot --prompt '...'
```

Schedule arg accepts:
- 5-field cron expression: `'0 10 * * 0'` (croniter, UTC)
- Interval shorthand: `30s`, `30m`, `2h`, `1d`
- One-shot: `once:<ISO 8601 timestamp>` (auto-disables after firing)

Most projects shell into a Docker container or wrap the call in a project script — see the migration section for examples.

---

## Migration from built-in cron

```bash
# Dry-run to preview the plan
python ~/.hermes/plugins/cron-plus/migrate.py --dry-run

# Migrate one job to test (recommended first)
python ~/.hermes/plugins/cron-plus/migrate.py --source-id <job-id>

# Trigger via cron-plus to confirm the subprocess flow works
python ~/.hermes/plugins/cron-plus/cli.py run <job-id>

# Confirm output appears at ~/.hermes/logs/cron-plus/<job>-<ts>.log
# AND any delivery (e.g., Telegram) lands as expected.

# Disable the original in built-in cron so it doesn't double-fire:
hermes cron pause <job-id>

# When ready, bulk migrate all enabled jobs:
python ~/.hermes/plugins/cron-plus/migrate.py
# (then disable each in built-in per the script's printed instructions)
```

Migration preserves job IDs so any external references continue to work. The migrated job appears in BOTH schedulers until you disable the built-in version — be deliberate about ordering or you'll get duplicate runs.

---

## Disaster recovery / re-deploys

The `~/.hermes/cron-plus/jobs.json` file holds both job *definitions* (id, name, schedule, prompt, model, …) and *runtime* fields (`next_run_at`, `last_run_at`, `last_run_success`, `last_error`, `last_delivery_error`). For DR, sanitize the runtime fields out and keep the result under version control as your source-of-truth, then deploy by snapshot + copy:

```bash
# 1. Sanitize live → repo source-of-truth
python -c "
import json
RUNTIME = {'next_run_at','last_run_at','last_run_success','last_error','last_delivery_error'}
d = json.load(open('/home/.../jobs.json'))
for j in d['jobs']:
    for k in RUNTIME: j.pop(k, None)
    if isinstance(j.get('repeat'), dict): j['repeat'].pop('completed', None)
d['jobs'].sort(key=lambda j: j.get('name',''))
json.dump(d, open('config/cron-plus-jobs.json','w'), indent=2)
"

# 2. Restore: snapshot + copy
cp -p ~/.hermes/cron-plus/jobs.json ~/.hermes/cron-plus/jobs.json.bak.$(date +%s)
cp -p config/cron-plus-jobs.json ~/.hermes/cron-plus/jobs.json
```

**As of v0.1.2, no extra seed step is required.** The next scheduler tick (~60s) calls `claim_due_jobs()`, which detects every enabled job's null `next_run_at` and self-heals it from the schedule under the same exclusive lock as the claim itself. Pre-v0.1.2 deploys had to run an external `seed-cron-plus.py` workaround inside the gateway container or jobs would sit silent forever — see CHANGELOG for the gory details.

---

## Built-in vs cron-plus

| Aspect | Built-in (`hermes cron`) | cron-plus |
|---|---|---|
| Storage | `~/.hermes/cron/jobs.json` | `~/.hermes/cron-plus/jobs.json` |
| Execution model | In-process; workdir jobs serialize | Subprocess-per-job; full parallelism |
| Tick lock duration | Held for entire serial workdir-job duration (5-60+ min) | Held for ~1s (just enumerate + Popen) |
| Triggered-job semantics | Race against tick boundary; can starve under contention | Reliable: `next_run = now - 1s` + idempotency guard |
| Duplicate trigger handling | None — pile-ups fire as concurrent sessions | PID-tracked: refuses re-trigger if previous still alive |
| Memory | One Python process | One process per concurrent job (~100MB each) |
| Process startup cost | 0 | ~2-3s per spawn (negligible vs 5-25 min runtimes) |
| Live adapter sharing | Yes (uses gateway's polling adapters directly) | No (subprocess can't reach the gateway's adapters) — but delivery delegates to upstream `cron.scheduler._deliver_result` which falls back to standalone HTTP send for each platform |
| Observability | Single `agent.log` stream | Per-job log at `~/.hermes/logs/cron-plus/<job>-<ts>.log` |
| Compatibility with `hermes cron` CLI | Yes (native) | No — use `python ~/.hermes/plugins/cron-plus/cli.py` |

---

## Environment variables

| Var | Default | Effect |
|---|---|---|
| `CRON_PLUS_TICK_INTERVAL_S` | `60` | Ticker wake interval (clamped to `[5, 3600]`) |
| `CRON_PLUS_DISABLED` | unset | Set to `1` to disable the ticker entirely (jobs persist but don't fire) |
| `CRON_PLUS_LOG_LEVEL` | `INFO` | Subprocess runner log level |

Read from `~/.hermes/.env` automatically (loaded into `os.environ` by Hermes' env loader at gateway boot).

Delivery is handled by upstream Hermes' `cron.scheduler._deliver_result`,
so cron-plus inherits whatever delivery targets your Hermes install
supports — Telegram, Slack, Discord, Matrix, the originating chat, and
per-job target syntax (e.g. `deliver: telegram:<chat_id>:<thread_id>`,
`deliver: telegram,local`, etc.). The same env vars Hermes' built-in
cron uses for each platform apply (e.g., `TELEGRAM_BOT_TOKEN` +
`TELEGRAM_HOME_CHANNEL` for default Telegram delivery). The runner
also honors the agent's `[SILENT]` marker to suppress delivery when
there is nothing to report.

---

## Architecture

```
~/.hermes/plugins/cron-plus/
  __init__.py        # register(ctx) — spawns daemon ticker thread on plugin load
  scheduler.py       # tick() — fcntl lock → enumerate due → Popen each → release
  jobs.py            # CRUD + croniter-based schedule eval
  runner.py          # subprocess entry point — invokes Hermes run_job + delivery
  cli.py             # argparse-based subcommand handlers
  migrate.py         # one-shot migration from built-in cron

~/.hermes/cron-plus/
  jobs.json          # job definitions (disjoint from built-in cron/)
  .tick.lock         # fcntl lock held by the active tick (released in <1s normally)
  pids/<job-id>.pid  # PID of the subprocess for each running job
  output/<job-id>/<ts>.md  # full output doc per run (for `deliver: local`)

~/.hermes/logs/cron-plus/<job-name>-<ts>.log  # per-subprocess stdout+stderr
```

### Tick lifecycle

```
ticker thread wakes (every CRON_PLUS_TICK_INTERVAL_S, default 60s)
  ↓
acquire ~/.hermes/cron-plus/.tick.lock (fcntl LOCK_EX | LOCK_NB)
  ↓ (failed → another tick in flight, skip)
load jobs.json under shared lock
  ↓
filter to due (next_run_at <= now AND enabled)
  ↓
for each due job:
    if PID file says previous run is alive → skip (idempotency)
    advance_next_run(job_id)         # at-most-once semantics preserved
    Popen([python, runner.py, --job-id, <id>])
    log spawn
release tick lock
  ↓ (returns in <1s typically)
sleep, repeat
```

### Subprocess lifecycle

```
runner.py starts
  ↓
write PID file ~/.hermes/cron-plus/pids/<job-id>.pid
  ↓
load_hermes_dotenv() — populate os.environ from ~/.hermes/.env
  ↓
fetch job dict from cron-plus jobs.json
  ↓
import cron.scheduler.run_job; call it
  ↓ (run_job handles wake-gate, prompt build, agent invocation, session save)
save full output to ~/.hermes/cron-plus/output/<job-id>/<ts>.md
  ↓
hand off to cron.scheduler._deliver_result (handles all platforms,
  per-target syntax, MEDIA: attachments, response wrapping)
  ↓
mark_job_run(success, delivery_error=...)  # distinct fields
  ↓
delete PID file (in finally block)
  ↓
exit
```

---

## Operational notes

cron-plus is a long-running subprocess-spawning scheduler. Treat it
like any production cron host: budget for resources, plan for cleanup,
know what happens when the gateway dies.

**Resource limits per concurrent job.** Each spawned job loads a fresh
Hermes runtime — about 100 MB resident set size baseline, plus
whatever the agent's tool calls allocate. With N jobs concurrent at
peak, expect ~100 MB × N + agent overhead. If you're running in a
container, set memory limits at the container level
(`docker compose deploy.resources.limits.memory`) or per-process via a
wrapper. There is currently no per-job memory cap inside cron-plus.

**Concurrent job limits.** Currently unbounded — every job that's
due gets `Popen`'d each tick. If a tick claims more jobs than the
host has memory for, you'll thrash. Mitigations until cron-plus has a
built-in limit (planned for v0.2):
- Stagger schedules so no single tick claims more than N jobs
- Set `CRON_PLUS_TICK_INTERVAL_S` higher to reduce burst rate
- Container-level cap on the gateway process

**Log retention.** Per-job logs accumulate at
`~/.hermes/logs/cron-plus/<job-name>-<ts>.log` with no built-in
rotation. For a portfolio firing 17 jobs every 30 minutes, that's
~800 logs/day. Recommend an external log-rotation cron (e.g.,
`find ~/.hermes/logs/cron-plus -name "*.log" -mtime +14 -delete`).

**Stuck PID recovery.** If a runner subprocess crashes hard (SIGKILL,
host OOM-kill, gateway container restart) it may leave a PID file
behind. The next tick's `_job_is_running` check verifies the recorded
PID is alive AND has the matching `started_at` create-time — so a
truly-dead-and-PID-not-reused leaves the file as garbage but is
correctly cleaned up on the first re-check. A reused PID belonging to
an unrelated process is detected by the create-time mismatch (requires
`psutil`, `/proc`, or `ps -o lstart=` available — see
[Troubleshooting](#troubleshooting)). Manual recovery if anything
looks stuck:

```bash
ls ~/.hermes/cron-plus/pids/         # who's "running"?
rm ~/.hermes/cron-plus/pids/<id>.pid # force forget; next tick re-evaluates
```

**Gateway restart behavior.** When the gateway process exits, all
running runner subprocesses are orphaned (they were started with
`start_new_session=True` so they survive the parent's death and
continue to completion). Their PID files persist; on next gateway
boot, the new ticker's `_job_is_running` check correctly sees the
orphaned runner as still alive and won't double-spawn. When the
runner finally exits, it cleans up its own PID file.

If you actively want to kill orphaned runners on gateway restart,
that's up to your container/systemd setup — cron-plus does not
shepherd subprocesses.

---

## Trust & security model

cron-plus runs scheduled jobs through Hermes' agent runtime, which
means **every job has the same privileges as the gateway process
itself**:

- Reads/writes to the gateway's `workdir` (per-job, set in jobs.json)
  and any path the agent's tools can touch
- Network access to all configured providers (with the API keys in
  `~/.hermes/.env`)
- Can run shell commands via the agent's `terminal` / `code_exec`
  tools, with the gateway user's shell permissions
- Can deliver to any platform the gateway has credentials for
  (Telegram bot token, Slack webhook, Discord, Matrix)
- Persists results to the gateway's `state.db`

**Anyone who can write to `~/.hermes/cron-plus/jobs.json` can
effectively execute arbitrary code as the gateway user**, including
exfiltration via the configured delivery targets. This is the same
trust model as Hermes' built-in cron — cron-plus does not weaken or
expand it — but it's worth stating explicitly because the plugin's
"add another scheduler" framing might otherwise read as more
isolated than it actually is.

Practical implications:
- Don't let untrusted operators write to that file
- Treat the per-job log files (which contain agent transcripts)
  as sensitive
- If the gateway process compromises, every cron-plus job becomes
  a code-execution beachhead

---

## Smoke test (manual end-to-end)

We don't ship an automated end-to-end test — that would require a
running Hermes gateway. The following is the manual smoke test we
run after each cron-plus change to validate the integration with
Hermes itself.

Setup:
1. Have a working Hermes gateway running with this plugin enabled.
2. Have at least one configured delivery target (e.g.,
   `TELEGRAM_BOT_TOKEN` + `TELEGRAM_HOME_CHANNEL` in `~/.hermes/.env`).

Procedure:

```bash
# 1. Plugin loads cleanly
hermes plugins list | grep cron-plus
grep "cron-plus ticker started" ~/.hermes/logs/agent.log | tail -1

# 2. Create a short interval job with a trivial prompt
python ~/.hermes/plugins/cron-plus/cli.py create 2m \
    --name smoke-1 --prompt "Respond with exactly: smoke ok" \
    --deliver local

# 3. Trigger it immediately
JOB_ID=$(python ~/.hermes/plugins/cron-plus/cli.py list | grep smoke-1 | awk '{print $1}')
python ~/.hermes/plugins/cron-plus/cli.py run "$JOB_ID"

# 4. Wait for the next tick (~60s)
sleep 90
python ~/.hermes/plugins/cron-plus/cli.py show "$JOB_ID"   # last_run_success: true

# 5. Verify per-job log + output landed
ls ~/.hermes/logs/cron-plus/smoke-1-*.log | tail -1 | xargs cat | head
ls ~/.hermes/cron-plus/output/$JOB_ID/                     # one .md file per run

# 6. Test [SILENT] suppresses delivery
python ~/.hermes/plugins/cron-plus/cli.py create 2m \
    --name smoke-silent --prompt "Respond with exactly: [SILENT]" \
    --deliver telegram
SILENT_ID=$(python ~/.hermes/plugins/cron-plus/cli.py list | grep smoke-silent | awk '{print $1}')
python ~/.hermes/plugins/cron-plus/cli.py run "$SILENT_ID"
sleep 90
# Confirm: no Telegram message arrived; agent.log shows "[SILENT] — skipping delivery"

# 7. Test failed delivery shows distinctly in CLI
# (set deliver to a chat the bot can't reach; e.g., wrong chat_id)
python ~/.hermes/plugins/cron-plus/cli.py create 2m \
    --name smoke-baddelivery --prompt "Hello" \
    --deliver telegram:99999999999999
BAD_ID=$(python ~/.hermes/plugins/cron-plus/cli.py list | grep smoke-baddelivery | awk '{print $1}')
python ~/.hermes/plugins/cron-plus/cli.py run "$BAD_ID"
sleep 90
python ~/.hermes/plugins/cron-plus/cli.py list | grep smoke-baddelivery
# Should show: ⚠ delivery failed (...)
# NOT: ✓

# 8. Test workdir job runs in its own working dir
python ~/.hermes/plugins/cron-plus/cli.py create 2m \
    --name smoke-workdir --prompt "Run \`pwd\` and report what you see." \
    --workdir /tmp \
    --deliver local
WD_ID=$(python ~/.hermes/plugins/cron-plus/cli.py list | grep smoke-workdir | awk '{print $1}')
python ~/.hermes/plugins/cron-plus/cli.py run "$WD_ID"
sleep 90
cat ~/.hermes/cron-plus/output/$WD_ID/*.md | grep -i tmp   # should mention /tmp

# Cleanup
for id in "$JOB_ID" "$SILENT_ID" "$BAD_ID" "$WD_ID"; do
    python ~/.hermes/plugins/cron-plus/cli.py rm "$id" -f
done
```

Pass criteria:
- Step 4: `last_run_success: true`
- Step 5: log file exists, output `.md` file exists
- Step 6: no Telegram message; agent.log shows the SILENT skip
- Step 7: CLI shows `⚠ delivery failed` (not `✓`)
- Step 8: agent transcript mentions `/tmp` (proves workdir landed)

---

## Troubleshooting

**Plugin doesn't appear in `hermes plugins list`**
- Confirm `~/.hermes/config.yaml` has `plugins.enabled: [cron-plus]`
- Confirm the plugin directory exists: `ls ~/.hermes/plugins/cron-plus/`
- Restart Hermes / the gateway

**Ticker logs nothing at startup**
- Look for `cron-plus ticker started` in `~/.hermes/logs/agent.log`
- If missing, check the gateway log for an import error in the `cron_plus` module
- Verify `CRON_PLUS_DISABLED` is not set in your `.env`

**A job in jobs.json never fires**
- Confirm it's enabled: `python ~/.hermes/plugins/cron-plus/cli.py show <id>`
- Confirm `next_run_at` is in the past: ticker only picks up due jobs
- Check the gateway log for `cron-plus tick: N job(s) due` — if N=0 the gate filtered it out
- Check no PID file is lingering: `ls ~/.hermes/cron-plus/pids/`. A stale file from a crashed subprocess can block the idempotency guard. Delete it manually if so.

**Subprocess crashes on import**
- Each spawn is `python ~/.hermes/plugins/cron-plus/runner.py --job-id <id>`
- Check the per-job log file at `~/.hermes/logs/cron-plus/<job>-<ts>.log` for the traceback
- Common cause: Hermes runtime not findable on `PYTHONPATH` inside the subprocess

**Delivery fails but agent ran fine**
- Delivery delegates to upstream Hermes' `cron.scheduler._deliver_result`, so
  the same env vars the built-in scheduler needs apply (e.g.,
  `TELEGRAM_BOT_TOKEN` + `TELEGRAM_HOME_CHANNEL` for default Telegram).
- Agent success and delivery success are tracked separately:
  `last_run_success` reflects only the agent run; `last_delivery_error`
  is set when delivery fails. The CLI renders `⚠ delivery failed (…)`
  in that case rather than a misleading `✓`.
- Check the per-job log at `~/.hermes/logs/cron-plus/<job>-<ts>.log`
  for the dispatcher's error message.

**Jobs are firing twice (once in built-in, once in cron-plus)**
- You migrated but didn't disable the built-in version
- Run: `hermes cron pause <id>`

---

## What's NOT supported (yet)

- The `hermes cron-plus <subcmd>` CLI only works inside agent invocations. Hermes' plugin loader doesn't discover plugins for arbitrary CLI subcommands. Use `python ~/.hermes/plugins/cron-plus/cli.py ...` for clean-shell access.
- No equivalent of Hermes' `cron edit` — for now, edit `~/.hermes/cron-plus/jobs.json` directly (snapshot first)
- No support for `repeat: {times: N}` (built-in's "run N times then auto-disable"). Use `once:` for true one-shots.
- Per-job model overrides via the `model` / `provider` / `base_url` fields work because we pass the full job dict to Hermes' `run_job()` — but they aren't surfaced in the CLI yet (edit `~/.hermes/cron-plus/jobs.json` directly for now).
- pip / NixOS distribution — currently directory-install only via `git clone`. PyPI packaging is on the roadmap.

---

## Contributing

Issues and PRs welcome. The main areas needing work:

- **End-to-end CI test against a real Hermes** — current 60-test suite covers `jobs.py`, `scheduler.py`, the migration script, and a battery of regression cases, but doesn't exercise the full subprocess→Hermes-runtime→delivery path automatically. The manual procedure in [Smoke test](#smoke-test-manual-end-to-end) covers it; landing a containerized integration test in CI would let us advance the PyPI Development Status classifier with confidence
- **CLI command parity** — `edit`, `tail` (follow latest log), `output` (cat the most recent agent output) would all be useful additions
- **PyPI packaging** — need a `pyproject.toml` with the right entry points so `pip install hermes-cron-plus` works
- **Job-import format** — a YAML schema so jobs can be defined declaratively and imported via `cron-plus import jobs.yml` instead of one `create` per job

## License

MIT — see [LICENSE](./LICENSE).
