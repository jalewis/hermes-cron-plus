"""Migrate jobs from Hermes' built-in cron to cron-plus.

Source: ~/.hermes/cron/jobs.json   (built-in scheduler)
Target: ~/.hermes/cron-plus/jobs.json   (this plugin)

Runs as a script:
    python /opt/data/plugins/cron-plus/migrate.py [--dry-run] [--source-id <id>]

Default behaviour: copy ALL enabled jobs from built-in to cron-plus,
preserving the same job IDs (so existing references in scripts /
docs continue to work). The original built-in jobs are NOT touched
— operator must run `hermes cron pause <id>` per-job to disable
them after verifying the cron-plus version works.

Pass --source-id to migrate just one job. Use --dry-run to print the
plan without writing.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import jobs as jobs_mod  # type: ignore[import]

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("cron-plus.migrate")

HERMES_HOME = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
BUILTIN_JOBS_FILE = HERMES_HOME / "cron" / "jobs.json"


def _load_builtin() -> list[dict]:
    if not BUILTIN_JOBS_FILE.exists():
        logger.error("built-in jobs.json not found at %s", BUILTIN_JOBS_FILE)
        return []
    try:
        data = json.loads(BUILTIN_JOBS_FILE.read_text())
    except (OSError, json.JSONDecodeError) as e:
        logger.error("failed to read built-in jobs: %s", e)
        return []
    return list(data.get("jobs", []))


def _convert_schedule(builtin_sched: dict) -> dict:
    """Map built-in schedule format to cron-plus format. They're nearly
    identical — built-in's {kind, expr, display} → cron-plus's {kind, expr}."""
    if not isinstance(builtin_sched, dict):
        return {}
    kind = builtin_sched.get("kind")
    if kind == "cron":
        return {"kind": "cron", "expr": builtin_sched.get("expr")}
    if kind == "interval":
        return {"kind": "interval", "interval_s": int(builtin_sched.get("seconds", 0))}
    if kind == "once":
        return {"kind": "once", "run_at": builtin_sched.get("run_at")}
    # Unknown kind — preserve as-is and let cron-plus's compute_next_run reject it
    return dict(builtin_sched)


# Runtime-only fields we drop during migration. Everything else is
# preserved so that fields like origin / context_from / skills /
# enabled_toolsets / model / provider / base_url / repeat / etc. that
# Hermes' run_job() consumes still work after the migrated job runs
# under cron-plus.
_RUNTIME_ONLY_FIELDS = frozenset({
    "next_run_at", "last_run_at", "last_run_success", "last_status",
    "last_error", "last_delivery_error", "paused_at", "paused_reason",
    "state", "created_at",
})


def _convert_job(builtin: dict) -> dict:
    """Convert a built-in cron job dict to cron-plus format.

    Preserves ALL fields from the upstream job spec (origin,
    context_from, skills, enabled_toolsets, model, provider, base_url,
    repeat, schedule_display, etc.) so that Hermes' run_job() can use
    them when the migrated job runs under cron-plus. Only runtime state
    fields (last_run_*, paused_*, state) are stripped — those get
    rewritten as the job runs.

    Job ID is preserved so existing references in scripts/docs continue
    to work.
    """
    out: dict = {}
    for key, value in builtin.items():
        if key in _RUNTIME_ONLY_FIELDS:
            continue
        out[key] = value

    # Translate schedule format to cron-plus's variant
    out["schedule"] = _convert_schedule(builtin.get("schedule") or {})

    # Initial runtime state for the new cron-plus job
    out["next_run_at"] = jobs_mod.compute_next_run(out["schedule"])
    out["last_run_at"] = None
    out["last_run_success"] = None
    out["created_at"] = datetime.now(timezone.utc).isoformat()
    out["_migrated_from_builtin"] = True
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate built-in cron jobs to cron-plus")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the migration plan without writing")
    parser.add_argument("--source-id", default=None,
                        help="Migrate just one job by ID (default: all enabled)")
    parser.add_argument("--include-disabled", action="store_true",
                        help="Migrate disabled jobs too (default: skip)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite cron-plus jobs that already exist by ID")
    args = parser.parse_args()

    builtin = _load_builtin()
    if not builtin:
        return 1

    if args.source_id:
        builtin = [j for j in builtin if j.get("id") == args.source_id]
        if not builtin:
            print(f"No built-in job with id={args.source_id}", file=sys.stderr)
            return 1
    elif not args.include_disabled:
        builtin = [j for j in builtin if j.get("enabled", True)]

    existing = {j["id"]: j for j in jobs_mod.load_jobs()}

    print(f"Migration plan ({'dry run' if args.dry_run else 'live'}):")
    print(f"  Source: {BUILTIN_JOBS_FILE}")
    print(f"  Target: {jobs_mod.JOBS_FILE}")
    print(f"  Jobs to migrate: {len(builtin)}")
    print()

    plan: list[tuple[str, dict, str]] = []  # (action, converted_job, reason)
    for src in builtin:
        sid = src["id"]
        sname = src.get("name", "?")
        if sid in existing and not args.overwrite:
            plan.append(("skip", src, f"id {sid} already in cron-plus (use --overwrite to replace)"))
            continue
        converted = _convert_job(src)
        if not converted["schedule"].get("kind"):
            plan.append(("skip", src, "could not parse schedule"))
            continue
        plan.append(("migrate", converted, ""))

    for action, job, reason in plan:
        flag = "✓" if action == "migrate" else "—"
        sched = job.get("schedule") or {}
        sched_str = sched.get("expr") or sched.get("interval_s") or "?"
        print(f"  {flag} {action:8s} {job.get('id','?'):14s} {job.get('name','?'):32s} {sched_str}")
        if reason:
            print(f"      → {reason}")

    if args.dry_run:
        print("\nDry run — no changes written. Re-run without --dry-run to apply.")
        return 0

    # Apply
    target = jobs_mod.load_jobs()
    target_by_id = {j["id"]: i for i, j in enumerate(target)}
    migrated = 0
    for action, job, _ in plan:
        if action != "migrate":
            continue
        if job["id"] in target_by_id:
            target[target_by_id[job["id"]]] = job  # overwrite
        else:
            target.append(job)
        migrated += 1

    jobs_mod.save_jobs(target)
    print(f"\nMigrated {migrated} job(s) to {jobs_mod.JOBS_FILE}")

    if migrated:
        print("\nNext steps:")
        print("  1. Verify migrated jobs: bash scripts/cron-plus.sh list")
        print("  2. Trigger a low-risk job to confirm it runs in cron-plus:")
        print(f"        bash scripts/cron-plus.sh run {plan[0][1]['id']}")
        print("  3. After confirming, disable the originals in built-in cron:")
        for action, job, _ in plan:
            if action == "migrate":
                print(f"        docker compose exec --user hermes gateway /opt/hermes/.venv/bin/hermes cron pause {job['id']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
