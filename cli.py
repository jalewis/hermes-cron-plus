"""cron-plus CLI — manage jobs.

Usable two ways:

1. Via Hermes plugin CLI: `hermes cron-plus <subcmd>`
   (Only works for `hermes chat`-style invocations, since Hermes only
   discovers plugins for agent commands, not arbitrary subcommands.)

2. Directly as a Python script: `python /opt/data/plugins/cron-plus/cli.py <subcmd>`
   The host wrapper at `scripts/cron-plus.sh` shells into the container
   and runs this — works from a clean host shell without needing
   `hermes chat`.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# When run as `python cli.py ...`, resolve siblings on sys.path
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    from . import jobs as jobs_mod  # type: ignore[import]
    from . import scheduler as scheduler_mod  # type: ignore[import]
except ImportError:
    import jobs as jobs_mod  # type: ignore[import]
    import scheduler as scheduler_mod  # type: ignore[import]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _fmt_relative(iso_ts: str | None) -> str:
    if not iso_ts:
        return "-"
    try:
        ts = datetime.fromisoformat(iso_ts)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except ValueError:
        return iso_ts
    delta = ts - datetime.now(timezone.utc)
    total = int(delta.total_seconds())
    sign = "in " if total >= 0 else ""
    suffix = "" if total >= 0 else " ago"
    total = abs(total)
    if total < 60:
        return f"{sign}{total}s{suffix}"
    if total < 3600:
        return f"{sign}{total//60}m{suffix}"
    if total < 86400:
        return f"{sign}{total//3600}h{(total%3600)//60}m{suffix}"
    return f"{sign}{total//86400}d{suffix}"


def _cmd_list(args: argparse.Namespace) -> int:
    jobs = jobs_mod.list_jobs(include_disabled=True)
    if not jobs:
        print("No cron-plus jobs configured.")
        return 0
    print(f"{'ID':<14} {'NAME':<32} {'ENABLED':<8} {'SCHEDULE':<22} {'NEXT RUN':<22} {'LAST OK'}")
    print("-" * 110)
    for j in sorted(jobs, key=lambda x: x.get("name", "")):
        sched = j.get("schedule") or {}
        sched_str = sched.get("expr") or sched.get("interval_s") or sched.get("run_at") or "?"
        # Distinct rendering for agent-success-vs-delivery-failure
        #.
        last_ok = j.get("last_run_success")
        delivery_err = j.get("last_delivery_error")
        if last_ok is True and delivery_err:
            last_str = f"⚠ delivery failed ({delivery_err[:40]})"
        elif last_ok is True:
            last_str = "✓"
        elif last_ok is False:
            last_str = "✗"
            if j.get("last_error"):
                last_str += f" ({j['last_error'][:30]})"
        else:
            last_str = "-"
        print(
            f"{j['id']:<14} {j.get('name', '?'):<32} "
            f"{'yes' if j.get('enabled', True) else 'no':<8} "
            f"{str(sched_str):<22} "
            f"{_fmt_relative(j.get('next_run_at')):<22} "
            f"{last_str}"
        )
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    j = jobs_mod.get_job(args.job_id)
    if not j:
        print(f"Job not found: {args.job_id}", file=sys.stderr)
        return 1
    print(json.dumps(j, indent=2, default=str))
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    j = jobs_mod.trigger_job(args.job_id)
    if not j:
        print(f"Job not found: {args.job_id}", file=sys.stderr)
        return 1
    print(f"Triggered: {j.get('name', args.job_id)} (id={j['id']})")
    print(f"  next_run_at: {j['next_run_at']}")
    print(f"  Will fire on the next ticker pass (within ~60s).")
    return 0


def _cmd_pause(args: argparse.Namespace) -> int:
    j = jobs_mod.pause_job(args.job_id)
    if not j:
        print(f"Job not found: {args.job_id}", file=sys.stderr)
        return 1
    print(f"Paused: {j.get('name')} (id={j['id']})")
    return 0


def _cmd_resume(args: argparse.Namespace) -> int:
    j = jobs_mod.resume_job(args.job_id)
    if not j:
        print(f"Job not found: {args.job_id}", file=sys.stderr)
        return 1
    print(f"Resumed: {j.get('name')} (id={j['id']})  next_run_at={j['next_run_at']}")
    return 0


def _cmd_rm(args: argparse.Namespace) -> int:
    if not args.force:
        j = jobs_mod.get_job(args.job_id)
        if not j:
            print(f"Job not found: {args.job_id}", file=sys.stderr)
            return 1
        print(f"Will delete: {j.get('name')} (id={j['id']})")
        confirm = input("Type 'yes' to confirm: ").strip().lower()
        if confirm != "yes":
            print("Aborted.")
            return 1
    if jobs_mod.remove_job(args.job_id):
        print(f"Removed: {args.job_id}")
        return 0
    print(f"Job not found: {args.job_id}", file=sys.stderr)
    return 1


def _cmd_tick(args: argparse.Namespace) -> int:
    """Manually fire one scheduler tick (useful for testing)."""
    summary = scheduler_mod.tick()
    print(json.dumps(summary, indent=2))
    return 0


def _cmd_create(args: argparse.Namespace) -> int:
    """Create a job. Schedule arg can be a 5-field cron expr, an interval
    spec like '10m'/'2h', or 'once:<ISO timestamp>'."""
    sched_raw = args.schedule.strip()
    schedule: dict
    if sched_raw.startswith("once:"):
        schedule = {"kind": "once", "run_at": sched_raw[len("once:"):].strip()}
    elif sched_raw[-1:].lower() in ("s", "m", "h", "d") and sched_raw[:-1].isdigit():
        unit = sched_raw[-1].lower()
        n = int(sched_raw[:-1])
        seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit] * n
        schedule = {"kind": "interval", "interval_s": seconds}
    elif len(sched_raw.split()) == 5:
        schedule = {"kind": "cron", "expr": sched_raw}
    else:
        print(f"Could not parse schedule: {sched_raw!r}", file=sys.stderr)
        print("  Examples: '0 10 * * 0' (cron), '30m' (interval), 'once:2026-05-03T18:00:00+00:00'", file=sys.stderr)
        return 2

    job = jobs_mod.create_job(
        name=args.name,
        schedule=schedule,
        workdir=args.workdir,
        prompt=args.prompt,
        script=args.script,
        deliver=args.deliver,
    )
    print(f"Created: {job['name']} (id={job['id']})")
    print(f"  schedule: {schedule}")
    print(f"  next_run_at: {job['next_run_at']}")
    return 0


def _setup_argparse(subparser: argparse.ArgumentParser) -> None:
    subs = subparser.add_subparsers(dest="cron_plus_cmd", required=False)

    p_list = subs.add_parser("list", help="List all cron-plus jobs")
    p_list.set_defaults(func=_cmd_list)

    p_show = subs.add_parser("show", help="Show full JSON for a job")
    p_show.add_argument("job_id")
    p_show.set_defaults(func=_cmd_show)

    p_run = subs.add_parser("run", help="Trigger a job to run on the next tick")
    p_run.add_argument("job_id")
    p_run.set_defaults(func=_cmd_run)

    p_pause = subs.add_parser("pause", help="Disable a job")
    p_pause.add_argument("job_id")
    p_pause.set_defaults(func=_cmd_pause)

    p_resume = subs.add_parser("resume", help="Re-enable a job")
    p_resume.add_argument("job_id")
    p_resume.set_defaults(func=_cmd_resume)

    p_rm = subs.add_parser("rm", help="Remove a job")
    p_rm.add_argument("job_id")
    p_rm.add_argument("-f", "--force", action="store_true", help="Skip confirmation prompt")
    p_rm.set_defaults(func=_cmd_rm)

    p_tick = subs.add_parser("tick", help="Manually fire one scheduler tick (testing)")
    p_tick.set_defaults(func=_cmd_tick)

    p_create = subs.add_parser("create", help="Create a new job")
    p_create.add_argument("schedule",
        help="Cron expr ('0 10 * * 0'), interval ('30m', '2h'), or 'once:<ISO timestamp>'")
    p_create.add_argument("--name", required=True)
    p_create.add_argument("--workdir", default=None)
    p_create.add_argument("--prompt", default=None)
    p_create.add_argument("--script", default=None)
    p_create.add_argument(
        "--deliver", default="local",
        help="Delivery target(s). Examples: 'local', 'telegram', "
             "'telegram:-1001234:17', 'telegram,local'. Validation deferred "
             "to runtime so per-job target syntax is not blocked.",
    )
    p_create.set_defaults(func=_cmd_create)


def _handler(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        # No subcommand → show help
        print("Usage: hermes cron-plus <list|show|run|pause|resume|rm|tick|create>")
        print("Run with --help on any subcommand for details.")
        return 1
    return func(args)


# ─── Standalone-script entry point ─────────────────────────────────────


def _main() -> int:
    """Standalone entry point. Usable as:
        python /opt/data/plugins/cron-plus/cli.py <subcmd> [args]
    """
    parser = argparse.ArgumentParser(prog="cron-plus", description="Manage cron-plus jobs")
    _setup_argparse(parser)
    args = parser.parse_args()
    return _handler(args)


if __name__ == "__main__":
    sys.exit(_main())
