"""cron-plus subprocess runner — full agent invocation entry point.

Invoked by scheduler.py as:
    python /path/to/cron-plus/runner.py --job-id <id>

Runs in its own subprocess. Each subprocess has its own os.environ,
so the TERMINAL_CWD pollution problem that forces Hermes' built-in
scheduler to serialize workdir jobs is contained.

Lifecycle:
1. Record PID + started_at to ~/.hermes/cron-plus/pids/<id>.pid (JSON
   format — scheduler reads this to defend against PID reuse on the
   next tick's idempotency check).
2. Load Hermes runtime (via hermes_cli.env_loader.load_hermes_dotenv)
   so subsequent imports see the operator's env.
3. Fetch job dict from cron-plus jobs.json.
4. Reuse Hermes' cron.scheduler.run_job() — handles wake-gate
   execution, prompt building, agent invocation, session persistence,
   output saving. Returns (success, output_doc, response, error).
5. Always save the output document locally to
   ~/.hermes/cron-plus/output/<id>/<ts>.md (recoverable trail even
   when delivery fails).
6. If response contains [SILENT], skip delivery (agent's escape
   hatch). Otherwise hand off to Hermes' built-in delivery
   dispatcher cron.scheduler._deliver_result, which handles every
   platform Hermes supports (Telegram with topic targets, Slack,
   Discord, Matrix, origin chat), per-platform target syntax,
   MEDIA: attachment extraction, and config-driven response
   wrapping. We pass adapters=None / loop=None because we are in
   a subprocess without the gateway's live adapters — the
   dispatcher falls back to standalone HTTP send for each platform.
7. Mark job run outcome — success reflects only the agent run;
   delivery failure goes into a separate last_delivery_error field
   so the CLI can render the distinction.
8. Clean up PID file, exit.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional
from pathlib import Path

# Make sibling cron-plus modules importable when invoked as a script.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

# Bootstrap Hermes' env (loads ~/.hermes/.env into os.environ for any
# downstream tool that reads from there — inference-provider keys,
# TELEGRAM_BOT_TOKEN, and any other secrets the job needs at runtime).
try:
    from hermes_cli.env_loader import load_hermes_dotenv
    load_hermes_dotenv()
except Exception as e:
    sys.stderr.write(f"warning: load_hermes_dotenv failed: {e}\n")

import jobs as jobs_mod  # type: ignore[import]  # noqa: E402

logging.basicConfig(
    level=os.environ.get("CRON_PLUS_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("cron-plus.runner")

OUTPUT_DIR = jobs_mod.CRON_PLUS_HOME / "output"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _write_pid_file(job_id: str) -> Path:
    """Record this runner's PID + start time so scheduler.py's
    `_job_is_running` can detect PID reuse on the next tick.

    Format: JSON {"pid": int, "started_at": ISO 8601 UTC}
    """
    pid_file = jobs_mod.CRON_PLUS_HOME / "pids" / f"{job_id}.pid"
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    rec = {"pid": os.getpid(), "started_at": _utc_now().isoformat()}
    try:
        pid_file.write_text(json.dumps(rec))
    except OSError as e:
        logger.warning("failed to write PID file %s: %s", pid_file, e)
    return pid_file


def _save_local_output(job: dict, output_doc: str) -> Path:
    """Write the agent's full output document to disk for `deliver: local`
    AND as a debug archive for any delivery target. Always called regardless
    of delivery target so failed deliveries leave a recoverable trail.
    """
    job_dir = OUTPUT_DIR / job["id"]
    job_dir.mkdir(parents=True, exist_ok=True)
    ts = _utc_now().strftime("%Y-%m-%d_%H-%M-%S")
    out = job_dir / f"{ts}.md"
    out.write_text(output_doc)
    return out


def _deliver_via_upstream(job: dict, content: str) -> Optional[str]:
    """Hand off delivery to Hermes' built-in cron.scheduler._deliver_result.

    Avoid divergence from upstream's
    delivery semantics by reusing the upstream dispatcher. It already
    handles every platform Hermes supports (Telegram with topic targets,
    Slack, Discord, Matrix), origin-chat delivery, MEDIA: attachment
    extraction, comma-separated multi-target dispatch, and the
    config-driven response wrapping. We pass adapters=None / loop=None
    because we are in a subprocess without the gateway's live adapters —
    the dispatcher falls back to standalone HTTP send for each platform.

    Returns None on success, or an error string on failure.
    """
    try:
        from cron.scheduler import _deliver_result  # type: ignore[import]
    except ImportError as e:
        return f"could not import upstream _deliver_result: {e}"
    try:
        return _deliver_result(job, content, adapters=None, loop=None)
    except Exception as e:
        return f"upstream _deliver_result raised {type(e).__name__}: {e}"


def _run_via_hermes(job: dict) -> tuple[bool, str, str, str | None]:
    """Invoke Hermes' built-in cron.scheduler.run_job() — it handles the
    wake-gate, prompt building, agent run, and session persistence.
    We just get back the output."""
    from cron.scheduler import run_job  # type: ignore[import]
    return run_job(job)


def main() -> int:
    parser = argparse.ArgumentParser(description="cron-plus subprocess runner")
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()

    pid_file = _write_pid_file(args.job_id)
    started_at = _utc_now()

    try:
        job = jobs_mod.get_job(args.job_id)
        if not job:
            logger.error("job not found: %s", args.job_id)
            return 2

        job_name = job.get("name", args.job_id)
        logger.info(
            "cron-plus runner starting (job=%s, id=%s, pid=%d, started=%s)",
            job_name, args.job_id, os.getpid(), started_at.isoformat(),
        )

        # Run the agent (or skip if wake-gate emits false)
        try:
            success, output_doc, response, error = _run_via_hermes(job)
        except Exception as e:
            logger.error("agent invocation crashed: %s", e, exc_info=True)
            jobs_mod.mark_job_run(args.job_id, success=False, error=str(e))
            return 1

        if not success:
            logger.error("agent run failed: %s", error or "(no error message)")
            jobs_mod.mark_job_run(args.job_id, success=False, error=error)
            return 1

        # Always save the full output locally for inspection — even when
        # delivering to a remote target, so a failed delivery leaves a
        # recoverable trail at ~/.hermes/cron-plus/output/.
        local_path = _save_local_output(job, output_doc)
        logger.info("output saved to %s", local_path)

        # Honor the [SILENT] marker — agent says "nothing to report,
        # suppress delivery". Same semantics as upstream Hermes cron.
        SILENT_MARKER = "[SILENT]"
        if response and SILENT_MARKER in response.strip().upper():
            logger.info("agent returned %s — skipping delivery", SILENT_MARKER)
            jobs_mod.mark_job_run(args.job_id, success=True, error=None)
            return 0

        # Hand delivery off to upstream Hermes' dispatcher — handles all
        # platforms (Telegram, Slack, Discord, Matrix), origin chat,
        # per-platform targets, MEDIA: attachments, and config-driven
        # response wrapping.
        deliver_err = _deliver_via_upstream(job, response)
        if deliver_err:
            # Agent succeeded but delivery failed — record both facts
            # distinctly so CLI shows the delivery problem instead of
            # a misleading ✓.
            logger.error("delivery failed: %s", deliver_err)
            jobs_mod.mark_job_run(
                args.job_id,
                success=True,
                error=None,
                delivery_error=deliver_err,
            )
        else:
            logger.info("delivery ok")
            jobs_mod.mark_job_run(args.job_id, success=True, error=None, delivery_error=None)
        logger.info(
            "cron-plus runner completed (job=%s, elapsed=%.1fs)",
            job_name, (_utc_now() - started_at).total_seconds(),
        )
        return 0
    finally:
        try:
            pid_file.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
