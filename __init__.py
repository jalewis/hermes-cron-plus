"""cron-plus — subprocess-per-job cron scheduler for Hermes.

Replaces the in-process serialization model of the built-in scheduler.
Each due workdir-bearing job is spawned as its own Python subprocess
(fire-and-forget), so:

- Multiple workdir jobs run truly in parallel (no os.environ pollution)
- The ticker thread returns in <1s instead of holding a lock for 30+ min
- Process-level fault isolation: one stuck job can't starve others
- True cron semantics, matching most operators' mental model

Storage is disjoint from the built-in scheduler:
- Built-in:   ~/.hermes/cron/jobs.json
- cron-plus:  ~/.hermes/cron-plus/jobs.json

Both schedulers can coexist. Operators migrate jobs over time.

Lifecycle:
- register(ctx) is called once at gateway/CLI startup
- We spawn a daemon ticker thread that wakes every N seconds, reads
  jobs.json, fires due jobs as subprocesses, returns immediately
- Daemon thread is GC'd when the gateway process exits
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# Module-level guard so the ticker thread is only started once even if
# register() is somehow called multiple times (e.g., tests).
_TICKER_STARTED = False
_TICKER_LOCK = threading.Lock()


def _start_ticker_thread() -> None:
    """Spawn the daemon ticker thread. Idempotent."""
    global _TICKER_STARTED
    with _TICKER_LOCK:
        if _TICKER_STARTED:
            return
        if os.environ.get("CRON_PLUS_DISABLED", "").lower() in ("1", "true", "yes"):
            logger.info("cron-plus ticker disabled via CRON_PLUS_DISABLED env var")
            _TICKER_STARTED = True
            return

        try:
            interval = int(os.environ.get("CRON_PLUS_TICK_INTERVAL_S", "60"))
        except ValueError:
            interval = 60
        interval = max(5, min(interval, 3600))  # clamp to [5s, 1h]

        from . import scheduler

        thread = threading.Thread(
            target=scheduler.run_ticker,
            args=(interval,),
            name="cron-plus-ticker",
            daemon=True,
        )
        thread.start()
        _TICKER_STARTED = True
        logger.info("cron-plus ticker started (interval=%ds)", interval)


# cron-plus integrates against two Hermes internals — declared explicitly
# so we can surface a clear error at plugin load if either is missing,
# rather than failing at first job execution. Bump the supported-version
# range when this list changes.
_REQUIRED_HERMES_SYMBOLS = (
    ("cron.scheduler", "run_job"),         # public-ish — invokes the agent
    ("cron.scheduler", "_deliver_result"),  # PRIVATE (leading _) — used for
                                            #   delivery dispatch parity
)


def _check_hermes_compatibility() -> str | None:
    """Verify required Hermes internals are importable. Returns None on
    success or an error string describing what's missing."""
    import importlib
    missing: list[str] = []
    for module_name, symbol_name in _REQUIRED_HERMES_SYMBOLS:
        try:
            mod = importlib.import_module(module_name)
            if not hasattr(mod, symbol_name):
                missing.append(f"{module_name}.{symbol_name} (symbol missing)")
        except ImportError as e:
            missing.append(f"{module_name}.{symbol_name} (module not found: {e})")
        except Exception as e:
            # The module exists but raises during import — typically a
            # missing secondary dep, malformed config, or a regression
            # in upstream Hermes. Treat as incompatibility so the
            # operator sees `cron-plus DISABLED: <reason>` at load
            # rather than a per-job failure cascade.
            missing.append(
                f"{module_name}.{symbol_name} (import raised "
                f"{type(e).__name__}: {e})"
            )
    if missing:
        return (
            "cron-plus requires the following Hermes internals: "
            + ", ".join(missing)
            + ". This usually means the installed Hermes version is "
            "incompatible with this cron-plus release. Tested against "
            "Hermes ~v0.11+ (commits where cron.scheduler exposes both "
            "run_job and _deliver_result). Consider pinning Hermes to a "
            "compatible version or filing an issue at "
            "https://github.com/jalewis/hermes-cron-plus/issues."
        )
    return None


def register(ctx) -> None:
    """Plugin entry point. Called once at startup by Hermes' plugin loader.

    Registers:
      - daemon ticker thread (the actual scheduler)
      - `hermes cron-plus <subcmd>` CLI for managing jobs
    No tools or runtime hooks — cron-plus is purely a scheduler.

    Verifies compatibility with the host Hermes install at load time so
    breakage from upstream rename/removal of cron.scheduler internals
    surfaces immediately rather than at first job execution.
    """
    incompatibility = _check_hermes_compatibility()
    if incompatibility:
        logger.error("cron-plus DISABLED: %s", incompatibility)
        return  # don't start the ticker — the runner would just fail per-job

    _start_ticker_thread()

    try:
        from . import cli
        ctx.register_cli_command(
            name="cron-plus",
            help="Manage cron-plus jobs (subprocess-per-job scheduler)",
            setup_fn=cli._setup_argparse,
            handler_fn=cli._handler,
        )
    except Exception as e:
        logger.warning("cron-plus: CLI registration failed: %s", e)
