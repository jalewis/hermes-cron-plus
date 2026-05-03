"""cron-plus scheduler — fire-and-forget subprocess-per-job ticker.

Reads ~/.hermes/cron-plus/jobs.json, identifies due jobs, spawns each
as its own Python subprocess via cron-plus.runner. Returns immediately
without waiting — true cron semantics.

Design choices:
- File lock prevents concurrent ticks across multiple gateway instances
  (only one tick fires per interval globally)
- Each tick is short (<1s) — just enumerates due jobs and Popen's them
- Subprocess output goes to per-job log files (not the main agent.log)
- next_run_at is advanced BEFORE Popen so a crash mid-spawn doesn't
  cause double-fire on the next tick
- Idempotency: if a job's previous subprocess is still running (PID
  recorded in state), skip this tick for that job
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from . import jobs as jobs_mod  # type: ignore[import]
except ImportError:
    import jobs as jobs_mod  # type: ignore[import]

logger = logging.getLogger(__name__)

HERMES_HOME = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
CRON_PLUS_HOME = HERMES_HOME / "cron-plus"
LOG_DIR = HERMES_HOME / "logs" / "cron-plus"
LOCK_FILE = CRON_PLUS_HOME / ".tick.lock"
PID_FILE_DIR = CRON_PLUS_HOME / "pids"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_dirs() -> None:
    CRON_PLUS_HOME.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PID_FILE_DIR.mkdir(parents=True, exist_ok=True)


def _try_acquire_lock():
    """Acquire the per-tick file lock. Returns the open file handle on
    success, None if another tick holds the lock. Uses fcntl on Unix."""
    import fcntl
    _ensure_dirs()
    fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except (OSError, IOError):
        fd.close()
        return None


def _release_lock(fd) -> None:
    if fd is None:
        return
    import fcntl
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        fd.close()


def _job_pid_file(job_id: str) -> Path:
    return PID_FILE_DIR / f"{job_id}.pid"


def _write_pid_record(job_id: str, pid: int) -> None:
    """Record PID + recorded start time in the pidfile.

    Uses JSON instead of bare int so we can defend against PID reuse on
    any OS (compare actual process create_time against our recorded
    started_at — mismatch = reused PID, treat as not-our-process).
    Backward-compatible: legacy bare-int pidfiles are still readable.
    """
    rec = {"pid": pid, "started_at": _utc_now().isoformat()}
    _job_pid_file(job_id).write_text(json.dumps(rec))


def _read_pid_record(job_id: str) -> tuple[int | None, datetime | None]:
    """Return (pid, started_at_dt) from the pidfile, or (None, None)
    if the file is missing/malformed. Tolerates the legacy bare-int
    pidfile format too."""
    pid_file = _job_pid_file(job_id)
    if not pid_file.exists():
        return None, None
    try:
        raw = pid_file.read_text().strip()
    except OSError:
        return None, None
    # Try JSON form first
    try:
        rec = json.loads(raw)
        pid = int(rec["pid"])
        started_iso = rec.get("started_at")
        started = datetime.fromisoformat(started_iso) if started_iso else None
        return pid, started
    except (ValueError, KeyError, TypeError):
        pass
    # Legacy: bare int
    try:
        return int(raw), None
    except ValueError:
        return None, None


def _process_create_time(pid: int) -> datetime | None:
    """Return the OS-reported create time for a PID, or None if not
    determinable. Tries (in order): psutil if installed, /proc on Linux,
    `ps -o lstart=` shell call as last resort."""
    # Best: psutil — cross-platform, pure Python
    try:
        import psutil  # type: ignore[import]
        try:
            return datetime.fromtimestamp(
                psutil.Process(pid).create_time(), tz=timezone.utc,
            )
        except psutil.NoSuchProcess:
            return None
        except Exception:
            pass
    except ImportError:
        pass

    # Linux fallback: /proc/<pid>/stat field 22 = starttime in clock ticks
    # since boot. Combine with /proc/stat btime to get absolute time.
    try:
        stat_data = Path(f"/proc/{pid}/stat").read_text()
        # field 22 — but careful, comm field can contain spaces wrapped in
        # parens. Split on the closing paren first.
        rparen = stat_data.rfind(")")
        rest = stat_data[rparen + 2:].split()
        starttime_jiffies = int(rest[19])  # field 22 is index 19 after comm
        with open("/proc/uptime") as f:
            uptime_s = float(f.read().split()[0])
        with open("/proc/stat") as f:
            for line in f:
                if line.startswith("btime "):
                    btime = int(line.split()[1])
                    break
            else:
                return None
        # CLK_TCK is typically 100 on Linux — try sysconf
        try:
            clk_tck = os.sysconf("SC_CLK_TCK")
        except (ValueError, OSError):
            clk_tck = 100
        process_start_unix = btime + (starttime_jiffies / clk_tck)
        return datetime.fromtimestamp(process_start_unix, tz=timezone.utc)
    except (OSError, ValueError, IndexError, KeyError):
        pass

    # Last resort: shell out to ps. Slow per check but cross-platform.
    # Note: `ps -o lstart=` emits LOCAL time, not UTC. Pre-fix tagged
    # the parse result as UTC, which on any non-UTC system shifted the
    # comparison against our recorded started_at by the local TZ offset
    # — typically 4-12 hours of skew, well beyond the 5s slack window,
    # so EVERY check on macOS-without-psutil treated the live runner as
    # a reused PID and spawned a duplicate. Now: parse as naive, attach local TZ via .astimezone(),
    # convert to UTC.
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            capture_output=True, text=True, timeout=2,
        )
        if result.returncode == 0 and result.stdout.strip():
            from email.utils import parsedate_to_datetime
            ts_str = result.stdout.strip()
            # `ps -o lstart=` default format: "Sat May  2 14:37:15 2026"
            # (single space when day-of-month is single digit). Try ctime
            # parse first; the strptime format below handles both via the
            # \s+ in the format string.
            try:
                # ctime-style parse — naive datetime in LOCAL time
                naive = datetime.strptime(ts_str, "%a %b %d %H:%M:%S %Y")
            except ValueError:
                # Some ps versions double-space the day. Try alternate.
                try:
                    naive = datetime.strptime(
                        " ".join(ts_str.split()),  # collapse runs of whitespace
                        "%a %b %d %H:%M:%S %Y",
                    )
                except ValueError:
                    # Last attempt: RFC 2822-ish via parsedate_to_datetime
                    try:
                        return parsedate_to_datetime(ts_str).astimezone(timezone.utc)
                    except (TypeError, ValueError):
                        return None
            # Attach LOCAL timezone (Python 3.6+: naive.astimezone() treats
            # naive as system local), then convert to UTC for comparison.
            local_aware = naive.astimezone()
            return local_aware.astimezone(timezone.utc)
    except (OSError, subprocess.TimeoutExpired):
        pass

    return None


def _job_is_running(job_id: str) -> bool:
    """Return True if a subprocess for this job is still alive AND is
    the one we spawned (vs a reused PID).

    PID-reuse defense: compare the OS-reported process create_time
    against the started_at recorded in the pidfile. If they don't
    match within ~5s (write/check timing slack), the PID has been
    reused — treat as not-our-process and clean up the stale pidfile.

    Cross-platform via _process_create_time (psutil → /proc → ps).
    """
    pid, recorded_start = _read_pid_record(job_id)
    if pid is None:
        return False

    pid_file = _job_pid_file(job_id)

    # Check process exists (signal 0 = test only)
    try:
        os.kill(pid, 0)
    except OSError:
        # Process gone — clean up stale pid file
        try:
            pid_file.unlink()
        except OSError:
            pass
        return False

    # If we have a recorded start time, verify it matches the process's
    # actual create_time — defense against PID reuse.
    if recorded_start is not None:
        actual_start = _process_create_time(pid)
        if actual_start is not None:
            delta = abs((actual_start - recorded_start).total_seconds())
            if delta > 5:
                # PID was reused — different process now. Clean up.
                logger.warning(
                    "cron-plus: pidfile for job %s shows pid=%d started %s, "
                    "but pid %d actually started %s — PID reused, clearing",
                    job_id, pid, recorded_start.isoformat(),
                    pid, actual_start.isoformat(),
                )
                try:
                    pid_file.unlink()
                except OSError:
                    pass
                return False
        else:
            # Could not determine actual start time on this platform.
            # Be conservative: assume the PID is ours (matches pre-fix
            # behavior on systems without /proc), but warn the operator
            # that PID-reuse defense is degraded.
            logger.debug(
                "cron-plus: cannot verify process start time for pid %d "
                "(no psutil, no /proc, ps unavailable) — assuming job %s "
                "is still running",
                pid, job_id,
            )

    return True


def _safe_log_filename(name: str, max_len: int = 80) -> str:
    """Return a filesystem-safe slug derived from ``name``.

    Replaces any character outside [A-Za-z0-9._-] with ``_`` so a malicious
    or accidental job name like ``../../etc/passwd`` cannot escape LOG_DIR.
    Also caps length to avoid filesystem limits on very long names.
    """
    import re as _re
    safe = _re.sub(r"[^A-Za-z0-9._-]", "_", str(name)) or "unnamed"
    return safe[:max_len]


def _spawn_job_subprocess(job: dict) -> subprocess.Popen | None:
    """Spawn a subprocess to run one cron-plus job. Returns the Popen
    handle (caller doesn't wait — fire-and-forget).

    Returns None if spawn fails (logged).
    """
    _ensure_dirs()  # idempotent; safe even if tick() already called it
    job_id = job["id"]
    job_name = job.get("name", job_id)
    ts = _utc_now().strftime("%Y%m%d-%H%M%S")
    safe_name = _safe_log_filename(job_name)
    log_path = LOG_DIR / f"{safe_name}-{ts}.log"
    # Defense in depth: confirm the resolved path is inside LOG_DIR.
    # _safe_log_filename should make this impossible to fail, but if a
    # symlink is in play this catches the escape.
    try:
        if not log_path.resolve().is_relative_to(LOG_DIR.resolve()):
            logger.error(
                "cron-plus: refusing to spawn job %s — log_path %s escapes LOG_DIR",
                job_id, log_path,
            )
            return None
    except (OSError, ValueError) as e:
        logger.error("cron-plus: log_path validation failed for job %s: %s", job_id, e)
        return None

    cmd = [
        sys.executable,
        "-m", "cron_plus.runner",
        "--job-id", job_id,
    ]
    env = os.environ.copy()
    # Help the runner module find the plugin package on sys.path
    plugin_dir = Path(__file__).resolve().parent.parent  # ~/.hermes/plugins
    pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{plugin_dir}:{pythonpath}" if pythonpath else str(plugin_dir)
    )
    # Suppress the inner ticker in the runner subprocess. When runner.py
    # boots Hermes' agent runtime, the plugin loader walks every enabled
    # plugin and calls register(), which would spawn a fresh daemon ticker
    # inside this short-lived subprocess. That ticker would compete for the
    # tick lock, log noisy "ticker started" lines, and (under load) could
    # claim+spawn additional due jobs before the runner exits. The
    # CRON_PLUS_DISABLED check in __init__.py:_start_ticker_thread() bails
    # out early when this is set.
    env["CRON_PLUS_DISABLED"] = "1"
    # The plugin dir contains "cron-plus" (with hyphen) — Python module names
    # can't have hyphens. Symlink trick: also expose as cron_plus via a
    # __init__.py shim. Simpler: invoke runner.py directly by path.
    runner_path = Path(__file__).parent / "runner.py"
    cmd = [sys.executable, str(runner_path), "--job-id", job_id]

    log_fd = None
    try:
        log_fd = open(log_path, "w")
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=log_fd,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
                close_fds=True,
            )
        finally:
            # Close the parent's copy of log_fd as soon as Popen has
            # dup'd it into the child. Without this, the gateway/ticker
            # leaks one fd per spawned job and eventually hits the
            # process fd limit.
            try:
                log_fd.close()
            except OSError:
                pass
            log_fd = None
        # Record PID for idempotency tracking. Runner.py overwrites this
        # with its own PID + start time on entry — what we write here is
        # the placeholder. If the write fails we MUST kill the child to
        # avoid spawning a duplicate on the next tick .
        try:
            _write_pid_record(job_id, proc.pid)
        except OSError as e:
            logger.error(
                "cron-plus: PID file write failed for job %s: %s — killing "
                "spawned child (pid=%d) to prevent duplicate on next tick",
                job_id, e, proc.pid,
            )
            try:
                proc.terminate()
                # Give it a moment to exit cleanly
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        pass
            except Exception as kill_err:
                logger.error("cron-plus: could not terminate child %d: %s",
                             proc.pid, kill_err)
            return None

        logger.info(
            "cron-plus spawned job '%s' (id=%s, pid=%d, log=%s)",
            job_name, job_id, proc.pid, log_path.name,
        )
        return proc
    except Exception as e:
        logger.error("cron-plus failed to spawn job '%s' (id=%s): %s",
                     job_name, job_id, e)
        if log_fd is not None:
            log_fd.close()
        return None


def tick() -> dict:
    """Run one scheduler tick. Returns a summary dict.

    - Acquires file lock (one tick at a time globally)
    - Loads jobs.json
    - For each due job: advance next_run, check running-job idempotency,
      Popen the subprocess, return immediately
    - Releases lock

    Tick should complete in <1s under normal conditions.
    """
    summary: dict = {"due": 0, "spawned": 0, "skipped_running": 0, "errors": 0}

    lock_fd = _try_acquire_lock()
    if lock_fd is None:
        logger.debug("cron-plus tick: another tick holds the lock; skipping")
        summary["skipped_lock_contention"] = True
        return summary

    try:
        # Atomic claim: under a single jobs.json lock, re-verify each due
        # job is still enabled, not removed, and not currently running,
        # advance next_run_at, and return the snapshot. Eliminates the
        # snapshot-then-spawn race the original tick had. (HIGH 1 fix.)
        skipped_running_count = [0]
        def _running_check(jid):
            running = _job_is_running(jid)
            if running:
                skipped_running_count[0] += 1
            return running

        claimed = jobs_mod.claim_due_jobs(is_running_check=_running_check)
        summary["due"] = len(claimed) + skipped_running_count[0]
        summary["skipped_running"] = skipped_running_count[0]
        if not claimed:
            logger.debug("cron-plus tick: no claimable jobs")
            return summary

        logger.info("cron-plus tick: %d job(s) claimed for spawn", len(claimed))

        for job in claimed:
            job_id = job["id"]
            job_name = job.get("name", job_id)

            proc = _spawn_job_subprocess(job)
            if proc is None:
                # Spawn failed AFTER claim already advanced next_run_at.
                # Mark the job failed so operators see it in CLI/state.
                #
                jobs_mod.mark_job_run(
                    job_id, success=False,
                    error=f"cron-plus: subprocess spawn failed for job '{job_name}'",
                )
                summary["errors"] += 1
            else:
                summary["spawned"] += 1

        return summary
    finally:
        _release_lock(lock_fd)


def run_ticker(interval_s: int) -> None:
    """Daemon thread entry point. Runs tick() forever every interval_s
    seconds. Exits silently on KeyboardInterrupt or SystemExit."""
    logger.info("cron-plus ticker thread starting (interval=%ds)", interval_s)
    while True:
        try:
            tick()
            time.sleep(interval_s)
        except (KeyboardInterrupt, SystemExit):
            logger.info("cron-plus ticker exiting")
            return
        except Exception as e:
            logger.error("cron-plus tick error: %s", e, exc_info=True)
            try:
                time.sleep(interval_s)
            except (KeyboardInterrupt, SystemExit):
                logger.info("cron-plus ticker exiting")
                return
