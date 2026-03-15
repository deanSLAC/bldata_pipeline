#!/usr/bin/env python3
"""Main sync entry point: runs data sync, then log sync."""

import atexit
import logging
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCK_PATH = os.path.join(SCRIPT_DIR, ".sync.lock")


def _is_pid_alive(pid):
    """Check if a process with the given PID is still running."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_lock():
    """Create a lock file to prevent overlapping runs. Returns True if acquired."""
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        atexit.register(release_lock)
        return True
    except FileExistsError:
        try:
            with open(LOCK_PATH) as f:
                old_pid = int(f.read().strip())
            if not _is_pid_alive(old_pid):
                os.remove(LOCK_PATH)
                return acquire_lock()
        except (ValueError, OSError):
            os.remove(LOCK_PATH)
            return acquire_lock()
        return False


def release_lock():
    """Remove the lock file."""
    try:
        os.remove(LOCK_PATH)
    except FileNotFoundError:
        pass


def setup_logging(log_file="sync.log"):
    log_path = log_file if os.path.isabs(log_file) else os.path.join(SCRIPT_DIR, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("sync")


def main():
    logger = setup_logging()

    if not acquire_lock():
        logger.warning("Another sync is already running (lock file exists: %s). Exiting.", LOCK_PATH)
        sys.exit(2)

    # --- Data sync ---
    import sync_data
    logger.info("Starting data sync")
    data_ok = sync_data.run(logger)

    # --- Log sync ---
    log_ok = True
    try:
        from bllogs_pipeline import sync_logs
        logger.info("Starting log sync")
        log_ok = sync_logs.run(logger)
    except Exception as e:
        logger.warning("Log sync skipped: %s", e)
        log_ok = False

    # --- Summary ---
    if not data_ok or not log_ok:
        if not data_ok:
            logger.error("Data sync failed")
        if not log_ok:
            logger.error("Log sync failed")
        sys.exit(1)

    logger.info("All syncs completed successfully")


if __name__ == "__main__":
    main()
