#!/usr/bin/env python3
"""Data sync script: rsyncs experiment folders (YYYY-mm_ExperimenterName) from source to destination."""

import atexit
import glob
import logging
import os
import re
import subprocess
import sys

import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.yaml")
LOCK_PATH = os.path.join(SCRIPT_DIR, ".sync.lock")

# Matches folders like 2025-03_Smith or 2024-11_Jane_Doe
EXPERIMENT_PATTERN = re.compile(r"^\d{4}-\d{2}_.+$")


def acquire_lock():
    """Create a lock file to prevent overlapping runs. Returns True if acquired."""
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        atexit.register(release_lock)
        return True
    except FileExistsError:
        return False


def release_lock():
    """Remove the lock file."""
    try:
        os.remove(LOCK_PATH)
    except FileNotFoundError:
        pass


def load_config():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    required = ["source_dir", "dest_dir"]
    for key in required:
        if not config.get(key):
            raise ValueError(f"Missing required config key: {key}")

    # Normalize list fields
    config["exclude_folders"] = config.get("exclude_folders") or []
    config["exclude_patterns"] = config.get("exclude_patterns") or []
    config.setdefault("delete", False)
    config.setdefault("dry_run", False)

    return config


def setup_logging(log_file):
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


def find_experiment_folders(source_dir):
    """Return (base_dir, folder_names) for experiment dirs matching YYYY-mm_<Name> pattern.

    source_dir can contain glob wildcards (e.g. /data/2026*). When a glob is
    present, matched directories are filtered by the experiment pattern and the
    base directory is derived from the non-glob prefix of the path.
    """
    has_glob = any(c in source_dir for c in ("*", "?", "["))

    if has_glob:
        # Derive the base directory from the non-glob prefix
        # e.g. "/data/experiments/2026*" → base="/data/experiments"
        parts = source_dir.split(os.sep)
        base_parts = []
        for part in parts:
            if any(c in part for c in ("*", "?", "[")):
                break
            base_parts.append(part)
        base_dir = os.sep.join(base_parts) or os.sep

        if not os.path.isdir(base_dir):
            raise FileNotFoundError(f"Base source directory does not exist: {base_dir}")

        matches = sorted(glob.glob(source_dir))
        folders = []
        for path in matches:
            if os.path.isdir(path):
                name = os.path.basename(path)
                if EXPERIMENT_PATTERN.match(name):
                    folders.append(name)
        return base_dir, folders
    else:
        if not os.path.isdir(source_dir):
            raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

        folders = []
        for entry in sorted(os.listdir(source_dir)):
            full_path = os.path.join(source_dir, entry)
            if os.path.isdir(full_path) and EXPERIMENT_PATTERN.match(entry):
                folders.append(entry)
        return source_dir, folders


def build_rsync_excludes(exclude_patterns):
    """Convert exclude_patterns list to rsync --exclude arguments."""
    args = []
    for item in exclude_patterns:
        args.extend(["--exclude", item])
    return args


def sync_folder(source_dir, dest_dir, folder_name, exclude_args, delete, dry_run, logger):
    """Rsync a single experiment folder. Returns True on success."""
    src = os.path.join(source_dir, folder_name) + "/"
    dst = os.path.join(dest_dir, folder_name) + "/"

    cmd = ["rsync", "-av"]
    if delete:
        cmd.append("--delete")
    if dry_run:
        cmd.append("--dry-run")
    cmd += exclude_args + [src, dst]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error("rsync failed for %s (exit %d): %s", folder_name, result.returncode, result.stderr.strip())
        return False

    # Only log transferred files (skip blank lines and summary)
    transferred = [
        line for line in result.stdout.strip().splitlines()
        if line and not line.startswith("sending") and not line.startswith("sent ")
        and not line.startswith("total size") and line != "./"
    ]
    if transferred:
        prefix = "[DRY RUN] " if dry_run else ""
        logger.info("%sSynced %s (%d item(s) transferred)", prefix, folder_name, len(transferred))
    return True


def main():
    config = load_config()
    logger = setup_logging(config.get("log_file", "sync.log"))

    if not acquire_lock():
        logger.warning("Another sync is already running (lock file exists: %s). Exiting.", LOCK_PATH)
        sys.exit(0)

    source_dir = config["source_dir"]
    dest_dir = config["dest_dir"]
    exclude_folders = config["exclude_folders"]
    exclude_patterns = config["exclude_patterns"]
    delete = config["delete"]
    dry_run = config["dry_run"]

    if dry_run:
        logger.info("Running in dry-run mode — no changes will be made")

    # find_experiment_folders returns the resolved base dir (handles globs)
    source_dir, all_folders = find_experiment_folders(source_dir)
    folders = [f for f in all_folders if f not in exclude_folders]
    skipped = [f for f in all_folders if f in exclude_folders]

    if skipped:
        logger.info("Skipping excluded experiment folders: %s", ", ".join(skipped))

    if not folders:
        logger.info("No experiment folders to sync in %s", source_dir)
        return

    exclude_args = build_rsync_excludes(exclude_patterns)

    errors = 0
    for folder in folders:
        if not sync_folder(source_dir, dest_dir, folder, exclude_args, delete, dry_run, logger):
            errors += 1

    if errors:
        logger.warning("Sync completed with %d error(s) out of %d folder(s)", errors, len(folders))
    else:
        logger.info("Sync completed successfully (%d folder(s))", len(folders))


if __name__ == "__main__":
    main()
