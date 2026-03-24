#!/usr/bin/env python3
"""Log file sync module: rsyncs log files from a source directory (with optional glob) to a remote destination."""

import glob
import os
import subprocess

import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.yaml")


def load_config():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    required = ["source_dir", "dest_dir"]
    for key in required:
        if not config.get(key):
            raise ValueError(f"Missing required config key: {key}")

    config["exclude_patterns"] = config.get("exclude_patterns") or []
    config.setdefault("delete", False)
    config.setdefault("dry_run", False)
    config.setdefault("chmod", "")
    config.setdefault("chown", "")

    return config


def resolve_sources(source_dir):
    """Resolve source_dir, expanding globs if present.

    Returns (base_dir, matched_paths) where matched_paths is None if no glob
    (meaning sync the whole directory), or a list of expanded paths.
    """
    has_glob = any(c in source_dir for c in ("*", "?", "["))

    if not has_glob:
        if not os.path.isdir(source_dir):
            raise FileNotFoundError(f"Log source directory does not exist: {source_dir}")
        return source_dir, None

    # Derive base directory from non-glob prefix
    parts = source_dir.split(os.sep)
    base_parts = []
    for part in parts:
        if any(c in part for c in ("*", "?", "[")):
            break
        base_parts.append(part)
    base_dir = os.sep.join(base_parts) or os.sep

    if not os.path.isdir(base_dir):
        raise FileNotFoundError(f"Base log source directory does not exist: {base_dir}")

    matched = sorted(glob.glob(source_dir))
    return base_dir, matched


def build_rsync_excludes(exclude_patterns):
    """Convert exclude_patterns list to rsync --exclude arguments."""
    args = []
    for item in exclude_patterns:
        args.extend(["--exclude", item])
    return args


def sync_logs(source_dir, dest_dir, exclude_args, delete, dry_run, logger, matched_paths=None, chmod="", chown=""):
    """Rsync log files to the destination. Returns True on success.

    If matched_paths is None, syncs the entire source_dir.
    If matched_paths is a list, syncs only those specific paths.
    """
    dst = dest_dir.rstrip("/") + "/"

    cmd = ["rsync", "-av"]
    if delete:
        cmd.append("--delete")
    if dry_run:
        cmd.append("--dry-run")
    if chmod:
        cmd.append(f"--chmod={chmod}")
    if chown:
        cmd.append(f"--chown={chown}")

    if matched_paths is not None:
        cmd += exclude_args + matched_paths + [dst]
    else:
        src = source_dir.rstrip("/") + "/"
        cmd += exclude_args + [src, dst]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        logger.error("rsync timed out after 5 minutes")
        return False

    if result.returncode != 0:
        logger.error("rsync failed (exit %d): %s", result.returncode, result.stderr.strip())
        return False

    transferred = [
        line for line in result.stdout.strip().splitlines()
        if line and not line.startswith("sending") and not line.startswith("sent ")
        and not line.startswith("total size") and line != "./"
    ]
    if transferred:
        prefix = "[DRY RUN] " if dry_run else ""
        logger.info("%sSynced %d log item(s)", prefix, len(transferred))
    else:
        logger.info("No new log files to transfer")

    return True


def run(logger):
    """Run the log sync. Returns True on success, False on failure."""
    config = load_config()

    source_dir = config["source_dir"]
    dest_dir = config["dest_dir"]
    exclude_patterns = config["exclude_patterns"]
    delete = config["delete"]
    dry_run = config["dry_run"]
    chmod = config["chmod"]
    chown = config["chown"]

    if dry_run:
        logger.info("Running in dry-run mode — no changes will be made")

    try:
        base_dir, matched_paths = resolve_sources(source_dir)
    except FileNotFoundError as e:
        logger.error(str(e))
        return False

    if matched_paths is not None and not matched_paths:
        logger.info("No log files matched pattern: %s", source_dir)
        return True

    exclude_args = build_rsync_excludes(exclude_patterns)

    return sync_logs(base_dir, dest_dir, exclude_args, delete, dry_run, logger, matched_paths, chmod, chown)
