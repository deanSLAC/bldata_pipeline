#!/usr/bin/env python3
"""Log file sync module: rsyncs all log files from a flat source directory to a remote destination."""

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

    return config


def build_rsync_excludes(exclude_patterns):
    """Convert exclude_patterns list to rsync --exclude arguments."""
    args = []
    for item in exclude_patterns:
        args.extend(["--exclude", item])
    return args


def sync_logs(source_dir, dest_dir, exclude_args, delete, dry_run, logger):
    """Rsync the entire source directory to the destination. Returns True on success."""
    src = source_dir.rstrip("/") + "/"
    dst = dest_dir.rstrip("/") + "/"

    cmd = ["rsync", "-av"]
    if delete:
        cmd.append("--delete")
    if dry_run:
        cmd.append("--dry-run")
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

    if dry_run:
        logger.info("Running in dry-run mode — no changes will be made")

    if not os.path.isdir(source_dir):
        logger.error("Log source directory does not exist: %s", source_dir)
        return False

    exclude_args = build_rsync_excludes(exclude_patterns)

    return sync_logs(source_dir, dest_dir, exclude_args, delete, dry_run, logger)
