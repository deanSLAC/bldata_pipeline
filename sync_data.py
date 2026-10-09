#!/usr/bin/env python3
"""Data sync module: rsyncs experiment folders (YYYY-mm_ExperimenterName) from source to destination."""

import glob
import os
import re
import subprocess

import yaml

from rsync_ssh import build_rsync_ssh

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.yaml")

# Matches folders like 2025-03_Smith or 2024-11_Jane_Doe
EXPERIMENT_PATTERN = re.compile(r"^\d{4}-\d{2}_.+$")


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
    config.setdefault("chmod", "")
    config.setdefault("chown", "")
    config.setdefault("ssh_key", "")
    # One rsync now carries every folder, so the timeout covers the whole batch
    # rather than a single folder. The old per-folder value was 300.
    config.setdefault("rsync_timeout", 900)

    return config


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


_SUMMARY_PREFIXES = (
    "sending", "sent ", "total size", "created directory", "receiving",
    "building file list", "delta-transmission",
)


def _per_folder_counts(stdout):
    """Group rsync -av --relative output lines by their top-level folder.

    --files-from implies --relative, so every line rsync prints is already
    prefixed with the folder it belongs to (e.g. "2026-03_Smith/scan_01.raw").
    That lets one batched rsync still produce the per-folder log lines this
    pipeline has always written.
    """
    counts = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line or line == "./" or line.startswith(_SUMMARY_PREFIXES):
            continue
        folder = line.split("/", 1)[0]
        if folder:
            counts[folder] = counts.get(folder, 0) + 1
    return counts


def sync_folders(source_dir, dest_dir, folder_names, exclude_args, delete, dry_run,
                 logger, chmod="", chown="", ssh_args=None, timeout=900):
    """Rsync every experiment folder in ONE rsync invocation. True on success.

    This used to be one rsync -- and therefore one SSH connection, one
    authentication and one remote directory enumeration -- per folder. With 56
    folders on a 60-second cron that was 80,640 connections a day, and measured
    at ~780 ms per folder it was most of each tick's 43 seconds even when
    nothing had changed.

    The folder list goes to rsync over --files-from, which preserves the
    selection logic in find_experiment_folders() and run() exactly: globs, the
    YYYY-mm_ pattern and exclude_folders all still decide what is in the list.
    --files-from implies --relative, so a folder named in the list lands at
    <dest_dir>/<folder>/ just as it did before. The list is NUL-delimited
    (--from0) so no folder name can be misread.
    """
    src = source_dir.rstrip("/") + "/"
    dst = dest_dir.rstrip("/") + "/"

    # -r must be explicit: --files-from implies --dirs, which overrides the -r
    # that -a would otherwise supply. Without it rsync creates the folders at
    # the destination, transfers nothing into them, and exits 0.
    cmd = ["rsync", "-av", "-r", "--files-from=-", "--from0"]
    if ssh_args:
        cmd += ssh_args
    if delete:
        cmd.append("--delete")
    if dry_run:
        cmd.append("--dry-run")
    if chmod:
        cmd.append(f"--chmod={chmod}")
    if chown:
        cmd.append(f"--chown={chown}")
    cmd += exclude_args + [src, dst]

    payload = "".join(name + "\0" for name in folder_names)

    try:
        result = subprocess.run(cmd, input=payload, capture_output=True,
                                text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.error("rsync timed out after %d seconds (%d folder(s))",
                     timeout, len(folder_names))
        return False

    if result.returncode != 0:
        logger.error("rsync failed (exit %d): %s",
                     result.returncode, result.stderr.strip())
        return False

    prefix = "[DRY RUN] " if dry_run else ""
    counts = _per_folder_counts(result.stdout)
    for folder in folder_names:
        if folder in counts:
            logger.info("%sSynced %s (%d item(s) transferred)",
                        prefix, folder, counts[folder])
    return True


def run(logger):
    """Run the data sync. Returns True on success, False on failure."""
    config = load_config()

    source_dir = config["source_dir"]
    dest_dir = config["dest_dir"]
    exclude_folders = config["exclude_folders"]
    exclude_patterns = config["exclude_patterns"]
    delete = config["delete"]
    dry_run = config["dry_run"]
    chmod = config["chmod"]
    chown = config["chown"]
    ssh_args = build_rsync_ssh(config["ssh_key"])

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
        return True

    exclude_args = build_rsync_excludes(exclude_patterns)

    ok = sync_folders(source_dir, dest_dir, folders, exclude_args, delete, dry_run,
                      logger, chmod, chown, ssh_args, config["rsync_timeout"])

    if not ok:
        logger.warning("Data sync failed for the batch of %d folder(s)", len(folders))
        return False

    logger.info("Data sync completed successfully (%d folder(s))", len(folders))
    return True
