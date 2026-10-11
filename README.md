# BL Data Pipeline

> **Inactive.** Replaced on 2026-10-10 by
> [`beamline_syncer`](https://github.com/slaclab/beamline_syncer), which now
> runs the BL15-2 (bl152lx1) and BL4-1 (bl41lx1) syncs. It does everything this
> pipeline did, from the same kind of config. Kept for reference only.

Automated data sync tool that rsyncs experiment folders from a source directory to a destination directory.

## Setup

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Copy the example config and edit with your paths:
   ```bash
   cp config.yaml.example config.yaml
   ```

3. Run manually:
   ```bash
   python sync.py
   ```

4. Or install as a cron job (runs every minute):
   ```bash
   crontab -e
   # Add:
   * * * * * /path/to/venv/bin/python /path/to/sync.py
   ```
   A lock file prevents overlapping runs, so it's safe to schedule frequently.

## Configuration

Edit `config.yaml`:

- **source_dir** — Directory containing experiment folders (supports glob wildcards)
- **dest_dir** — Remote rsync destination (e.g. `user@host:/path/to/destination`)
- **log_file** — Path for log output (default: `sync.log`)
- **delete** — When `true`, rsync `--delete` removes destination files not in source (default: `false`)
- **dry_run** — When `true`, rsync performs a trial run with no changes (default: `false`)
- **exclude_folders** — List of experiment folder names to skip entirely
- **exclude_patterns** — List of rsync exclude patterns applied within each synced folder
- **ssh_key** — Private key for the rsync transport (see *SSH transport* below)
- **rsync_timeout** — Seconds before a sync is abandoned (default: `900`)

## Folder Detection

Only folders matching the pattern `YYYY-mm_<ExperimenterName>` (e.g. `2025-03_Smith`) are synced. All other folders in the source directory are ignored.

## How the transfer is batched

All selected folders move in **one** rsync invocation, via `--files-from` on stdin.
Folder selection is unchanged — globs, the `YYYY-mm_` pattern and `exclude_folders`
all still decide what goes in the list — and each folder still lands at
`<dest_dir>/<folder>/`, because `--files-from` implies `--relative`.

This used to be one rsync per folder, which meant one SSH connection, one
authentication and one remote directory enumeration per folder. At 56 folders on a
60-second cron that was **80,640 connections per day**, and measured at ~780 ms per
folder it was most of each tick's 43 seconds even when nothing had changed.

Two things about this are load-bearing if you edit `sync_folders()`:

- **`-r` must stay explicit.** `--files-from` implies `--dirs`, which overrides the
  `-r` that `-a` would otherwise supply. Drop the explicit `-r` and rsync creates
  every folder at the destination, transfers nothing into them, and exits 0.
- **The list stays NUL-delimited** (`--from0`), so no folder name can be misread.

`rsync_timeout` now covers the whole batch rather than a single folder, which is why
its default is 900 rather than the old per-folder 300.

## SSH transport

`rsync_ssh.py` holds the transport settings for **both** the data sync and the log
sync. They target the same host with the same key, so they share one connection and
the log sync authenticates zero extra times. Keep it in that one module — the two
halves having their own copy is how they drift apart.

- `IdentitiesOnly=yes`, `PreferredAuthentications=publickey` and
  `GSSAPIAuthentication=no` reduce each connection to **exactly one** authentication
  attempt. The DTN also offers `gssapi-keyex`, `gssapi-with-mic`, `password` and
  `keyboard-interactive`, and every method the client tries counts against the
  server's `MaxAuthTries`. That is the likely source of the
  `Too many authentication failures` disconnects that dominate `sync.log`.
- `BatchMode=yes` + `ConnectTimeout=30` make a broken transport **fail instead of
  hang**. Under cron nobody can answer a password or host-key prompt, so without
  them a rejected key or unreachable DTN sits until the rsync timeout, holding the
  lock the whole time.
- `ControlMaster=auto` + `ControlPersist=180` put every rsync in a run — and, because
  the persist window is longer than the cron interval, consecutive runs too — on
  **one TCP connection with one authentication**. At 0.31 ms RTT to the DTN a single
  TCP flow is not a throughput constraint.

If the control socket is ever left stale, ssh logs
`cannot bind to path ...: Address already in use` and falls back to an ordinary
unmultiplexed connection, so syncs keep working. Delete `.mux-*` to restore
multiplexing.

## Log rotation

`sync.log` has no rotation and has reached ~598 MB / 9.4M lines, of which 428,671 are
DTN auth-failure noise. Worth an `logrotate` entry.
