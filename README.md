# BL Data Pipeline

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

## Folder Detection

Only folders matching the pattern `YYYY-mm_<ExperimenterName>` (e.g. `2025-03_Smith`) are synced. All other folders in the source directory are ignored.
