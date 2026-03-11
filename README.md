# BL Data Pipeline

Automated data sync tool that rsyncs experiment folders from a source directory to a destination directory.

## Setup

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install pyyaml
   ```

2. Edit `config.yaml` with your source/destination paths and any exclusions.

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

## Configuration

Edit `config.yaml`:

- **source_dir** — Directory containing experiment folders
- **dest_dir** — Destination to sync to
- **log_file** — Path for log output (default: `sync.log`)
- **exclusions** — Comma-separated list of items to exclude from sync:
  - Full experiment folder names (e.g. `2025-03_Smith`)
  - Subfolder paths (e.g. `raw_data/temp`)
  - Specific filenames (e.g. `notes.txt`)
  - Wildcard patterns (e.g. `*.tmp`)

## Folder Detection

Only folders matching the pattern `YYYY-mm_<ExperimenterName>` (e.g. `2025-03_Smith`) are synced. All other folders in the source directory are ignored.
