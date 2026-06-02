#!/usr/bin/env python3
"""
download_data.py
----------------
Downloads Danish AIS CSV files for December 2021 from the Danish Maritime
Authority public FTP/HTTP mirror at https://web.ais.dk/aisdata/

Files are named:  aisdk-2021-12-DD.csv
There are 31 files (one per day).

Usage:
    python download_data.py [--data-dir /app/data]
"""

import argparse
import os
import sys
import time
import logging
import urllib.request
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL  = "https://web.ais.dk/aisdata"
START     = date(2021, 12, 1)
END       = date(2021, 12, 31)


def download_day(day: date, data_dir: str, force: bool = False) -> bool:
    filename = f"aisdk-{day.strftime('%Y-%m-%d')}.csv"
    url      = f"{BASE_URL}/{filename}"
    dest     = os.path.join(data_dir, filename)

    if os.path.exists(dest) and not force:
        log.info("Already downloaded: %s (skip)", filename)
        return True

    log.info("Downloading %s …", url)
    try:
        urllib.request.urlretrieve(url, dest)
        size_mb = os.path.getsize(dest) / 1_048_576
        log.info("  → saved %s  (%.1f MB)", dest, size_mb)
        return True
    except Exception as exc:
        log.error("  ✗ Failed to download %s: %s", url, exc)
        if os.path.exists(dest):
            os.remove(dest)
        return False


def main():
    parser = argparse.ArgumentParser(description="Download December 2021 AIS data")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "/app/data"),
                        help="Directory to save CSV files")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if files already exist")
    args = parser.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    log.info("Saving to: %s", args.data_dir)

    current = START
    failed  = []
    while current <= END:
        ok = download_day(current, args.data_dir, force=args.force)
        if not ok:
            failed.append(current)
        time.sleep(0.5)  # polite pause between requests
        current += timedelta(days=1)

    if failed:
        log.error("Failed to download %d file(s): %s", len(failed),
                  [d.isoformat() for d in failed])
        sys.exit(1)
    else:
        log.info("All 31 files downloaded successfully.")


if __name__ == "__main__":
    main()
