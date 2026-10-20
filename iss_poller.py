#!/usr/bin/env python3
"""Poll the ISS current-location API and save each response as <unixtimestamp>.json.

Files are written atomically for Spark textFileStream: the data goes to a hidden
temp file (dot prefix, ignored by Spark) in the same directory, gets a fresh
mtime, and is then moved to its final name.
"""

import argparse
import json
import os
import time
import urllib.request
from pathlib import Path

API_URL = "http://api.open-notify.org/iss-now.json"


def fetch(url: str, timeout: float = 10.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def save(data: dict, out_dir: Path) -> Path:
    # Use the API's own timestamp; fall back to local time if missing
    ts = int(data.get("timestamp", time.time()))
    final_path = out_dir / f"{ts}.json"
    tmp_path = out_dir / f".{ts}.json.tmp"  # leading dot -> ignored by Spark

    # One JSON object per line: textFileStream reads line by line
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(data, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())

    os.utime(tmp_path, None)          # fresh mtime so Spark sees it as new
    os.replace(tmp_path, final_path)  # atomic move within the same file system
    return final_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path, nargs="?", default=Path("tmp"),
                        help="Directory to write JSON files into (default: tmp)")
    parser.add_argument("interval", type=float, nargs="?", default=5.0,
                        help="Seconds between pulls (default: 5)")
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("interval must be greater than 0")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Polling {API_URL} every {args.interval}s -> {args.output_dir.resolve()}")

    try:
        while True:
            start = time.monotonic()
            try:
                path = save(fetch(API_URL), args.output_dir)
                print(f"Saved {path.name}")
            except Exception as exc:
                print(f"Fetch failed: {exc}")
            # Keep a steady cadence regardless of how long the request took
            time.sleep(max(0.0, args.interval - (time.monotonic() - start)))
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()