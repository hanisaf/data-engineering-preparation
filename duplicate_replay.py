#!/usr/bin/env python3
"""Replay a fixed, scripted timeline of sensor readings with retries (session 22, Example 4).

Sensors resend a reading when they are not sure it got through, so the same
reading can reach Spark more than once. There are 7 different readings here,
r1 to r7, and the duplicates come in two kinds:

    exact copy     same readingID, same event_time
    re-stamped     same readingID, but the sensor put a new time on the retry

Timestamps are fixed, so the notebook must clear the folder before each replay.

CSV columns: readingID, sensorID, event_time, temperature

Examples:
    python duplicate_replay.py                   # a new batch every 6 seconds
    python duplicate_replay.py --step            # press Enter for each batch, to narrate
    python duplicate_replay.py --no-duplicates   # the same readings, no retries
"""

import argparse
import datetime as dt
import time
from pathlib import Path
from typing import List, NamedTuple

from simulator_common import PROJECT_DIR, write_file

BASE = dt.datetime(2026, 1, 1, 12, 0, 0)


class Reading(NamedTuple):
    reading_id: str
    second: int  # event time, as seconds after 12:00:00
    temperature: float
    is_retry: bool = False

    @property
    def event_time(self) -> dt.datetime:
        return BASE + dt.timedelta(seconds=self.second)


# Each inner list is one file, and the notebook reads one file per micro-batch.
TIMELINE: List[List[Reading]] = [
    [Reading("r1", 1, 70.0), Reading("r2", 2, 72.0), Reading("r3", 3, 68.0)],
    [Reading("r2", 2, 72.0, True), Reading("r4", 7, 71.0)],              # exact copy of r2
    [Reading("r3", 6, 68.0, True), Reading("r5", 12, 73.0)],             # r3 re-stamped soon after
    [Reading("r6", 31, 74.0)],                                           # moves the watermark on
    [Reading("r1", 33, 70.0, True), Reading("r4", 7, 71.0, True),        # r1 re-stamped much later,
     Reading("r7", 36, 75.0)],                                           # r4 exact copy much later
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-duplicates", action="store_true",
                        help="Leave out every retry, so each reading arrives once")
    parser.add_argument("--pause", type=float, default=6,
                        help="Seconds between batches (default: 6). Keep it above 2, so that Spark "
                             "has time to clear old state between batches.")
    parser.add_argument("--step", action="store_true", help="Wait for Enter before each batch")
    parser.add_argument("--out", type=Path, default=PROJECT_DIR / "tmp" / "duplicate_stream",
                        help="Folder Spark is watching (default: tmp/duplicate_stream)")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    for number, batch in enumerate(TIMELINE, start=1):
        if args.step:
            input(f"Press Enter to send batch {number} ...")
        elif number > 1:
            time.sleep(args.pause)
        readings = [r for r in batch if not (args.no_duplicates and r.is_retry)]
        rows = [f"{r.reading_id},1,{r.event_time:%Y-%m-%d %H:%M:%S},{r.temperature}" for r in readings]
        write_file(args.out, f"batch_{number:02d}.csv", "\n".join(rows) + "\n")
        print(f"Batch {number}: " + ", ".join(
            f"{r.reading_id} at {r.event_time:%H:%M:%S}{' (retry)' if r.is_retry else ''}"
            for r in readings))
    print("Replay finished.")


if __name__ == "__main__":
    main()
