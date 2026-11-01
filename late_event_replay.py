#!/usr/bin/env python3
"""Replay a fixed, scripted timeline of sensor events (session 22, Example 3).

The other simulators use the real clock, so each run differs. This one always
sends the same events in the same order, one file per batch, so every outcome can
be predicted on paper and checked. The timestamps are fixed, which is why the
notebook must clear the folder before each replay.

CSV columns: sensorID, event_time, temperature

Examples:
    python late_event_replay.py            # a new batch every 8 seconds
    python late_event_replay.py --step     # press Enter for each batch, to narrate
    python late_event_replay.py --no-late  # same events, delivered in time order
"""

import argparse
import datetime as dt
import time
from pathlib import Path
from typing import List, NamedTuple

from simulator_common import PROJECT_DIR, write_file

BASE = dt.datetime(2026, 1, 1, 12, 0, 0)


class Reading(NamedTuple):
    second: int  # event time, as seconds after 12:00:00
    temperature: float
    note: str = ""  # label for the notebook's chart, not sent to Spark

    @property
    def event_time(self) -> dt.datetime:
        return BASE + dt.timedelta(seconds=self.second)


# Each inner list is one file, and the notebook reads one file per micro-batch.
TIMELINE: List[List[Reading]] = [
    [Reading(1, 70.0), Reading(2, 72.0), Reading(3, 68.0)],
    [Reading(7, 71.0), Reading(12, 73.0)],
    [Reading(4, 75.0, "out of order, accepted"), Reading(21, 74.0)],
    [Reading(2, 99.9, "window closed, dropped"), Reading(10, 69.0, "window open, accepted"),
     Reading(26, 76.0)],
    [Reading(40, 77.0)],
]


def in_time_order() -> List[List[Reading]]:
    """The same readings and batch sizes, but with nothing out of order."""
    readings = iter(sorted((r for batch in TIMELINE for r in batch), key=lambda r: r.second))
    return [[next(readings) for _ in batch] for batch in TIMELINE]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-late", action="store_true",
                        help="Deliver every event in time order, so nothing is late")
    parser.add_argument("--pause", type=float, default=8,
                        help="Seconds between batches (default: 8). Keep it above 2, so that Spark "
                             "has time to close windows between batches, as the notebook explains.")
    parser.add_argument("--step", action="store_true", help="Wait for Enter before each batch")
    parser.add_argument("--out", type=Path, default=PROJECT_DIR / "tmp" / "late_replay_stream",
                        help="Folder Spark is watching (default: tmp/late_replay_stream)")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    timeline = in_time_order() if args.no_late else TIMELINE

    for number, batch in enumerate(timeline, start=1):
        if args.step:
            input(f"Press Enter to send batch {number} ...")
        elif number > 1:
            time.sleep(args.pause)
        rows = [f"1,{r.event_time:%Y-%m-%d %H:%M:%S},{r.temperature}" for r in batch]
        write_file(args.out, f"batch_{number:02d}.csv", "\n".join(rows) + "\n")
        print(f"Batch {number}: event times " + ", ".join(f"{r.event_time:%H:%M:%S}" for r in batch))
    print("Replay finished.")


if __name__ == "__main__":
    main()
