#!/usr/bin/env python3
"""Simulate a stream of timestamped text lines, with optional late lines (session 22, Example 2).

Every two seconds the next line of a text file (looping forever) is stamped with
the current time and dropped into a folder as a JSON file for Spark to pick up.
Every Nth line is held back, so it reaches Spark late but keeps its timestamp.

JSON fields: event_time, line, late_by
`late_by` is a teaching aid so we can spot late rows. Spark's watermark ignores it.

Examples:
    python word_simulator.py                       # everything on time
    python word_simulator.py --late-seconds 12     # some lines arrive 12 seconds late
"""

import argparse
import datetime as dt
import itertools
import json
from pathlib import Path
from typing import List

from simulator_common import PROJECT_DIR, Event, add_late_arguments, run_simulation


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_late_arguments(parser, default_every=5)
    parser.add_argument("--duration", type=float, default=40, help="Seconds to run (default: 40)")
    parser.add_argument("--interval", type=float, default=2, help="Seconds between lines (default: 2)")
    parser.add_argument("--text", type=Path, default=PROJECT_DIR / "data" / "Safadi_poem.txt",
                        help="Text file to read lines from (default: data/Safadi_poem.txt)")
    parser.add_argument("--out", type=Path, default=PROJECT_DIR / "tmp" / "word_stream",
                        help="Folder Spark is watching (default: tmp/word_stream)")
    args = parser.parse_args()

    with open(args.text, encoding="utf-8") as f:
        lines = itertools.cycle([line.strip() for line in f if line.strip()])

    def make_events(now: dt.datetime) -> List[Event]:
        return [{"event_time": now, "line": next(lines)}]

    def describe(event: Event) -> str:
        return f'line "{event["line"][:30]}"'

    def encode(event: Event) -> str:
        return json.dumps({"event_time": f"{event['event_time']:%Y-%m-%d %H:%M:%S}",
                           "line": event["line"], "late_by": event["late_by"]})

    run_simulation(make_events, describe, encode, args.out, "words", "json",
                   args.duration, args.interval, args.late_seconds, args.late_every)


if __name__ == "__main__":
    main()
