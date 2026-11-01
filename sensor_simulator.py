#!/usr/bin/env python3
"""Simulate temperature sensors, with optional late readings (session 22, Example 1).

Every second each sensor takes a reading, stamped with the current time, and the
readings are dropped into a folder as a CSV file for Spark to pick up. Every Nth
reading is held back, so it reaches Spark late but keeps its original timestamp.

CSV columns: sensorID, event_time, temperature, late_by
`late_by` is a teaching aid so we can spot late rows. Spark's watermark ignores it.

Examples:
    python sensor_simulator.py                         # everything on time
    python sensor_simulator.py --late-seconds 8        # late, inside the watermark
    python sensor_simulator.py --late-seconds 25       # later than the watermark allows
    python sensor_simulator.py --late-seconds 8 25     # both kinds, taking turns
"""

import argparse
import datetime as dt
import random
from pathlib import Path
from typing import List

from simulator_common import PROJECT_DIR, Event, add_late_arguments, run_simulation


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_late_arguments(parser, default_every=20)
    parser.add_argument("--duration", type=float, default=60, help="Seconds to run (default: 60)")
    parser.add_argument("--interval", type=float, default=1, help="Seconds between readings (default: 1)")
    parser.add_argument("--sensors", type=int, default=3, help="Number of sensors (default: 3)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--out", type=Path, default=PROJECT_DIR / "tmp" / "sensor_stream",
                        help="Folder Spark is watching (default: tmp/sensor_stream)")
    args = parser.parse_args()

    rng = random.Random(args.seed)

    def make_events(now: dt.datetime) -> List[Event]:
        return [{"sensorID": sensor, "event_time": now, "temperature": rng.gauss(66, 5)}
                for sensor in range(1, args.sensors + 1)]

    def describe(event: Event) -> str:
        return f"sensor {event['sensorID']} reading"

    def encode(event: Event) -> str:
        return (f"{event['sensorID']},{event['event_time']:%Y-%m-%d %H:%M:%S},"
                f"{event['temperature']:.2f},{event['late_by']}")

    run_simulation(make_events, describe, encode, args.out, "sensors", "csv",
                   args.duration, args.interval, args.late_seconds, args.late_every)


if __name__ == "__main__":
    main()
