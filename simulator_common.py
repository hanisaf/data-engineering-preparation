"""Shared plumbing for sensor_simulator.py, word_simulator.py, late_event_replay.py.

Each simulator drops small files into a folder that a Spark file stream watches.
Lateness is created the way it happens in real life: an event gets its timestamp
when it happens, but the file carrying it reaches Spark only later.
"""

import argparse
import datetime as dt
import os
import time
from pathlib import Path
from typing import Callable, Dict, List

PROJECT_DIR = Path(__file__).resolve().parent
Event = Dict[str, object]


def utc_now() -> dt.datetime:
    """Current UTC time, whole seconds, no timezone (the notebook runs Spark in UTC)."""
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)


def clock(moment: dt.datetime) -> str:
    return moment.strftime("%H:%M:%S")


def write_file(out_dir: Path, name: str, text: str) -> None:
    """Write atomically: Spark ignores dot-files, so it never sees a half-written file."""
    final_path = out_dir / name
    tmp_path = out_dir / f".{name}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(text)
    os.utime(tmp_path, None)
    os.replace(tmp_path, final_path)


def add_late_arguments(parser: argparse.ArgumentParser, default_every: int) -> None:
    parser.add_argument(
        "--late-seconds", type=int, nargs="*", default=[], metavar="S",
        help="How late the held-back events arrive. Give several values to take "
             "turns, e.g. --late-seconds 8 25. Default: no late data.")
    parser.add_argument(
        "--late-every", type=int, default=default_every, metavar="N",
        help=f"Hold back every Nth event (default: {default_every}).")


def run_simulation(
    make_events: Callable[[dt.datetime], List[Event]],
    describe: Callable[[Event], str],
    encode: Callable[[Event], str],
    out_dir: Path,
    prefix: str,
    extension: str,
    duration: float,
    interval: float,
    late_seconds: List[int],
    late_every: int,
) -> None:
    """Emit one file per tick, holding every Nth event back until it is due.

    After `duration` no new event is held back, but on-time events keep flowing
    until the last late one is delivered. This matters: the watermark only moves
    when newer events arrive, so if the stream went quiet first, a "too late"
    event would meet a frozen watermark and be accepted.
    """
    if late_every < 1:
        raise SystemExit("--late-every must be 1 or more")
    out_dir.mkdir(parents=True, exist_ok=True)
    plan =f"late every {late_every} events by {late_seconds}s" if late_seconds else "no late data"
    print(f"Writing to {out_dir}: {plan}, for {duration:g}s. All times are UTC. Ctrl+C to stop.")

    held = []  # (due_time, event)
    generated = sent = held_total = 0
    start = time.monotonic()
    tick = 0
    try:
        while True:
            elapsed = time.monotonic() - start
            running = elapsed < duration
            if not running and not held:
                break

            now = utc_now()
            arriving = []
            for event in make_events(now):
                generated += 1
                if running and late_seconds and generated % late_every == 0:
                    delay = late_seconds[held_total % len(late_seconds)]
                    held_total += 1
                    event["late_by"] = delay
                    held.append((now + dt.timedelta(seconds=delay), event))
                    print(f"{clock(now)}  HOLD  {describe(event)}: will arrive {delay}s late")
                else:
                    event["late_by"] = 0
                    arriving.append(event)

            for due_time, event in [item for item in held if item[0] <= now]:
                held.remove((due_time, event))
                event["late_by"] = int((now - event["event_time"]).total_seconds())
                arriving.append(event)
                print(f"{clock(now)}  LATE  {describe(event)}: "
                      f"happened {clock(event['event_time'])}, arriving {event['late_by']}s late")

            if arriving:
                body = "".join(encode(event) + "\n" for event in arriving)
                write_file(out_dir, f"{prefix}_{int(time.time())}_{tick:05d}.{extension}", body)
                sent += len(arriving)
            print(f"{clock(now)}  sent {len(arriving)} event(s), {len(held)} held back")

            tick += 1
            time.sleep(max(0.0, start + tick * interval - time.monotonic()))
    except KeyboardInterrupt:
        print("\nStopped early.")
    print(f"Done: {sent} events sent, {held_total} of them were held back as late.")
