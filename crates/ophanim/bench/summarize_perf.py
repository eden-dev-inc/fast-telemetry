#!/usr/bin/env python3

import pathlib
import re
import sys


FIELDS = {
    "cycles": "cycles",
    "instructions": "instructions",
    "cache_refs": "cache-references",
    "cache_misses": "cache-misses",
    "l1_loads": "L1-dcache-loads",
    "l1_misses": "L1-dcache-load-misses",
}


def parse_perf_file(path: pathlib.Path):
    values = {key: 0 for key in FIELDS.keys()}
    text = path.read_text()
    for line in text.splitlines():
        match = re.match(r"\s*([\d,]+)\s+.*?/([^/]+)/", line)
        if not match:
            continue
        value = int(match.group(1).replace(",", ""))
        event = match.group(2)
        for key, needle in FIELDS.items():
            if event == needle:
                values[key] += value
    return values


def main() -> int:
    run_dir = pathlib.Path(sys.argv[1])
    modes = [m for m in sys.argv[2].split(",") if m]

    print("")
    print("Perf Summary (summed counters):")
    for mode in modes:
        path = run_dir / f"perf-{mode}.txt"
        if not path.exists():
            continue
        values = parse_perf_file(path)

        cycles = values["cycles"]
        instructions = values["instructions"]
        cache_refs = values["cache_refs"]
        cache_misses = values["cache_misses"]
        l1_loads = values["l1_loads"]
        l1_misses = values["l1_misses"]

        ipc = (instructions / cycles) if cycles else 0.0
        cache_miss_rate = (100.0 * cache_misses / cache_refs) if cache_refs else 0.0
        l1_miss_rate = (100.0 * l1_misses / l1_loads) if l1_loads else 0.0

        print(
            f"  {mode:6s} ipc={ipc:.3f} "
            f"cache_miss_rate={cache_miss_rate:.3f}% "
            f"l1_miss_rate={l1_miss_rate:.3f}% "
            f"cycles={cycles:,} instructions={instructions:,}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
