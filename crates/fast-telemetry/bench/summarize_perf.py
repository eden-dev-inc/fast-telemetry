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
        match = re.match(r"\s*([\d,]+)\s+([^\s#]+)", line)
        if not match:
            continue
        value = int(match.group(1).replace(",", ""))
        event = match.group(2).strip()
        event_name = event.split("/")[-2] if "/" in event else event
        for key, needle in FIELDS.items():
            if event_name == needle:
                values[key] += value
    return values


def parse_run_totals(path: pathlib.Path):
    values = {"total_ops": 0, "total_counter_writes": 0}
    if not path.exists():
        return values

    for line in path.read_text().splitlines():
        if line.startswith("total_ops="):
            values["total_ops"] = int(line.split("=", 1)[1])
        elif line.startswith("total_counter_writes="):
            values["total_counter_writes"] = int(line.split("=", 1)[1])

    return values


def main() -> int:
    run_dir = pathlib.Path(sys.argv[1])
    modes = [m for m in sys.argv[2].split(",") if m]

    rows = []
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
        totals = parse_run_totals(run_dir / f"{mode}-run-1.txt")
        total_ops = totals["total_ops"]
        total_counter_writes = totals["total_counter_writes"]
        cycles_per_op = (cycles / total_ops) if total_ops else 0.0
        cycles_per_counter_write = (
            cycles / total_counter_writes
            if total_counter_writes
            else 0.0
        )
        instructions_per_counter_write = (
            instructions / total_counter_writes
            if total_counter_writes
            else 0.0
        )

        rows.append({
            "mode": mode,
            "cycles": cycles,
            "instructions": instructions,
            "ipc": ipc,
            "cache_miss_rate": cache_miss_rate,
            "l1_miss_rate": l1_miss_rate,
            "total_ops": total_ops,
            "total_counter_writes": total_counter_writes,
            "cycles_per_op": cycles_per_op,
            "cycles_per_counter_write": cycles_per_counter_write,
            "instructions_per_counter_write": instructions_per_counter_write,
        })

        print(
            f"  {mode:6s} ipc={ipc:.3f} "
            f"cache_miss_rate={cache_miss_rate:.3f}% "
            f"l1_miss_rate={l1_miss_rate:.3f}% "
            f"cycles_per_counter_write={cycles_per_counter_write:.2f} "
            f"cycles={cycles:,} instructions={instructions:,}"
        )

    if rows:
        csv_lines = [
            "mode,cycles,instructions,ipc,cache_miss_rate_pct,l1_miss_rate_pct,"
            "total_ops,total_counter_writes,cycles_per_op,"
            "cycles_per_counter_write,instructions_per_counter_write"
        ]
        for row in rows:
            csv_lines.append(
                f"{row['mode']},{row['cycles']},{row['instructions']},"
                f"{row['ipc']:.6f},{row['cache_miss_rate']:.6f},"
                f"{row['l1_miss_rate']:.6f},{row['total_ops']},"
                f"{row['total_counter_writes']},{row['cycles_per_op']:.6f},"
                f"{row['cycles_per_counter_write']:.6f},"
                f"{row['instructions_per_counter_write']:.6f}"
            )
        (run_dir / "perf-summary.csv").write_text("\n".join(csv_lines) + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
