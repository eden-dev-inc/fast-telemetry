#!/usr/bin/env python3

import pathlib
import statistics
import sys


def parse_mode(run_dir: pathlib.Path, mode: str):
    record_vals = []
    total_vals = []
    export_vals = []
    for path in sorted(run_dir.glob(f"{mode}-run-*.txt")):
        record = None
        total = None
        export_avg = None
        for line in path.read_text().splitlines():
            if line.startswith("record_ops_per_sec="):
                record = float(line.split("=", 1)[1])
            elif line.startswith("total_ops_per_sec="):
                total = float(line.split("=", 1)[1])
            elif line.startswith("export_avg_ms="):
                export_avg = float(line.split("=", 1)[1])
        if record is not None:
            record_vals.append(record)
        if total is not None:
            total_vals.append(total)
        if export_avg is not None:
            export_vals.append(export_avg)
    return record_vals, total_vals, export_vals


def main() -> int:
    run_dir = pathlib.Path(sys.argv[1])
    modes = [m for m in sys.argv[2].split(",") if m]

    rows = []
    for mode in modes:
        record_vals, total_vals, export_vals = parse_mode(run_dir, mode)
        if not total_vals:
            continue
        rows.append(
            (
                mode,
                record_vals,
                total_vals,
                export_vals,
                statistics.median(record_vals) if record_vals else 0.0,
                statistics.median(total_vals),
                min(total_vals),
                max(total_vals),
                statistics.median(export_vals) if export_vals else 0.0,
            )
        )

    summary = [
        "mode,runs,median_record_ops_per_sec,median_total_ops_per_sec,min_total_ops_per_sec,max_total_ops_per_sec,median_export_avg_ms"
    ]
    for (
        mode,
        _,
        total_vals,
        _,
        med_record,
        med_total,
        lo_total,
        hi_total,
        med_export,
    ) in rows:
        summary.append(
            f"{mode},{len(total_vals)},{med_record:.2f},{med_total:.2f},{lo_total:.2f},{hi_total:.2f},{med_export:.6f}"
        )
    (run_dir / "summary.csv").write_text("\n".join(summary) + "\n")

    print("")
    print("Summary (median record + total throughput, total min/max):")
    for (
        mode,
        _,
        total_vals,
        _,
        med_record,
        med_total,
        lo_total,
        hi_total,
        med_export,
    ) in rows:
        print(
            f"  {mode:6s} runs={len(total_vals)} record_med={med_record:,.2f} total_med={med_total:,.2f} "
            f"min={lo_total:,.2f} max={hi_total:,.2f} export_avg_ms={med_export:.6f}"
        )

    if rows:
        med = {mode: m_total for mode, _, _, _, _, m_total, _, _, _ in rows}
        if "fast" in med and "atomic" in med:
            print(f"  fast/atomic total speedup: {med['fast'] / med['atomic']:.2f}x")
        if "fast" in med and "otel" in med:
            print(f"  fast/otel total speedup:   {med['fast'] / med['otel']:.2f}x")
        if "atomic" in med and "otel" in med:
            print(f"  atomic/otel total speedup: {med['atomic'] / med['otel']:.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
