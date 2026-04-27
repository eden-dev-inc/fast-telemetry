#!/usr/bin/env python3

import pathlib
import statistics
import sys


def parse_mode(run_dir: pathlib.Path, mode: str):
    record_vals = []
    total_vals = []
    export_vals = []
    cpu_user_vals = []
    cpu_system_vals = []
    cpu_total_vals = []
    cpu_core_vals = []
    cpu_util_vals = []
    cpu_ns_per_op_vals = []
    for path in sorted(run_dir.glob(f"{mode}-run-*.txt")):
        record = None
        total = None
        export_avg = None
        cpu_user = None
        cpu_system = None
        cpu_total = None
        cpu_avg_cores = None
        cpu_utilization = None
        cpu_ns_per_op = None
        for line in path.read_text().splitlines():
            if line.startswith("record_ops_per_sec="):
                record = float(line.split("=", 1)[1])
            elif line.startswith("total_ops_per_sec="):
                total = float(line.split("=", 1)[1])
            elif line.startswith("export_avg_ms="):
                export_avg = float(line.split("=", 1)[1])
            elif line.startswith("cpu_user_seconds="):
                cpu_user = float(line.split("=", 1)[1])
            elif line.startswith("cpu_system_seconds="):
                cpu_system = float(line.split("=", 1)[1])
            elif line.startswith("cpu_total_seconds="):
                cpu_total = float(line.split("=", 1)[1])
            elif line.startswith("cpu_avg_cores="):
                cpu_avg_cores = float(line.split("=", 1)[1])
            elif line.startswith("cpu_utilization_pct="):
                cpu_utilization = float(line.split("=", 1)[1])
            elif line.startswith("cpu_ns_per_op="):
                cpu_ns_per_op = float(line.split("=", 1)[1])
        if record is not None:
            record_vals.append(record)
        if total is not None:
            total_vals.append(total)
        if export_avg is not None:
            export_vals.append(export_avg)
        if cpu_user is not None:
            cpu_user_vals.append(cpu_user)
        if cpu_system is not None:
            cpu_system_vals.append(cpu_system)
        if cpu_total is not None:
            cpu_total_vals.append(cpu_total)
        if cpu_avg_cores is not None:
            cpu_core_vals.append(cpu_avg_cores)
        if cpu_utilization is not None:
            cpu_util_vals.append(cpu_utilization)
        if cpu_ns_per_op is not None:
            cpu_ns_per_op_vals.append(cpu_ns_per_op)
    return (
        record_vals,
        total_vals,
        export_vals,
        cpu_user_vals,
        cpu_system_vals,
        cpu_total_vals,
        cpu_core_vals,
        cpu_util_vals,
        cpu_ns_per_op_vals,
    )


def main() -> int:
    run_dir = pathlib.Path(sys.argv[1])
    modes = [m for m in sys.argv[2].split(",") if m]

    rows = []
    for mode in modes:
        (
            record_vals,
            total_vals,
            export_vals,
            cpu_user_vals,
            cpu_system_vals,
            cpu_total_vals,
            cpu_core_vals,
            cpu_util_vals,
            cpu_ns_per_op_vals,
        ) = parse_mode(run_dir, mode)
        if not total_vals:
            continue
        rows.append(
            (
                mode,
                record_vals,
                total_vals,
                export_vals,
                cpu_user_vals,
                cpu_system_vals,
                cpu_total_vals,
                cpu_core_vals,
                cpu_util_vals,
                cpu_ns_per_op_vals,
                statistics.median(record_vals) if record_vals else 0.0,
                statistics.median(total_vals),
                min(total_vals),
                max(total_vals),
                statistics.median(export_vals) if export_vals else 0.0,
                statistics.median(cpu_user_vals) if cpu_user_vals else 0.0,
                statistics.median(cpu_system_vals) if cpu_system_vals else 0.0,
                statistics.median(cpu_total_vals) if cpu_total_vals else 0.0,
                statistics.median(cpu_core_vals) if cpu_core_vals else 0.0,
                statistics.median(cpu_util_vals) if cpu_util_vals else 0.0,
                statistics.median(cpu_ns_per_op_vals) if cpu_ns_per_op_vals else 0.0,
            )
        )

    summary = [
        "mode,runs,median_record_ops_per_sec,median_total_ops_per_sec,min_total_ops_per_sec,max_total_ops_per_sec,median_export_avg_ms,median_cpu_user_seconds,median_cpu_system_seconds,median_cpu_total_seconds,median_cpu_avg_cores,median_cpu_utilization_pct,median_cpu_ns_per_op"
    ]
    for (
        mode,
        _,
        total_vals,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        med_record,
        med_total,
        lo_total,
        hi_total,
        med_export,
        med_cpu_user,
        med_cpu_system,
        med_cpu_total,
        med_cpu_cores,
        med_cpu_util,
        med_cpu_ns_per_op,
    ) in rows:
        summary.append(
            f"{mode},{len(total_vals)},{med_record:.2f},{med_total:.2f},{lo_total:.2f},{hi_total:.2f},{med_export:.6f},{med_cpu_user:.6f},{med_cpu_system:.6f},{med_cpu_total:.6f},{med_cpu_cores:.6f},{med_cpu_util:.2f},{med_cpu_ns_per_op:.2f}"
        )
    (run_dir / "summary.csv").write_text("\n".join(summary) + "\n")

    print("")
    print("Summary (median throughput and CPU cost, total min/max):")
    for (
        mode,
        _,
        total_vals,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        med_record,
        med_total,
        lo_total,
        hi_total,
        med_export,
        med_cpu_user,
        med_cpu_system,
        med_cpu_total,
        med_cpu_cores,
        med_cpu_util,
        med_cpu_ns_per_op,
    ) in rows:
        print(
            f"  {mode:6s} runs={len(total_vals)} record_med={med_record:,.2f} total_med={med_total:,.2f} "
            f"min={lo_total:,.2f} max={hi_total:,.2f} export_avg_ms={med_export:.6f} "
            f"cpu_total_s={med_cpu_total:.6f} cpu_user_s={med_cpu_user:.6f} cpu_system_s={med_cpu_system:.6f} "
            f"cpu_avg_cores={med_cpu_cores:.3f} cpu_util_pct={med_cpu_util:.2f} cpu_ns_per_op={med_cpu_ns_per_op:.2f}"
        )

    if rows:
        med = {row[0]: row[11] for row in rows}
        if "fast" in med and "metrics" in med:
            print(f"  fast/metrics total speedup: {med['fast'] / med['metrics']:.2f}x")
        if "fast" in med and "atomic" in med:
            print(f"  fast/atomic total speedup: {med['fast'] / med['atomic']:.2f}x")
        if "fast" in med and "otel" in med:
            print(f"  fast/otel total speedup:   {med['fast'] / med['otel']:.2f}x")
        if "metrics" in med and "otel" in med:
            print(f"  metrics/otel total speedup:{med['metrics'] / med['otel']:.2f}x")
        if "atomic" in med and "otel" in med:
            print(f"  atomic/otel total speedup: {med['atomic'] / med['otel']:.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
