#!/usr/bin/env python3

import pathlib
import statistics
import sys


def trimmed_mean(vals):
    """Mean after dropping the highest and lowest values.

    For >=5 runs, drops floor((n-1)/4) from each side. This makes a single
    process spike during one run not poison the headline number. For <5 runs,
    falls back to the median.
    """
    if not vals:
        return 0.0
    if len(vals) < 5:
        return statistics.median(vals)
    trim = (len(vals) - 1) // 4
    sorted_vals = sorted(vals)
    return statistics.fmean(sorted_vals[trim:len(sorted_vals) - trim])


def cv_pct(vals):
    """Coefficient of variation as a percentage. <10% is decision-quality."""
    if len(vals) < 2:
        return 0.0
    mean = statistics.fmean(vals)
    if mean == 0.0:
        return 0.0
    return (statistics.stdev(vals) / mean) * 100.0


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
    return {
        "record": record_vals,
        "total": total_vals,
        "export": export_vals,
        "cpu_user": cpu_user_vals,
        "cpu_system": cpu_system_vals,
        "cpu_total": cpu_total_vals,
        "cpu_cores": cpu_core_vals,
        "cpu_util": cpu_util_vals,
        "cpu_ns_per_op": cpu_ns_per_op_vals,
    }


def main() -> int:
    run_dir = pathlib.Path(sys.argv[1])
    modes = [m for m in sys.argv[2].split(",") if m]

    rows = []
    for mode in modes:
        vals = parse_mode(run_dir, mode)
        if not vals["total"]:
            continue
        rows.append({
            "mode": mode,
            "n": len(vals["total"]),
            "record_tm": trimmed_mean(vals["record"]),
            "total_tm": trimmed_mean(vals["total"]),
            "total_min": min(vals["total"]),
            "total_max": max(vals["total"]),
            "total_cv_pct": cv_pct(vals["total"]),
            "export_tm": trimmed_mean(vals["export"]),
            "cpu_user_tm": trimmed_mean(vals["cpu_user"]),
            "cpu_system_tm": trimmed_mean(vals["cpu_system"]),
            "cpu_total_tm": trimmed_mean(vals["cpu_total"]),
            "cpu_cores_tm": trimmed_mean(vals["cpu_cores"]),
            "cpu_util_tm": trimmed_mean(vals["cpu_util"]),
            "cpu_ns_per_op_tm": trimmed_mean(vals["cpu_ns_per_op"]),
        })

    csv_lines = [
        "mode,runs,trimmed_record_ops_per_sec,trimmed_total_ops_per_sec,"
        "min_total_ops_per_sec,max_total_ops_per_sec,total_cv_pct,"
        "trimmed_export_avg_ms,trimmed_cpu_user_seconds,trimmed_cpu_system_seconds,"
        "trimmed_cpu_total_seconds,trimmed_cpu_avg_cores,trimmed_cpu_utilization_pct,"
        "trimmed_cpu_ns_per_op"
    ]
    for r in rows:
        csv_lines.append(
            f"{r['mode']},{r['n']},{r['record_tm']:.2f},{r['total_tm']:.2f},"
            f"{r['total_min']:.2f},{r['total_max']:.2f},{r['total_cv_pct']:.2f},"
            f"{r['export_tm']:.6f},{r['cpu_user_tm']:.6f},{r['cpu_system_tm']:.6f},"
            f"{r['cpu_total_tm']:.6f},{r['cpu_cores_tm']:.6f},{r['cpu_util_tm']:.2f},"
            f"{r['cpu_ns_per_op_tm']:.2f}"
        )
    (run_dir / "summary.csv").write_text("\n".join(csv_lines) + "\n")

    print("")
    print("Summary (trimmed mean throughput and CPU cost, total min/max, CV):")
    for r in rows:
        cv_marker = " [HIGH-CV]" if r["total_cv_pct"] > 10.0 else ""
        print(
            f"  {r['mode']:6s} runs={r['n']} record_tm={r['record_tm']:,.2f} "
            f"total_tm={r['total_tm']:,.2f} min={r['total_min']:,.2f} max={r['total_max']:,.2f} "
            f"cv={r['total_cv_pct']:.1f}%{cv_marker} "
            f"export_avg_ms={r['export_tm']:.6f} "
            f"cpu_total_s={r['cpu_total_tm']:.6f} cpu_user_s={r['cpu_user_tm']:.6f} "
            f"cpu_system_s={r['cpu_system_tm']:.6f} cpu_avg_cores={r['cpu_cores_tm']:.3f} "
            f"cpu_util_pct={r['cpu_util_tm']:.2f} cpu_ns_per_op={r['cpu_ns_per_op_tm']:.2f}"
        )

    if rows:
        tm = {r["mode"]: r["total_tm"] for r in rows}
        if "fast" in tm and "metrics" in tm:
            print(f"  fast/metrics total speedup: {tm['fast'] / tm['metrics']:.2f}x")
        if "fast" in tm and "atomic" in tm:
            print(f"  fast/atomic total speedup: {tm['fast'] / tm['atomic']:.2f}x")
        if "fast" in tm and "otel" in tm:
            print(f"  fast/otel total speedup:   {tm['fast'] / tm['otel']:.2f}x")
        if "metrics" in tm and "otel" in tm:
            print(f"  metrics/otel total speedup:{tm['metrics'] / tm['otel']:.2f}x")
        if "atomic" in tm and "otel" in tm:
            print(f"  atomic/otel total speedup: {tm['atomic'] / tm['otel']:.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
