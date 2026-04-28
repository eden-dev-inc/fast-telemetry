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
            # Wall-time-derived throughput is noise-contaminated by scheduler
            # stalls when the host has ambient load. Kept for informational
            # purposes (it's what users actually experience) but NOT the
            # regression signal.
            "total_cv_pct": cv_pct(vals["total"]),
            "export_tm": trimmed_mean(vals["export"]),
            "cpu_user_tm": trimmed_mean(vals["cpu_user"]),
            "cpu_system_tm": trimmed_mean(vals["cpu_system"]),
            "cpu_total_tm": trimmed_mean(vals["cpu_total"]),
            "cpu_cores_tm": trimmed_mean(vals["cpu_cores"]),
            "cpu_util_tm": trimmed_mean(vals["cpu_util"]),
            # CPU-time per op is the regression-tracking metric: it counts only
            # time the bench process was on-CPU (via getrusage RUSAGE_SELF), so
            # it's invariant to scheduler stalls and noisy neighbors.
            "cpu_ns_per_op_tm": trimmed_mean(vals["cpu_ns_per_op"]),
            "cpu_ns_per_op_min": min(vals["cpu_ns_per_op"]) if vals["cpu_ns_per_op"] else 0.0,
            "cpu_ns_per_op_max": max(vals["cpu_ns_per_op"]) if vals["cpu_ns_per_op"] else 0.0,
            "cpu_ns_per_op_cv_pct": cv_pct(vals["cpu_ns_per_op"]),
        })

    csv_lines = [
        "mode,runs,trimmed_record_ops_per_sec,trimmed_total_ops_per_sec,"
        "min_total_ops_per_sec,max_total_ops_per_sec,total_cv_pct,"
        "trimmed_export_avg_ms,trimmed_cpu_user_seconds,trimmed_cpu_system_seconds,"
        "trimmed_cpu_total_seconds,trimmed_cpu_avg_cores,trimmed_cpu_utilization_pct,"
        "trimmed_cpu_ns_per_op,min_cpu_ns_per_op,max_cpu_ns_per_op,cpu_ns_per_op_cv_pct"
    ]
    for r in rows:
        csv_lines.append(
            f"{r['mode']},{r['n']},{r['record_tm']:.2f},{r['total_tm']:.2f},"
            f"{r['total_min']:.2f},{r['total_max']:.2f},{r['total_cv_pct']:.2f},"
            f"{r['export_tm']:.6f},{r['cpu_user_tm']:.6f},{r['cpu_system_tm']:.6f},"
            f"{r['cpu_total_tm']:.6f},{r['cpu_cores_tm']:.6f},{r['cpu_util_tm']:.2f},"
            f"{r['cpu_ns_per_op_tm']:.2f},{r['cpu_ns_per_op_min']:.2f},"
            f"{r['cpu_ns_per_op_max']:.2f},{r['cpu_ns_per_op_cv_pct']:.2f}"
        )
    (run_dir / "summary.csv").write_text("\n".join(csv_lines) + "\n")

    print("")
    print("Summary (cpu_ns_per_op is the regression metric; throughput is informational):")
    for r in rows:
        cpu_cv = r["cpu_ns_per_op_cv_pct"]
        wall_cv = r["total_cv_pct"]
        cpu_cv_marker = " [HIGH-CV]" if cpu_cv > 10.0 else ""
        print(
            f"  {r['mode']:6s} runs={r['n']} "
            f"cpu_ns_per_op={r['cpu_ns_per_op_tm']:.2f} (cv={cpu_cv:.1f}%{cpu_cv_marker}, "
            f"min={r['cpu_ns_per_op_min']:.2f} max={r['cpu_ns_per_op_max']:.2f})  "
            f"total_ops/s={r['total_tm']:,.0f} (wall_cv={wall_cv:.1f}%)  "
            f"cpu_total_s={r['cpu_total_tm']:.3f} cpu_avg_cores={r['cpu_cores_tm']:.2f} "
            f"export_avg_ms={r['export_tm']:.4f}"
        )

    if rows:
        # Speedup ratios from cpu_ns_per_op (lower is better, so divide).
        ns = {r["mode"]: r["cpu_ns_per_op_tm"] for r in rows if r["cpu_ns_per_op_tm"] > 0}
        if "fast" in ns and "metrics" in ns:
            print(f"  fast/metrics speedup: {ns['metrics'] / ns['fast']:.2f}x")
        if "fast" in ns and "atomic" in ns:
            print(f"  fast/atomic speedup:  {ns['atomic'] / ns['fast']:.2f}x")
        if "fast" in ns and "otel" in ns:
            print(f"  fast/otel speedup:    {ns['otel'] / ns['fast']:.2f}x")
        if "metrics" in ns and "otel" in ns:
            print(f"  metrics/otel speedup: {ns['otel'] / ns['metrics']:.2f}x")
        if "atomic" in ns and "otel" in ns:
            print(f"  atomic/otel speedup:  {ns['otel'] / ns['atomic']:.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
