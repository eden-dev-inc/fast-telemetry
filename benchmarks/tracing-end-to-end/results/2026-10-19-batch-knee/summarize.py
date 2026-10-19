"""Recompute the measured knee and paired source controls; no filtering."""
from pathlib import Path
import csv
import json
import statistics as stats

ROOT = Path(__file__).resolve().parent
SIZES = [64, 128, 256, 384, 512, 768, 1024]
WORKLOADS = ["Root", "Children", "Attributes"]


def read(name, expected):
    with (ROOT / name).open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == expected
    assert all(int(r["verified_spans"]) == 196608 for r in rows)
    assert all(int(r["sampled_out"]) == int(r["dropped"]) == 0 for r in rows)
    return rows


sweep = read("baseline-runs.csv", 126)
confirmation = read("confirmation-runs.csv", 108)
cells = []
for work in WORKLOADS:
    groups = {
        size: [float(r["pipeline_cpu_ns_per_trace"]) for r in sweep
               if r["workload"] == work and int(r["batch_spans_per_thread"]) == size]
        for size in SIZES
    }
    assert all(len(v) == 6 for v in groups.values())
    medians = {size: stats.median(v) for size, v in groups.items()}
    span = medians[64] - min(medians.values())
    # Normalize batch size and measured improvement to [0, 1]. The largest gap
    # above the endpoint chord locates the strongest bend in the sampled curve.
    distances = {size: (medians[64] - medians[size]) / span
                 - (size - 64) / (1024 - 64) for size in SIZES}
    cells.append({"workload": work, "knee": max(distances, key=distances.get),
                  "batches": {size: {"median_cpu_ns_per_trace": medians[size],
                                     "min_cpu_ns_per_trace": min(groups[size]),
                                     "max_cpu_ns_per_trace": max(groups[size]),
                                     "normalized_chord_distance": distances[size]}
                              for size in SIZES}})
controls = []
for threads in [1, 8]:
    for work in WORKLOADS:
        groups = {
            variant: {r["repetition"]: float(r["pipeline_cpu_ns_per_trace"])
                      for r in confirmation if r["workload"] == work
                      and int(r["threads"]) == threads and r["variant"] == variant}
            for variant in ["before", "configured-256", "default-384"]
        }
        before = groups["before"]
        assert len(before) == 6
        for variant, values in groups.items():
            assert values.keys() == before.keys()
            changes = [100 * (values[k] / before[k] - 1) for k in sorted(before)]
            controls.append({"threads": threads, "workload": work, "variant": variant,
                             "median_cpu_ns_per_trace": stats.median(values.values()),
                             "median_paired_change_percent": stats.median(changes),
                             "min_paired_change_percent": min(changes),
                             "max_paired_change_percent": max(changes),
                             "pairs_faster": sum(v < 0 for v in changes), "pairs": 6})
summary = {"selection_processes": len(sweep), "confirmation_processes": len(confirmation),
           "verified_spans": sum(int(r["verified_spans"]) for r in sweep + confirmation),
           "selection": cells, "confirmation": controls}
(ROOT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print("Measured knees:", {c["workload"]: c["knee"] for c in cells})
print("Verified spans:", summary["verified_spans"])
