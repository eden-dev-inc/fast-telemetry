"""Recompute medians and paired ratios from all successful raw samples."""
import collections, csv, json, pathlib, statistics
root = pathlib.Path(__file__).resolve().parent
rows = list(csv.DictReader((root / "all-runs.csv").open()))
assert len(rows) == 300
cells = collections.defaultdict(dict)
for row in rows:
    assert int(row["sampled_out"]) == int(row["dropped"]) == 0
    key = (int(row["threads"]), row["workload"])
    cell = cells[key].setdefault(row["variant"], {})
    repetition = int(row["repetition"])
    assert repetition not in cell
    cell[repetition] = float(row["pipeline_cpu_ns_per_trace"])
summary = {"processes": len(rows), "verified_spans": sum(int(r["verified_spans"]) for r in rows),
           "verified_traces": sum(int(r["verified_traces"]) for r in rows),
           "verified_events": sum(int(r["verified_events"]) for r in rows),
           "wire_requests": sum(int(r["wire_requests"]) for r in rows), "cells": []}
for (threads, workload), variants in sorted(cells.items()):
    assert len(variants) == 5 and all(len(v) == 10 for v in variants.values())
    def ratios(numerator, denominator):
        values = [variants[numerator][i] / variants[denominator][i] for i in range(10)]
        return dict(median=statistics.median(values), minimum=min(values), maximum=max(values))
    result = dict(threads=threads, workload=workload,
                  median_cpu_ns_per_trace={k: statistics.median(v.values()) for k, v in variants.items()},
                  original_over_candidate=ratios("original", "candidate-native"),
                  candidate_over_before_review=ratios("candidate-native", "before-review"),
                  otel_over_user_fields=ratios("otel", "candidate-user-fields"))
    summary["cells"].append(result)
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
