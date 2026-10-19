"""Retain paired CPU/wall ratios and standalone figures for HTTP exporters."""
from pathlib import Path
from statistics import median
import argparse
import csv
import json

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", type=Path)
parser.add_argument("--plot", action="store_true")
args = parser.parse_args()
rows = list(csv.DictReader((args.output / "all-runs.csv").open()))
groups = {}
for row in rows:
    cap = "all" if int(row["cap"]) > 1_000_000 else row["cap"]
    key = (row["workload"], int(row["batch"]), cap, int(row["ack_delay_us"]))
    group = groups.setdefault(key, {})
    pair = group.setdefault(int(row["repeat"]), {})
    assert row["mode"] not in pair
    pair[row["mode"]] = row
summary = []
for (workload, count, cap, delay), pairs in sorted(groups.items()):
    assert set(pairs) == set(range(8))
    assert all(set(pair) == {"native", "otel"} for pair in pairs.values())
    cell = dict(workload=workload, batch=count, cap=cap, ack_delay_us=delay, pairs=len(pairs))
    for name, field in [("cpu", "client_cpu_ns_per_span"), ("wall", "wall_ns_per_span")]:
        native = [float(pair["native"][field]) for pair in pairs.values()]
        otel = [float(pair["otel"][field]) for pair in pairs.values()]
        ratios = [b / a for a, b in zip(native, otel)]
        cell[name] = dict(native_ns_per_span=median(native), otel_ns_per_span=median(otel),
                          paired_otel_over_native=median(ratios), paired_range=[min(ratios), max(ratios)],
                          native_range=[min(native), max(native)], otel_range=[min(otel), max(otel)])
    for mode in ["native", "otel"]:
        sizes = {int(pair[mode]["protobuf_bytes_per_cycle"]) for pair in pairs.values()}
        assert len(sizes) == 1
        cell[mode + "_protobuf_bytes_per_cycle"] = sizes.pop()
    summary.append(cell)
result = dict(processes=len(rows), measured_spans=sum(int(r["verified_spans"]) for r in rows),
              cells=summary, environment=json.loads((args.output / "environment.json").read_text()))
(args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
print("RAW_BEGIN all-runs")
print((args.output / "all-runs.csv").read_text(), end="")
print("RAW_END all-runs")
print("SUMMARY_BEGIN")
print(json.dumps(result))
print("SUMMARY_END")
if args.plot:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    for delay, filename in [(0, "http-exporters"), (1000, "http-exporters-delayed")]:
        selected = [c for c in summary if c["workload"] == "attributes" and c["batch"] == 8192 and c["ack_delay_us"] == delay]
        selected.sort(key=lambda c: int(c["cap"]) if c["cap"] != "all" else 1_000_000)
        fig, axes = plt.subplots(1, 2, figsize=(10, 4), layout="constrained")
        for ax, field, ylabel, scale in [(axes[0], "cpu", "Client CPU (ns/span)", 1),
                                          (axes[1], "wall", "Acknowledged time (ms/drain)", 8192 / 1_000_000)]:
            for mode, label, offset in [("native", "Fast telemetry", -.18), ("otel", "OpenTelemetry 0.30", .18)]:
                ax.bar([i + offset for i in range(len(selected))], [c[field][mode + "_ns_per_span"] * scale for c in selected], width=.36, label=label)
            ax.set_xticks(range(len(selected)), [c["cap"] for c in selected])
            ax.set_xlabel("Spans per HTTP request")
            ax.set_ylabel(ylabel)
            ax.set_ylim(bottom=0)
        axes[0].legend()
        fig.suptitle("8,192 attributed spans, uncompressed persistent loopback" + ("; +1 ms per ACK" if delay else ""))
        for extension in ["png", "svg"]:
            fig.savefig(args.output / f"{filename}.{extension}", dpi=180)
        plt.close(fig)
