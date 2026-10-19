"""Summarize paired observations and optionally export Matplotlib figures."""
from pathlib import Path
import argparse
import csv
import itertools
import json
import statistics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", type=Path)
parser.add_argument("--plot", action="store_true")
args = parser.parse_args()
root = args.output
stages = ["controls", "allocator", "caps", "http"]
data = {stage: list(csv.DictReader((root / (stage + "-runs.csv")).open())) for stage in stages}
maximum = max(int(row["cap"]) for row in data["caps"])


def stats(values):
    return dict(median=statistics.median(values), minimum=min(values), maximum=max(values))


def paired(rows, numerator, denominator, metric):
    values = []
    for repeat in sorted({row["repeat"] for row in rows}):
        pair = {row[numerator[0]]: row for row in rows if row["repeat"] == repeat}
        if len(pair) != 2:
            raise ValueError("incomplete pair")
        values.append(100 * (float(pair[numerator[1]][metric]) / float(pair[denominator][metric]) - 1))
    return stats(values)


summary = {"environment": json.loads((root / "environment.json").read_text())}
summary["counts"] = {stage: dict(processes=len(rows), verified_encodings=sum(int(r["verified_spans"]) for r in rows))
                     for stage, rows in data.items()}
for stage in ["controls", "allocator"]:
    groups = []
    for workload, setup in itertools.product(["static", "attributes"], ["original", "matched"]):
        rows = [r for r in data[stage] if r["workload"] == workload and r["setup"] == setup]
        groups.append(dict(workload=workload, setup=setup,
            single=stats([float(r["cpu_ns_per_span"]) for r in rows if r["mode"] == "single"]),
            chunks=stats([float(r["cpu_ns_per_span"]) for r in rows if r["mode"] == "chunks"]),
            paired_cpu_change_percent=paired(rows, ("mode", "chunks"), "single", "cpu_ns_per_span")))
    summary[stage] = groups

groups = []
for workload, count, cap in itertools.product(["static", "attributes"], [3072, 4096, 6144, 8192], [256, 512, 1024, 2048]):
    rows = [r for r in data["caps"] if r["workload"] == workload and int(r["batch"]) == count and int(r["cap"]) in [cap, maximum]]
    cap_rows = [r for r in rows if int(r["cap"]) == cap]
    groups.append(dict(workload=workload, batch=count, cap=cap,
        cpu_ns_per_span=stats([float(r["cpu_ns_per_span"]) for r in cap_rows]),
        requests_per_cycle=int(cap_rows[0]["requests_per_cycle"]),
        paired_cpu_change_percent=paired(rows, ("cap", str(cap)), str(maximum), "cpu_ns_per_span")))
summary["caps"] = groups

groups = []
keys = sorted({(r["workload"], int(r["batch"]), int(r["cap"]), r["compression"], int(r["ack_delay_us"])) for r in data["http"]})
for workload, count, cap, compression, delay in keys:
    rows = [r for r in data["http"] if (r["workload"], int(r["batch"]), int(r["cap"]), r["compression"], int(r["ack_delay_us"])) == (workload, count, cap, compression, delay)]
    group = dict(workload=workload, batch=count, cap=cap, compression=compression, ack_delay_us=delay,
                 requests_per_cycle=int(rows[0]["requests_per_cycle"]))
    for metric in ["client_cpu_ns_per_span", "encode_cpu_ns_per_span", "transport_cpu_ns_per_span", "wall_ns_per_span"]:
        group[metric] = stats([float(r[metric]) for r in rows])
    group["wire_bytes_per_cycle"] = stats([int(r["wire_bytes"]) / int(r["cycles"]) for r in rows])
    groups.append(group)
summary["http"] = groups
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

if args.plot:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.3), sharey=True, constrained_layout=True)
    for axis, workload, title in zip(axes, ["static", "attributes"], ["Borrowed plain spans", "Owned attributes and event"]):
        for cap in [256, 512, 1024, 2048]:
            cells = sorted([r for r in summary["caps"] if r["workload"] == workload and r["cap"] == cap], key=lambda r: r["batch"])
            axis.plot([r["batch"] for r in cells], [r["paired_cpu_change_percent"]["median"] for r in cells], marker="o", label=f"cap {cap}")
        axis.axhline(0, color="black", linewidth=0.8)
        axis.set(title=title, xlabel="Spans per drain")
        axis.grid(alpha=0.2)
        axis.legend()
    axes[0].set_ylabel("Encoding + body cleanup CPU change (%)\nrelative to one unlimited request")
    fig.suptitle("Matched preparation: request cap tradeoff (lower is better)")
    fig.savefig(root / "request-cap-tradeoff.svg")
    fig.savefig(root / "request-cap-tradeoff.png", dpi=160)
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.2), constrained_layout=True)
    labels = ["Original verifier", "Matched verifier"]
    for axis, workload, title in zip(axes, ["static", "attributes"], ["Borrowed plain spans", "Owned attributes and event"]):
        for stage, label, color in [("controls", "Default allocator", "#2166ac"), ("allocator", "Fixed 1 GiB thresholds", "#b35806")]:
            cells = [next(r for r in summary[stage] if r["workload"] == workload and r["setup"] == setup)["paired_cpu_change_percent"] for setup in ["original", "matched"]]
            medians = [r["median"] for r in cells]
            axis.errorbar([0, 1], medians, yerr=[[r["median"] - r["minimum"] for r in cells], [r["maximum"] - r["median"] for r in cells]], marker="o", capsize=4, label=label, color=color)
        axis.axhline(0, color="black", linewidth=0.8)
        axis.set(title=title, xticks=[0, 1], xticklabels=labels, ylabel="CPU change: cap 512 vs unlimited (%)")
        axis.grid(alpha=0.2)
        axis.legend()
    fig.suptitle("8,192 spans: allocator/setup controls (whiskers: paired min/max)")
    fig.savefig(root / "allocator-controls.svg")
    fig.savefig(root / "allocator-controls.png", dpi=160)
    plt.close(fig)

    fig, axes = plt.subplots(2, 2, figsize=(11, 7), constrained_layout=True)
    for column, workload in enumerate(["static", "attributes"]):
        for row, metric in enumerate(["client_cpu_ns_per_span", "wall_ns_per_span"]):
            axis = axes[row, column]
            for compression in ["plain", "gzip"]:
                cells = [next(r for r in summary["http"] if r["workload"] == workload and r["batch"] == 8192 and r["cap"] == cap and r["compression"] == compression and r["ack_delay_us"] == 0) for cap in [512, 2048, maximum]]
                scale = 1 if row == 0 else 8192 / 1_000_000
                axis.plot([0, 1, 2], [r[metric]["median"] * scale for r in cells], marker="o", label=compression)
            axis.set(xticks=[0, 1, 2], xticklabels=["512 (16 req)", "2048 (4 req)", "all (1 req)"],
                     ylabel="Client CPU ns/span" if row == 0 else "Acknowledged wall ms/drain")
            axis.grid(alpha=0.2)
            axis.legend()
            if row == 0:
                axis.set_title("Borrowed plain spans" if workload == "static" else "Owned attributes and event")
    fig.suptitle("8,192 spans over persistent loopback HTTP; separate validating receiver")
    fig.savefig(root / "http-tradeoff.svg")
    fig.savefig(root / "http-tradeoff.png", dpi=160)
    plt.close(fig)

    fig, axis = plt.subplots(figsize=(8, 4.2), constrained_layout=True)
    for delay in [0, 1000]:
        cells = [next(r for r in summary["http"] if r["workload"] == "attributes" and r["batch"] == 8192 and r["cap"] == cap and r["compression"] == "plain" and r["ack_delay_us"] == delay) for cap in [512, 2048, maximum]]
        axis.plot([0, 1, 2], [r["wall_ns_per_span"]["median"] * 8192 / 1_000_000 for r in cells], marker="o", label=f"{delay} us added per-request delay")
    axis.set(xticks=[0, 1, 2], xticklabels=["512 (16 requests)", "2048 (4 requests)", "all (1 request)"],
             ylabel="Acknowledged wall ms/drain", title="8,192 attributed spans: extra request delay")
    axis.legend()
    axis.grid(alpha=0.2)
    fig.savefig(root / "acknowledgement-delay.svg")
    fig.savefig(root / "acknowledgement-delay.png", dpi=160)
    plt.close(fig)

for stage in stages:
    print("RAW_BEGIN " + stage)
    print((root / (stage + "-runs.csv")).read_text().strip())
    print("RAW_END " + stage)
print("SUMMARY_BEGIN")
print(json.dumps(summary))
print("SUMMARY_END")
