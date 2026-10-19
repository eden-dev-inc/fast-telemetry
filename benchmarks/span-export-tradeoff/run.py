"""Run fresh-process span export controls with resource admission and payload checks."""
from pathlib import Path
import argparse
import csv
import datetime
import hashlib
import io
import json
import os
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("stage", choices=["admit", "controls", "allocator", "caps", "http"])
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--binaries", type=Path, default=Path(__file__).parent / "target/release")
args = parser.parse_args()
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=True)
affinity = sorted(os.sched_getaffinity(0))
client_cpu = affinity[0]
receiver_cpu = affinity[min(8, len(affinity) - 1)]
repo = Path(__file__).resolve().parents[2]
environment = dict(os.environ, EXPORT_PROBE_RECEIVER_CPU=str(receiver_cpu))
rows = []


def admit(name):
    now = datetime.datetime.now(datetime.timezone.utc)
    receipt = dict(checked_utc=now.isoformat(), live_available_bytes=shutil.disk_usage("/").free)
    guard = Path.home() / ".local/state/eden-resource-guard"
    status_file = guard / "disk-status.tsv"
    receipt["admitted"] = receipt["live_available_bytes"] >= 20_000_000_000
    if status_file.exists():
        stamp, available, threshold, status = status_file.read_text().splitlines()[1].split()
        age = (now - datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))).total_seconds()
        receipt.update(guard_age_seconds=age, guard_available_bytes=int(available),
                       threshold_bytes=int(threshold), guard_status=status,
                       stop_marker=(guard / "DO_NOT_START_NEW_ADAM_RUNS").exists())
        receipt["admitted"] &= (0 <= age <= 90 and status == "OK" and int(threshold) == 20_000_000_000
                                and int(available) >= 20_000_000_000 and not receipt["stop_marker"])
    (output / (name + ".guard.json")).write_text(json.dumps(receipt, indent=2) + "\n")
    if not receipt["admitted"]:
        raise RuntimeError("resource guard closed: " + json.dumps(receipt))


def sample(name, command, repeat):
    command = ["taskset", "-c", str(client_cpu), *map(str, command)]
    admit(name)
    (output / (name + ".command.json")).write_text(json.dumps(dict(command=command,
        receiver_cpu=receiver_cpu, glibc_tunables=environment.get("GLIBC_TUNABLES")), indent=2) + "\n")
    result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=180)
    (output / (name + ".stdout")).write_text(result.stdout)
    (output / (name + ".stderr")).write_text(result.stderr)
    (output / (name + ".exitcode")).write_text(str(result.returncode))
    if result.returncode:
        raise RuntimeError(name + " failed:\n" + result.stderr[-12000:])
    values = list(csv.DictReader(io.StringIO(result.stdout)))
    if len(values) != 1:
        raise ValueError("expected one CSV result: " + name)
    row = dict(repeat=repeat, **values[0])
    if int(row["verified_spans"]) != int(row["batch"]) * int(row["cycles"]):
        raise ValueError("span conservation failed: " + name)
    expected_requests = (int(row["batch"]) + int(row["cap"]) - 1) // int(row["cap"])
    if int(row["requests_per_cycle"]) != expected_requests:
        raise ValueError("request conservation failed: " + name)
    rows.append(row)
    with (output / (args.stage + "-runs.csv")).open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(row))
        writer.writeheader()
        writer.writerows(rows)
    print(name + " passed", flush=True)


binary = args.binaries.resolve() / "span-export-tradeoff-probe"
http = args.binaries.resolve() / "span-export-http-probe"
if args.stage == "admit":
    admit("build")
elif args.stage in ["controls", "allocator"]:
    if args.stage == "allocator":
        environment["GLIBC_TUNABLES"] = "glibc.malloc.trim_threshold=1073741824:glibc.malloc.mmap_threshold=1073741824"
    variants = [("single", "original"), ("chunks", "original"), ("single", "matched"), ("chunks", "matched")]
    for repeat in range(8):
        shift = repeat % 4
        ordered = variants[shift:] + variants[:shift]
        if repeat >= 4:
            ordered = list(reversed(ordered))
        for workload in ["static", "attributes"] if repeat % 2 == 0 else ["attributes", "static"]:
            for mode, setup in ordered:
                sample(f"{args.stage}-{repeat}-{workload}-{mode}-{setup}",
                       [binary, mode, workload, 8192, 128, 512, setup], repeat)
elif args.stage == "caps":
    caps = [256, 512, 1024, 2048, "all"]
    cells = [(w, n) for w in ["static", "attributes"] for n in [3072, 4096, 6144, 8192]]
    for repeat in range(6):
        shift = repeat % 5
        ordered = caps[shift:] + caps[:shift]
        fixtures = cells[repeat:] + cells[:repeat]
        if repeat % 2:
            ordered, fixtures = list(reversed(ordered)), list(reversed(fixtures))
        for workload, count in fixtures:
            for cap in ordered:
                sample(f"caps-{repeat}-{workload}-{count}-{cap}", [binary,
                    "single" if cap == "all" else "chunks", workload, count,
                    (1_048_576 + count - 1) // count, 512 if cap == "all" else cap, "matched"], repeat)
else:
    caps = [512, 2048, "all"]
    cells = [(w, n, c, 0) for w in ["static", "attributes"] for n in [3072, 8192] for c in ["plain", "gzip"]]
    cells += [("attributes", 8192, "plain", 1000)]
    for repeat in range(4):
        shift = repeat % 3
        ordered, fixtures = caps[shift:] + caps[:shift], cells[repeat:] + cells[:repeat]
        if repeat % 2:
            ordered, fixtures = list(reversed(ordered)), list(reversed(fixtures))
        for workload, count, compression, delay in fixtures:
            for cap in ordered:
                sample(f"http-{repeat}-{workload}-{count}-{cap}-{compression}-{delay}",
                       [http, workload, count, cap, 32, compression, delay], repeat)

cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")), "unknown")
sources = ["crates/fast-telemetry/src/export/otlp/traces.rs", "crates/fast-telemetry-export/src/otlp/http.rs"]
metadata = dict(client_cpu=client_cpu, receiver_cpu=receiver_cpu, affinity=affinity, cpu_model=cpu,
                source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                libc=subprocess.check_output(["getconf", "GNU_LIBC_VERSION"], text=True).strip(),
                compiler=subprocess.check_output(["rustc", "-Vv"], text=True),
                source_hashes={p: hashlib.sha256((repo / p).read_bytes()).hexdigest() for p in sources},
                binary_hashes={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [binary, http] if p.is_file()})
(output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
print(f"{args.stage}: {len(rows)} processes; {sum(int(row['verified_spans']) for row in rows)} verified encodings")
