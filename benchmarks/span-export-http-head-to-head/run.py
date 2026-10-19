"""Run paired fresh-process production HTTP exporter measurements."""
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
parser.add_argument("stage", choices=["admit", "matrix"])
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--binary", type=Path, default=Path(__file__).parent / "target/release/span-export-http-head-to-head")
args = parser.parse_args()
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=True)
repo = Path(__file__).resolve().parents[2]
affinity = sorted(os.sched_getaffinity(0))
if len(affinity) < 2:
    raise RuntimeError("client and receiver need separate CPUs")
client_cpu, receiver_cpu = affinity[0], affinity[min(8, len(affinity) - 1)]
environment = dict(os.environ, EXPORT_PROBE_RECEIVER_CPU=str(receiver_cpu))


def admit(name):
    now = datetime.datetime.now(datetime.timezone.utc)
    receipt = dict(checked_utc=now.isoformat(), live_available_bytes=shutil.disk_usage(repo).free)
    receipt["admitted"] = receipt["live_available_bytes"] >= 20_000_000_000
    guard = Path.home() / ".local/state/eden-resource-guard"
    status_file = guard / "disk-status.tsv"
    if status_file.exists():
        stamp, available, threshold, status = status_file.read_text().splitlines()[1].split()
        age = (now - datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))).total_seconds()
        receipt.update(guard_age_seconds=age, guard_available_bytes=int(available), threshold_bytes=int(threshold),
                       guard_status=status, stop_marker=(guard / "DO_NOT_START_NEW_ADAM_RUNS").exists())
        receipt["admitted"] &= (0 <= age <= 90 and status == "OK" and int(threshold) == 20_000_000_000
                                and int(available) >= 20_000_000_000 and not receipt["stop_marker"])
    (output / f"{name}.guard.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if not receipt["admitted"]:
        raise RuntimeError("resource admission closed: " + json.dumps(receipt))


admit("build")
if args.stage == "admit":
    raise SystemExit(0)

binary = args.binary.resolve()
rows = []
cells = [(w, 512, 512, 0) for w in ["static", "attributes"]]
cells += [(w, n, c, 0) for w in ["static", "attributes"] for n in [3072, 8192] for c in [512, 2048, "all"]]
cells += [("attributes", 8192, c, 1000) for c in [512, 2048, "all"]]
for repeat in range(8):
    fixtures = cells[repeat:] + cells[:repeat]
    if repeat % 2:
        fixtures.reverse()
    for workload, count, cap, delay in fixtures:
        for mode in ["native", "otel"] if repeat % 2 == 0 else ["otel", "native"]:
            name = f"pair-{repeat}-{workload}-{count}-{cap}-{delay}-{mode}"
            admit(name)
            command = ["taskset", "-c", str(client_cpu), str(binary), mode, workload,
                       str(count), str(cap), "32", str(delay)]
            (output / f"{name}.command.json").write_text(json.dumps(dict(command=command,
                receiver_cpu=receiver_cpu, glibc_tunables=environment.get("GLIBC_TUNABLES")), indent=2) + "\n")
            result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=180)
            (output / f"{name}.stdout").write_text(result.stdout)
            (output / f"{name}.stderr").write_text(result.stderr)
            (output / f"{name}.exitcode").write_text(str(result.returncode) + "\n")
            if result.returncode:
                raise RuntimeError(name + " failed:\n" + result.stderr[-12000:])
            values = list(csv.DictReader(io.StringIO(result.stdout)))
            if len(values) != 1:
                raise ValueError("expected one CSV result: " + name)
            row = dict(repeat=repeat, **values[0])
            if (row["mode"] != mode or row["workload"] != workload or int(row["batch"]) != count
                    or int(row["cycles"]) != 32 or int(row["ack_delay_us"]) != delay
                    or int(row["verified_spans"]) != count * 32):
                raise ValueError("fixture or span conservation failed: " + name)
            expected_requests = 1 if cap == "all" else (count + cap - 1) // cap
            if (int(row["requests_per_cycle"]) != expected_requests
                    or int(row["wire_bytes"]) != int(row["protobuf_bytes_per_cycle"]) * 32):
                raise ValueError("request/byte conservation failed: " + name)
            rows.append(row)
            with (output / "all-runs.csv").open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=list(row))
                writer.writeheader()
                writer.writerows(rows)
            print(name + " passed", flush=True)

sources = ["crates/fast-telemetry/src/export/otlp/traces.rs", "crates/fast-telemetry-export/src/otlp/http.rs",
           "benchmarks/span-export-http-head-to-head/src/main.rs", "benchmarks/span-export-http-head-to-head/src/receiver.rs",
           "benchmarks/span-export-http-head-to-head/Cargo.lock"]
cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")), "unknown")
metadata = dict(client_cpu=client_cpu, receiver_cpu=receiver_cpu, affinity=affinity, cpu_model=cpu,
                source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                libc=subprocess.check_output(["getconf", "GNU_LIBC_VERSION"], text=True).strip(),
                compiler=subprocess.check_output(["rustc", "-Vv"], text=True),
                source_hashes={p: hashlib.sha256((repo / p).read_bytes()).hexdigest() for p in sources},
                binary_hash=hashlib.sha256(binary.read_bytes()).hexdigest())
(output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
print(f"{len(rows)} processes; {sum(int(row['verified_spans']) for row in rows)} verified measured spans", flush=True)
