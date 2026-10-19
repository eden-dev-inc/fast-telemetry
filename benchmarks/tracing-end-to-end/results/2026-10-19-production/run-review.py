"""Run sequential fresh processes with balanced cyclic/reverse process order."""
import argparse, csv, datetime, io, json, os, pathlib, shutil, subprocess

parser = argparse.ArgumentParser()
for name in ("baseline", "before", "candidate", "output"):
    parser.add_argument("--" + name, required=True)
parser.add_argument("--affinity", default="0-15")
parser.add_argument("--cycles", type=int, default=512)
args = parser.parse_args()
output = pathlib.Path(args.output)
output.mkdir(parents=True, exist_ok=True)
variants = [("original", args.baseline, "native"), ("before-review", args.before, "native"),
            ("candidate-native", args.candidate, "native"),
            ("candidate-user-fields", args.candidate, "drop-in"),
            ("otel", args.candidate, "otel")]
guard = pathlib.Path.home() / ".local/state/eden-resource-guard"
with (output / "all-runs.csv").open("w", newline="") as csvfile:
    writer = None
    ordinal = 0
    for repetition in range(10):
        sequence = variants[repetition % 5:] + variants[:repetition % 5]
        if repetition >= 5:
            sequence = list(reversed(sequence))
        for threads in (1, 8):
            for workload in ("root", "children", "attributes"):
                for variant, binary, mode in sequence:
                    if shutil.disk_usage(output).free < 20_000_000_000:
                        raise RuntimeError("insufficient free disk")
                    if (guard / "disk-status.tsv").exists():
                        stamp, available, threshold, status = (guard / "disk-status.tsv").read_text().splitlines()[1].split()
                        age = (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))).total_seconds()
                        if not (0 <= age <= 90 and status == "OK" and int(available) >= int(threshold) >= 20_000_000_000 and not (guard / "DO_NOT_START_NEW_ADAM_RUNS").exists()):
                            raise RuntimeError("resource guard closed")
                    command = ["taskset", "-c", args.affinity, str(pathlib.Path(binary).resolve()), mode,
                               workload, str(threads), str(512 // threads), str(args.cycles), "16", "capture"]
                    result = subprocess.run(command, capture_output=True, text=True, timeout=180)
                    stem = output / f"{ordinal:03}-{variant}-{workload}-{threads}"
                    stem.with_suffix(".stdout").write_text(result.stdout)
                    stem.with_suffix(".stderr").write_text(result.stderr)
                    if result.returncode:
                        raise RuntimeError(f"{command}: {result.stderr}")
                    rows = list(csv.DictReader(io.StringIO(result.stdout)))
                    assert len(rows) == 1
                    row = dict(ordinal=ordinal, repetition=repetition, variant=variant, affinity=args.affinity, **rows[0])
                    assert int(row["verified_spans"]) == 512 * args.cycles
                    assert int(row["wire_requests"]) == args.cycles
                    assert int(row["sampled_out"]) == int(row["dropped"]) == 0
                    if writer is None:
                        writer = csv.DictWriter(csvfile, fieldnames=list(row), lineterminator="\n")
                        writer.writeheader()
                    writer.writerow(row); csvfile.flush()
                    ordinal += 1
        print(f"repetition {repetition + 1}/10: {ordinal} successful processes", flush=True)
