"""Repeat matched, lossless OTLP pipeline measurements in balanced mode order."""
import argparse
import csv
import datetime
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--threads', type=int, nargs='+', default=[1, 8, 16])
    parser.add_argument('--export-batch', type=int, default=512)
    parser.add_argument('--cycles', type=int, default=128)
    parser.add_argument('--warmup', type=int, default=16)
    parser.add_argument('--repeats', type=int, default=6)
    parser.add_argument('--transport', default='capture')
    parser.add_argument('--workloads', nargs='+', choices=['root', 'children', 'attributes'],
                        default=['root', 'children', 'attributes'])
    parser.add_argument('--admission-command', nargs='+',
                        help='Command run before every sample; failure stops the matrix')
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if min(args.threads + [args.export_batch, args.cycles, args.repeats]) <= 0 or args.warmup < 0:
        parser.error('counts must be positive; warmup may be zero')
    for threads in args.threads:
        if args.export_batch % threads or not 1 <= args.export_batch // threads <= 1024:
            parser.error('export batch must divide evenly across threads, with 1..=1024 spans each')
        for workload in args.workloads:
            spans = {'root': 1, 'children': 4, 'attributes': 2}[workload]
            if (args.export_batch // threads) % spans:
                parser.error('per-thread batch must contain complete traces')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary_hash = digest(binary)
    metadata = dict(binary=str(binary), binary_sha256=binary_hash, platform=platform.platform(),
                    started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    cpu_affinity=sorted(os.sched_getaffinity(0)),
                    options={key: str(value) if isinstance(value, Path) else value
                             for key, value in vars(args).items()})
    (output / 'metadata.json').write_text(json.dumps(metadata, indent=2))
    modes = ['native', 'drop-in', 'otel']
    orders = list(itertools.permutations(modes))
    rows = []
    for repeat in range(args.repeats):
        for threads in args.threads:
            for workload in args.workloads:
                for mode in orders[repeat % len(orders)]:
                    name = f'{repeat}-{threads}-{workload}-{mode}'
                    if args.admission_command:
                        env = os.environ.copy()
                        env['TRACE_E2E_SAMPLE_NAME'] = name
                        subprocess.run(args.admission_command, env=env, check=True)
                    if digest(binary) != binary_hash:
                        raise RuntimeError('benchmark binary changed during measurement')
                    command = [str(binary), mode, workload, str(threads),
                               str(args.export_batch // threads), str(args.cycles),
                               str(args.warmup), args.transport]
                    (output / (name + '.command.json')).write_text(json.dumps(command, indent=2))
                    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
                    (output / (name + '.csv')).write_text(result.stdout)
                    (output / (name + '.stderr')).write_text(result.stderr)
                    (output / (name + '.exitcode')).write_text(str(result.returncode))
                    if result.returncode:
                        raise RuntimeError(f'{name} failed: {result.stderr}')
                    row, = list(csv.DictReader(result.stdout.splitlines()))
                    expected = args.export_batch * args.cycles
                    if int(row['verified_spans']) != expected or row['sampled_out'] != '0' or row['dropped'] != '0':
                        raise RuntimeError('retention accounting failed: ' + name)
                    row.update(repeat=repeat, mode_arg=mode)
                    rows.append(row)
        print(f'{repeat + 1}/{args.repeats} balanced repetitions complete', flush=True)
    with (output / 'all-runs.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summaries = []
    for threads in args.threads:
        for workload in args.workloads:
            members = {mode: [row for row in rows if int(row['threads']) == threads
                             and row['workload'].lower() == workload and row['mode_arg'] == mode]
                       for mode in modes}
            group = dict(threads=threads, workload=workload, export_batch=args.export_batch)
            for mode, samples in members.items():
                for metric in ['pipeline_cpu_ns_per_trace', 'pipeline_traces_per_sec', 'pipeline_spans_per_sec']:
                    group[mode + '_' + metric] = statistics.median(float(row[metric]) for row in samples)
            for mode in modes[:2]:
                cpu, rate = [], []
                for before, after in zip(members['otel'], members[mode], strict=True):
                    assert before['repeat'] == after['repeat']
                    cpu.append(float(before['pipeline_cpu_ns_per_trace']) / float(after['pipeline_cpu_ns_per_trace']))
                    rate.append(float(after['pipeline_traces_per_sec']) / float(before['pipeline_traces_per_sec']))
                group[mode + '_median_paired_cpu_speedup'] = statistics.median(cpu)
                group[mode + '_median_paired_throughput_ratio'] = statistics.median(rate)
            summaries.append(group)
    (output / 'summary.json').write_text(json.dumps(summaries, indent=2))
    metadata['completed_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metadata['samples'] = len(rows)
    metadata['binary_sha256_after'] = digest(binary)
    if metadata['binary_sha256_after'] != binary_hash:
        raise RuntimeError('benchmark binary changed')
    (output / 'metadata.json').write_text(json.dumps(metadata, indent=2))
    print(json.dumps(summaries, indent=2))


if __name__ == '__main__':
    main()
