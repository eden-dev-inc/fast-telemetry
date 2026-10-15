#![cfg(feature = "bench-tools")]

use std::process::Command;

fn row(csv: &str) -> std::collections::HashMap<&str, &str> {
    let mut lines = csv.lines();
    let header = lines.next().unwrap();
    let data = lines.next().unwrap();
    assert!(lines.next().is_none());
    let columns: Vec<_> = header.split(',').collect();
    let values: Vec<_> = data.split(',').collect();
    assert_eq!(columns.len(), values.len());
    columns.into_iter().zip(values).collect()
}

#[test]
fn benchmark_checks_actual_completed_sinks_for_every_mode_and_workload() {
    for (dispatch, mode) in ["scoped", "global"].into_iter().flat_map(|dispatch| {
        [
            "native",
            "native-matched",
            "native-user-fields",
            "fast",
            "direct",
            "direct-user-fields",
            "otel",
            "disabled",
            "registry",
        ]
        .map(|mode| (dispatch, mode))
    }) {
        for workload in ["root", "attributes", "children"] {
            let output = Command::new(env!("CARGO_BIN_EXE_bench_tracing_backends"))
                .args([
                    "--dispatch",
                    dispatch,
                    "--mode",
                    mode,
                    "--workload",
                    workload,
                    "--threads",
                    "2",
                    "--iters",
                    "7",
                    "--warmup",
                    "3",
                    "--batch",
                    "4",
                    "--capacity",
                    "64",
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{mode}/{workload}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let csv = String::from_utf8(output.stdout).unwrap();
            let columns = row(&csv);
            assert_eq!(columns["dispatch"], dispatch);
            let spans: u64 = columns["completed_spans"].parse().unwrap();
            let events: u64 = columns["events"].parse().unwrap();
            let retains = !["disabled", "registry"].contains(&mode);
            let multiplier = match workload {
                "root" => 1,
                "attributes" => 2,
                _ => 4,
            };
            assert_eq!(spans, if !retains { 0 } else { 14 * multiplier });
            assert_eq!(
                events,
                if retains && workload == "attributes" {
                    14
                } else {
                    0
                }
            );
            assert_eq!(columns["dropped"], "0");
            assert_eq!(columns["sampled_out"], "0");
            assert_eq!(
                columns["recorded_roots"],
                if mode.starts_with("native") {
                    "14"
                } else {
                    "0"
                }
            );
            assert_eq!(
                columns["attempted_spans"].parse::<u64>().unwrap(),
                14 * multiplier
            );
            if mode != "otel" {
                let attributes = match (mode, workload) {
                    ("native" | "native-user-fields" | "direct-user-fields", "attributes") => 6,
                    ("native" | "native-user-fields" | "direct-user-fields", _)
                    | ("disabled" | "registry", _) => 0,
                    (_, "root") => 5,
                    (_, "attributes") => 21,
                    (_, "children") => 20,
                    _ => unreachable!(),
                };
                assert_eq!(
                    columns["payload_attributes"].parse::<u64>().unwrap(),
                    14 * attributes
                );
            }
            for metric in [
                "record_cpu_ns_per_span",
                "pipeline_cpu_ns_per_span",
                "pipeline_with_destroy_cpu_ns_per_span",
            ] {
                assert_eq!(columns[metric] == "NA", !retains, "{mode}: {metric}");
            }
        }
    }
}

#[test]
fn native_retains_full_boundary_batches_and_partial_batches_with_workers_alive() {
    for mode in ["native", "native-matched", "native-user-fields"] {
        let output = Command::new(env!("CARGO_BIN_EXE_bench_tracing_backends"))
            .args([
                "--mode",
                mode,
                "--workload",
                "children",
                "--threads",
                "2",
                "--iters",
                "257",
                "--warmup",
                "257",
                "--batch",
                "256",
                "--capacity",
                "1",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let csv = String::from_utf8(output.stdout).unwrap();
        let columns = row(&csv);
        assert_eq!(columns["completed_spans"], "2056");
        assert_eq!(columns["recorded_roots"], "514");
        assert_eq!(columns["sampled_out"], "0");
    }
}

#[test]
fn native_rejects_sampling_pressure_in_measured_and_warmup_batches() {
    for mode in ["native", "native-matched", "native-user-fields"] {
        for (iterations, warmup) in [("257", "0"), ("1", "257")] {
            let output = Command::new(env!("CARGO_BIN_EXE_bench_tracing_backends"))
                .args([
                    "--mode",
                    mode,
                    "--workload",
                    "children",
                    "--threads",
                    "1",
                    "--iters",
                    iterations,
                    "--warmup",
                    warmup,
                    "--batch",
                    "257",
                ])
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("native batch exceeds 1024 completed spans per worker")
            );
            assert!(output.stdout.is_empty());
        }
    }
}

#[test]
fn benchmark_exits_nonzero_when_real_sinks_lose_spans() {
    for mode in ["fast", "direct", "direct-user-fields", "otel"] {
        let output = Command::new(env!("CARGO_BIN_EXE_bench_tracing_backends"))
            .args([
                "--mode",
                mode,
                "--workload",
                "children",
                "--threads",
                "2",
                "--iters",
                "2",
                "--warmup",
                "0",
                "--batch",
                "2",
                "--capacity",
                "1",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "loss must fail for {mode}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("sink count mismatch"));
    }
}
