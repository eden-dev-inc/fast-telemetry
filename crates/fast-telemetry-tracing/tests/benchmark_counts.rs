#![cfg(feature = "bench-tools")]

use std::process::Command;

#[test]
fn benchmark_checks_actual_completed_sinks_for_every_mode_and_workload() {
    for mode in ["fast", "otel", "disabled"] {
        for workload in ["root", "attributes", "children"] {
            let output = Command::new(env!("CARGO_BIN_EXE_bench_tracing_backends"))
                .args([
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
            let columns: Vec<_> = csv.lines().nth(1).unwrap().split(',').collect();
            let spans: u64 = columns[8].parse().unwrap();
            let events: u64 = columns[9].parse().unwrap();
            let multiplier = match workload {
                "root" => 1,
                "attributes" => 2,
                _ => 4,
            };
            assert_eq!(
                spans,
                if mode == "disabled" {
                    0
                } else {
                    14 * multiplier
                }
            );
            assert_eq!(
                events,
                if mode != "disabled" && workload == "attributes" {
                    14
                } else {
                    0
                }
            );
            assert_eq!(columns[10], "0");
        }
    }
}

#[test]
fn benchmark_exits_nonzero_when_real_sinks_lose_spans() {
    for mode in ["fast", "otel"] {
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
