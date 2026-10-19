#![cfg(all(target_os = "linux", feature = "bench-tools"))]

use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_bench_span_propagation");

#[test]
fn native_propagation_benchmark_verifies_complete_and_partial_batches() {
    for mode in [
        "parse",
        "header",
        "trace_display",
        "span_display",
        "root_header",
        "remote_header",
    ] {
        for iterations in [1, 17, 257] {
            let output = Command::new(BINARY)
                .args([mode, &iterations.to_string()])
                .output()
                .expect("run propagation benchmark");
            assert!(
                output.status.success(),
                "{mode}/{iterations}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).expect("benchmark CSV");
            let lines: Vec<_> = stdout.lines().collect();
            assert_eq!(lines.len(), 2);
            assert_eq!(
                lines[0],
                "mode,iterations,cpu_ns_per_operation,retained_spans,sampled_out,dropped"
            );
            let values: Vec<_> = lines[1].split(',').collect();
            assert_eq!(values.len(), 6);
            assert_eq!(values[0], mode);
            assert_eq!(values[1].parse::<usize>().expect("iterations"), iterations);
            let cpu = values[2].parse::<f64>().expect("CPU time");
            assert!(cpu.is_finite() && cpu >= 0.0);
            assert_eq!(
                values[3].parse::<usize>().expect("retained count"),
                if mode.ends_with("_header") {
                    iterations
                } else {
                    0
                }
            );
            assert_eq!(values[4], "0");
            assert_eq!(values[5], "0");
        }
    }
}

#[test]
fn native_propagation_benchmark_rejects_invalid_arguments() {
    for args in [
        vec![],
        vec!["unknown", "1"],
        vec!["header", "0"],
        vec!["parse", "0"],
        vec!["parse", "-1"],
        vec!["header", "-1"],
        vec!["header", "invalid"],
        vec!["header", "1", "extra"],
    ] {
        let output = Command::new(BINARY)
            .args(args)
            .output()
            .expect("run invalid benchmark");
        assert!(!output.status.success());
        assert!(!output.stderr.is_empty());
    }
}
