#![cfg(all(target_os = "linux", feature = "bench-tools", feature = "otlp"))]

use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_bench_span_export");

#[test]
fn span_export_benchmark_verifies_all_encoding_modes() {
    for mode in ["borrow", "consume", "native"] {
        for workload in ["static", "owned", "attributes"] {
            for batch in [1, 65] {
                let output = Command::new(BINARY)
                    .args([mode, workload, &batch.to_string(), "2"])
                    .output()
                    .expect("run export benchmark");
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let stdout = String::from_utf8(output.stdout).expect("benchmark CSV");
                let lines: Vec<_> = stdout.lines().collect();
                assert_eq!(lines.len(), 2);
                assert_eq!(
                    lines[0],
                    "mode,workload,batch,cycles,verified_spans,wire_bytes,cpu_ns_per_span"
                );
                let values: Vec<_> = lines[1].split(',').collect();
                assert_eq!(values.len(), 7);
                assert_eq!(values[0], mode);
                assert_eq!(values[1], workload);
                assert_eq!(values[2].parse::<usize>().expect("batch"), batch);
                assert_eq!(values[3], "2");
                assert_eq!(values[4].parse::<usize>().expect("spans"), batch * 2);
                assert!(values[5].parse::<usize>().expect("wire bytes") > 0);
                let cpu = values[6].parse::<f64>().expect("CPU time");
                assert!(cpu.is_finite() && cpu >= 0.0);
            }
        }
    }
}

#[test]
fn span_export_benchmark_rejects_invalid_arguments() {
    for args in [
        vec![],
        vec!["unknown", "static", "1", "1"],
        vec!["borrow", "unknown", "1", "1"],
        vec!["borrow", "static", "0", "1"],
        vec!["borrow", "static", "1", "0"],
        vec!["borrow", "static", "-1", "1"],
        vec!["borrow", "static", "invalid", "1"],
    ] {
        let output = Command::new(BINARY)
            .args(args)
            .output()
            .expect("run invalid benchmark");
        assert!(!output.status.success());
        assert!(!output.stderr.is_empty());
    }
}
