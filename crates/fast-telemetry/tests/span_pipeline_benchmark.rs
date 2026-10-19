#![cfg(all(target_os = "linux", feature = "bench-tools", feature = "otlp"))]

use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_bench_span_pipeline");

#[test]
fn native_pipeline_benchmark_verifies_all_modes_and_retention_boundaries() {
    for encoding in ["record", "native", "protobuf"] {
        for workload in ["root", "attributes", "children"] {
            let per_trace = if workload == "children" { 4 } else { 1 };
            let maximum = 1024 / per_trace;
            for (batch, cycles) in [(1, 2), (65, 2), (128, 1), (maximum, 1)] {
                let output = Command::new(BINARY)
                    .args([encoding, workload, &batch.to_string(), &cycles.to_string()])
                    .output()
                    .expect("run native pipeline benchmark");
                assert!(
                    output.status.success(),
                    "{encoding}/{workload}/{batch}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let stdout = String::from_utf8(output.stdout).expect("benchmark CSV");
                let lines: Vec<_> = stdout.lines().collect();
                assert_eq!(lines.len(), 2);
                assert_eq!(
                    lines[0],
                    "encoding,workload,batch,cycles,verified_traces,verified_spans,sampled_out,dropped,wire_bytes,cpu_ns_per_trace,cpu_ns_per_span"
                );
                let values: Vec<_> = lines[1].split(',').collect();
                assert_eq!(values.len(), 11);
                assert_eq!(values[0], encoding);
                assert_eq!(values[1], workload);
                assert_eq!(values[2].parse::<usize>().expect("batch"), batch);
                assert_eq!(values[3].parse::<usize>().expect("cycles"), cycles);
                assert_eq!(values[4].parse::<usize>().expect("traces"), batch * cycles);
                assert_eq!(
                    values[5].parse::<usize>().expect("spans"),
                    batch * cycles * per_trace
                );
                assert_eq!(values[6], "0");
                assert_eq!(values[7], "0");
                let wire_bytes = values[8].parse::<usize>().expect("wire bytes");
                if encoding == "record" {
                    assert_eq!(wire_bytes, 0);
                } else {
                    assert!(wire_bytes > 0);
                }
                let trace_cpu = values[9].parse::<f64>().expect("CPU per trace");
                let span_cpu = values[10].parse::<f64>().expect("CPU per span");
                assert!(trace_cpu.is_finite() && trace_cpu >= 0.0);
                assert!(span_cpu.is_finite() && span_cpu >= 0.0);
                assert!((trace_cpu - span_cpu * per_trace as f64).abs() <= 0.01);
            }
        }
    }
}

#[test]
fn native_pipeline_benchmark_rejects_invalid_arguments_before_recording() {
    let cases = [
        vec![],
        vec!["unknown", "root", "1", "1"],
        vec!["native", "unknown", "1", "1"],
        vec!["native", "root", "0", "1"],
        vec!["native", "root", "1", "0"],
        vec!["native", "root", "-1", "1"],
        vec!["native", "root", "1", "-1"],
        vec!["native", "root", "invalid", "1"],
        vec!["native", "root", "1", "1", "extra"],
        vec!["native", "root", "1025", "1"],
        vec!["native", "children", "257", "1"],
    ];
    let overflow = usize::MAX.to_string();
    for args in cases.into_iter().chain([
        vec!["native", "root", "2", overflow.as_str()],
        vec!["native", "children", "1", overflow.as_str()],
    ]) {
        let output = Command::new(BINARY)
            .args(args)
            .output()
            .expect("run invalid pipeline benchmark");
        assert!(!output.status.success());
        assert!(!output.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    }
}
