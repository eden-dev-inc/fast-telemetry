use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_bench_tracing_saturation");

#[test]
fn three_backends_reconcile_sampling_retention_and_trace_bounds() {
    for mode in ["native", "fast", "registry"] {
        for workload in ["root", "children"] {
            let per_trace = if workload == "children" { 4 } else { 1 };
            for (producers, collectors) in [(1, 1), (4, 1), (8, 3)] {
                let output = Command::new(BINARY)
                    .args([
                        mode,
                        workload,
                        &producers.to_string(),
                        &collectors.to_string(),
                        "20",
                        "64",
                    ])
                    .output()
                    .expect("run matched saturation benchmark");
                assert!(
                    output.status.success(),
                    "{mode}/{workload}/{producers}/{collectors}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let stdout = String::from_utf8(output.stdout).expect("CSV");
                let lines: Vec<_> = stdout.lines().collect();
                assert_eq!(lines.len(), 2);
                let row: Vec<_> = lines[1].split(',').collect();
                assert_eq!(row.len(), 17);
                assert_eq!(row[0], mode);
                assert_eq!(row[1], workload);
                assert_eq!(row[5], if mode == "registry" { "false" } else { "true" });
                let seconds = row[6].parse::<f64>().expect("seconds");
                assert!(seconds.is_finite() && seconds >= 0.020);
                let counts: Vec<_> = row[7..15]
                    .iter()
                    .map(|v| v.parse::<u64>().expect("count"))
                    .collect();
                let [
                    attempted,
                    recorded,
                    sampled,
                    roots,
                    spans,
                    missing,
                    lower,
                    upper,
                ] = counts[..]
                else {
                    panic!("eight counters");
                };
                assert_eq!(attempted, recorded + sampled);
                assert_eq!(attempted % 64, 0);
                assert!(lower <= upper && upper <= recorded);
                if mode == "registry" {
                    assert_eq!([roots, spans, missing, lower, upper], [0; 5]);
                } else {
                    assert_eq!(spans + missing, recorded * per_trace);
                    assert!(upper <= roots && upper <= spans / per_trace);
                    if per_trace == 1 {
                        assert_eq!(lower, spans);
                        assert_eq!(upper, spans);
                    }
                }
                if mode != "native" {
                    assert_eq!(sampled, 0);
                }
                for (value, count) in row[15..].iter().zip([attempted, spans]) {
                    let rate = value.parse::<f64>().expect("rate");
                    assert!(rate.is_finite() && rate >= 0.0);
                    assert!((rate - count as f64 / seconds).abs() <= rate * 0.000_001 + 0.001);
                }
            }
        }
    }
}

#[test]
fn invalid_comparison_options_do_not_start_workers() {
    for args in [
        vec![],
        vec!["invalid", "root", "1", "1", "1", "64"],
        vec!["native", "invalid", "1", "1", "1", "64"],
        vec!["native", "root", "0", "1", "1", "64"],
        vec!["fast", "root", "257", "1", "1", "64"],
        vec!["registry", "root", "1", "0", "1", "64"],
        vec!["native", "root", "1", "2", "1", "64"],
        vec!["fast", "root", "1", "1", "0", "64"],
        vec!["registry", "root", "1", "1", "60001", "64"],
        vec!["native", "root", "1", "1", "1", "0"],
        vec!["fast", "root", "1", "1", "1", "65537"],
        vec!["registry", "root", "-1", "1", "1", "64"],
        vec!["native", "root", "1", "1", "invalid", "64"],
        vec!["fast", "root", "1", "1", "18446744073709551616", "64"],
        vec!["registry", "root", "1", "1", "1", "64", "extra"],
    ] {
        let output = Command::new(BINARY)
            .args(args)
            .output()
            .expect("run invalid comparison");
        assert!(!output.status.success());
        assert!(!output.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    }
}
