use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_bench_span_saturation");

#[test]
fn saturation_benchmark_accounts_for_every_trace_and_span() {
    for workload in ["root", "children"] {
        let per_trace = if workload == "children" { 4 } else { 1 };
        for (producers, collectors) in [(1, 1), (4, 1), (4, 2), (4, 4), (8, 3)] {
            for burst in [1, 64] {
                let output = Command::new(BINARY)
                    .args([
                        workload,
                        &producers.to_string(),
                        &collectors.to_string(),
                        "20",
                        &burst.to_string(),
                    ])
                    .output()
                    .expect("run saturation benchmark");
                assert!(
                    output.status.success(),
                    "{workload}/{producers}/{collectors}/{burst}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let stdout = String::from_utf8(output.stdout).expect("CSV");
                let lines: Vec<_> = stdout.lines().collect();
                assert_eq!(lines.len(), 2);
                assert_eq!(
                    lines[0],
                    "workload,producers,collectors,duration_ms,burst_traces,elapsed_seconds,attempted_traces,recorded_traces,sampled_out_traces,collected_root_spans,collected_spans,uncollected_spans,complete_trace_lower_bound,complete_trace_upper_bound,attempted_traces_per_second,complete_trace_lower_bound_per_second,collected_spans_per_second"
                );
                let row: Vec<_> = lines[1].split(',').collect();
                assert_eq!(row.len(), 17);
                assert_eq!(row[0], workload);
                assert_eq!(row[1].parse::<usize>().expect("producers"), producers);
                assert_eq!(row[2].parse::<usize>().expect("collectors"), collectors);
                assert_eq!(row[3], "20");
                assert_eq!(row[4].parse::<usize>().expect("burst"), burst);
                let seconds = row[5].parse::<f64>().expect("seconds");
                assert!(seconds.is_finite() && seconds >= 0.020);
                let counts: Vec<_> = row[6..14]
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
                assert!(lower <= upper && upper <= roots && upper <= spans / per_trace);
                assert_eq!(spans + missing, recorded * per_trace);
                if per_trace == 1 {
                    assert_eq!(lower, spans);
                    assert_eq!(upper, spans);
                }
                assert_eq!(attempted % burst as u64, 0);
                for (value, count) in row[14..].iter().zip([attempted, lower, spans]) {
                    let rate = value.parse::<f64>().expect("rate");
                    assert!(rate.is_finite() && rate >= 0.0);
                    assert!((rate - count as f64 / seconds).abs() <= rate * 0.000_001 + 0.001);
                }
            }
        }
    }
}

#[test]
fn saturation_benchmark_rejects_invalid_arguments_before_starting_workers() {
    for args in [
        vec![],
        vec!["unknown", "1", "1", "1", "64"],
        vec!["root", "0", "1", "1", "64"],
        vec!["root", "257", "1", "1", "64"],
        vec!["root", "1", "0", "1", "64"],
        vec!["root", "1", "2", "1", "64"],
        vec!["root", "1", "1", "0", "64"],
        vec!["root", "1", "1", "60001", "64"],
        vec!["root", "1", "1", "1", "0"],
        vec!["root", "1", "1", "1", "65537"],
        vec!["root", "-1", "1", "1", "64"],
        vec!["root", "1", "1", "invalid", "64"],
        vec!["root", "1", "1", "1", "64", "extra"],
        vec!["root", "1", "1", "18446744073709551616", "64"],
    ] {
        let output = Command::new(BINARY)
            .args(args)
            .output()
            .expect("run invalid saturation configuration");
        assert!(!output.status.success());
        assert!(!output.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    }
}
