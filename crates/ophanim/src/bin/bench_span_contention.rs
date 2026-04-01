#[cfg(feature = "bench-tools")]
include!("../../bench/workloads/span_contention.rs");

#[cfg(not(feature = "bench-tools"))]
fn main() {
    eprintln!("bench_span_contention requires --features bench-tools");
    std::process::exit(1);
}
