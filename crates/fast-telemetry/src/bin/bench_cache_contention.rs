#[cfg(feature = "bench-tools")]
include!("../../bench/workloads/cache_contention.rs");

#[cfg(not(feature = "bench-tools"))]
fn main() {
    eprintln!("bench_cache_contention requires --features bench-tools");
    std::process::exit(1);
}
