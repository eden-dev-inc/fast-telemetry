// Bench-local thread affinity helpers.
//
// This is intentionally small and self-contained:
// - Used only by benchmark workload binaries.
// - Linux-only affinity calls; non-Linux platforms no-op.
// - Best-effort behavior: pinning failures do not fail the benchmark run.
//
// Motivation:
// Benchmarks can become bimodal when threads migrate across cores mid-run,
// especially on asymmetric (non-symmetric) CPU topologies where logical CPUs
// do not have equivalent performance characteristics (for example P/E-core
// designs or mixed-capacity systems such as DGX Spark-class hosts).
// Optional per-thread pinning gives us a more deterministic mode for
// publishable comparisons while keeping default scheduler-driven behavior.

#[derive(Copy, Clone)]
pub enum ThreadAffinityMode {
    /// Do not pin worker threads; leave placement to OS scheduler.
    Off,
    /// Pin worker thread `t` to `allowed_cpus[t % allowed_cpus.len()]`.
    ///
    /// `allowed_cpus` is discovered from current process affinity mask, so this
    /// composes correctly with `taskset`/cpuset/cgroup constraints.
    RoundRobin,
}

impl ThreadAffinityMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "off" => Some(Self::Off),
            "round_robin" | "rr" => Some(Self::RoundRobin),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::RoundRobin => "round_robin",
        }
    }
}

pub fn pin_worker_thread(_worker_index: usize, mode: ThreadAffinityMode) {
    // Best-effort by design: benchmark should still run even if affinity is
    // unavailable or denied by the host.
    match mode {
        ThreadAffinityMode::Off => {}
        ThreadAffinityMode::RoundRobin => {
            #[cfg(target_os = "linux")]
            {
                if let Some(cpu) = next_cpu(_worker_index) {
                    let _ = pin_current_thread_to_cpu(cpu);
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn next_cpu(worker_index: usize) -> Option<usize> {
    use std::sync::OnceLock;
    // Discover once and reuse across all worker spawns in this process.
    static CPUS: OnceLock<Vec<usize>> = OnceLock::new();
    let cpus = CPUS.get_or_init(|| discover_allowed_cpus().unwrap_or_default());
    if cpus.is_empty() {
        None
    } else {
        Some(cpus[worker_index % cpus.len()])
    }
}

#[cfg(target_os = "linux")]
fn discover_allowed_cpus() -> std::io::Result<Vec<usize>> {
    // Use sched_getaffinity so we honor parent-level restrictions (taskset,
    // container cpuset, cgroup limits) instead of assuming all host CPUs.
    let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let result = unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut cpuset) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }

    let mut cpus = Vec::new();
    for cpu in 0..(libc::CPU_SETSIZE as usize) {
        if unsafe { libc::CPU_ISSET(cpu, &cpuset) } {
            cpus.push(cpu);
        }
    }
    Ok(cpus)
}

#[cfg(target_os = "linux")]
fn pin_current_thread_to_cpu(cpu: usize) -> std::io::Result<()> {
    // Bind current pthread to one logical CPU to reduce migration jitter.
    let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(cpu, &mut cpuset);
        let thread = libc::pthread_self();
        let result = libc::pthread_setaffinity_np(thread, std::mem::size_of::<libc::cpu_set_t>(), &cpuset);
        if result != 0 {
            return Err(std::io::Error::from_raw_os_error(result));
        }
    }
    Ok(())
}
