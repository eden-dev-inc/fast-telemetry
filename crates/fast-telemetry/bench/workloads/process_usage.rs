#[derive(Copy, Clone, Debug)]
pub(crate) struct ProcessCpuSnapshot {
    user_seconds: f64,
    system_seconds: f64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ProcessCpuUsage {
    pub(crate) user_seconds: f64,
    pub(crate) system_seconds: f64,
    pub(crate) total_seconds: f64,
}

impl ProcessCpuSnapshot {
    pub(crate) fn capture() -> std::io::Result<Self> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }

        let usage = unsafe { usage.assume_init() };
        Ok(Self {
            user_seconds: timeval_to_seconds(usage.ru_utime),
            system_seconds: timeval_to_seconds(usage.ru_stime),
        })
    }

    pub(crate) fn elapsed_since(self, start: Self) -> ProcessCpuUsage {
        let user_seconds = (self.user_seconds - start.user_seconds).max(0.0);
        let system_seconds = (self.system_seconds - start.system_seconds).max(0.0);

        ProcessCpuUsage {
            user_seconds,
            system_seconds,
            total_seconds: user_seconds + system_seconds,
        }
    }
}

fn timeval_to_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + (value.tv_usec as f64 / 1_000_000.0)
}
