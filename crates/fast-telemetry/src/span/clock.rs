//! Unix timestamps anchored to a monotonic clock at initialization.
//!
//! Linux with 64-bit pointers reads `CLOCK_MONOTONIC` directly and computes the
//! offset in integer nanoseconds. This uses the same OS clock as `Instant` while
//! avoiding intermediate `Duration` construction and out-of-line subtraction.
//! Other targets retain the portable `Instant` implementation. Neither path
//! caches readings, changes clock resolution, or calibrates a hardware counter.

use std::time::{SystemTime, UNIX_EPOCH};

pub(super) use platform::now_nanos;

/// Capture wall time once; later timestamps follow only the monotonic clock.
fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
mod platform {
    //! Linux monotonic timespec with a precomputed Unix nanosecond offset.

    use std::sync::LazyLock;

    /// One wall/monotonic pair and its precomputed modular nanosecond offset.
    struct Anchor {
        mono: libc::timespec,
        wall: u64,
        offset_ns: u64,
    }

    impl Anchor {
        fn new(mono: libc::timespec, wall: u64) -> Self {
            Self {
                mono,
                wall,
                offset_ns: wall.wrapping_sub(timespec_nanos(mono)),
            }
        }

        #[inline]
        fn timestamp(&self, end: libc::timespec) -> u64 {
            if end.tv_sec < self.mono.tv_sec
                || (end.tv_sec == self.mono.tv_sec && end.tv_nsec < self.mono.tv_nsec)
            {
                return self.wall;
            }
            let timestamp = timespec_nanos(end).wrapping_add(self.offset_ns);
            // Keep ordinary addition so the caller's overflow-check setting is
            // preserved. With wrapping addition the optimizer cancels this pair.
            self.wall + timestamp.wrapping_sub(self.wall)
        }
    }

    /// Convert normalized timespec fields modulo u64, matching Duration's cast.
    #[inline]
    fn timespec_nanos(time: libc::timespec) -> u64 {
        (time.tv_sec as u64)
            .wrapping_mul(1_000_000_000)
            .wrapping_add(time.tv_nsec as u64)
    }

    static ANCHOR: LazyLock<Anchor> = LazyLock::new(|| {
        let wall = super::unix_nanos();
        Anchor::new(monotonic_timespec(), wall)
    });

    /// Read the OS monotonic clock, preserving its resolution and ordering.
    ///
    /// # Panics
    /// Panics if the OS clock read fails, as does `Instant::now` on Linux.
    #[inline]
    fn monotonic_timespec() -> libc::timespec {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `time` is an initialized, aligned, writable timespec and the
        // OS retains no pointer. CLOCK_MONOTONIC is supported on Linux.
        let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) };
        if result != 0 {
            clock_read_failed();
        }
        time
    }

    #[cold]
    fn clock_read_failed() -> ! {
        panic!(
            "monotonic clock read failed: {}",
            std::io::Error::last_os_error()
        );
    }

    /// Current Unix nanoseconds from a fixed wall/monotonic anchor.
    #[inline]
    pub(crate) fn now_nanos() -> u64 {
        ANCHOR.timestamp(monotonic_timespec())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn check_anchor(start: (i64, i64), end: (i64, i64), wall: u64) {
            let expected = ((i128::from(end.0) - i128::from(start.0)) * 1_000_000_000
                + i128::from(end.1)
                - i128::from(start.1))
            .max(0) as u64;
            let start = libc::timespec {
                tv_sec: start.0,
                tv_nsec: start.1,
            };
            let end = libc::timespec {
                tv_sec: end.0,
                tv_nsec: end.1,
            };
            if wall.checked_add(expected).is_none() {
                return;
            }
            assert_eq!(
                Anchor::new(start, wall).timestamp(end),
                wall.wrapping_add(expected)
            );
        }

        #[test]
        fn precomputed_anchor_matches_wide_timestamp_reference() {
            let mut state = 0x94d0_49bb_1331_11eb_u64;
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            for _ in 0..10000 {
                let start = (next() as i64, (next() % 1_000_000_000) as i64);
                let end = (next() as i64, (next() % 1_000_000_000) as i64);
                check_anchor(start, end, next());
            }
            for start in [
                (i64::MIN, 999_999_999),
                (-10, 0),
                (0, 0),
                (1, 999_999_999),
                (i64::MAX, 0),
            ] {
                for end in [
                    (i64::MIN, 0),
                    (-1, 999_999_999),
                    (0, 1),
                    (2, 0),
                    (i64::MAX, 999_999_999),
                ] {
                    for wall in [0, 1, u64::MAX] {
                        check_anchor(start, end, wall);
                    }
                }
            }
        }

        #[test]
        fn precomputed_anchor_preserves_wall_add_overflow_behavior() {
            let anchor = Anchor::new(
                libc::timespec {
                    tv_sec: 1,
                    tv_nsec: 0,
                },
                u64::MAX,
            );
            let end = libc::timespec {
                tv_sec: 1,
                tv_nsec: 1,
            };
            let result = std::panic::catch_unwind(|| anchor.timestamp(end));
            let expected = std::panic::catch_unwind(|| {
                std::hint::black_box(u64::MAX) + std::hint::black_box(1_u64)
            });
            match expected {
                Ok(expected) => assert_eq!(result.expect("matching wall addition"), expected),
                Err(_) => assert!(result.is_err()),
            }
        }

        fn check(start: (i64, i64), end: (i64, i64)) {
            let expected = ((i128::from(end.0) - i128::from(start.0)) * 1_000_000_000
                + i128::from(end.1)
                - i128::from(start.1))
            .max(0) as u64;
            assert_eq!(
                Anchor::new(
                    libc::timespec {
                        tv_sec: start.0,
                        tv_nsec: start.1,
                    },
                    0,
                )
                .timestamp(libc::timespec {
                    tv_sec: end.0,
                    tv_nsec: end.1,
                }),
                expected,
                "start={start:?}, end={end:?}"
            );
        }

        #[test]
        fn elapsed_conversion_handles_borrow_backwards_and_wide_seconds() {
            for (start, end) in [
                ((0, 0), (0, 0)),
                ((1, 0), (1, 1)),
                ((1, 2), (1, 1)),
                ((1, 999_999_999), (2, 0)),
                ((1, 0), (2, 999_999_999)),
                ((2, 0), (1, 999_999_999)),
                ((-10, 5), (2, 0)),
                ((2, 0), (-10, 5)),
                ((0, 0), (i64::MAX, 999_999_999)),
                ((i64::MIN, 999_999_999), (i64::MAX, 0)),
                ((i64::MAX, 0), (i64::MIN, 999_999_999)),
            ] {
                check(start, end);
            }
        }

        #[test]
        fn elapsed_conversion_matches_wide_reference() {
            let mut state = 0x9e37_79b9_7f4a_7c15u64;
            let mut next = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            for _ in 0..10_000 {
                let start = (next() as i64, (next() % 1_000_000_000) as i64);
                let end = (next() as i64, (next() % 1_000_000_000) as i64);
                check(start, end);
            }
        }
    }
}

#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
mod platform {
    //! Portable monotonic anchor for targets without the Linux fast path.

    use std::sync::LazyLock;
    use std::time::Instant;

    static ANCHOR: LazyLock<(Instant, u64)> = LazyLock::new(|| {
        let wall = super::unix_nanos();
        (Instant::now(), wall)
    });

    /// Current Unix nanoseconds from the portable monotonic clock anchor.
    #[inline]
    pub(crate) fn now_nanos() -> u64 {
        let (mono, wall) = *ANCHOR;
        wall + mono.elapsed().as_nanos() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn timestamp_reads_are_nondecreasing() {
        let mut previous = now_nanos();
        assert!(previous > 0);
        for _ in 0..10_000 {
            let current = now_nanos();
            assert!(current >= previous);
            previous = current;
        }
    }

    #[test]
    fn timestamp_deltas_match_monotonic_brackets() {
        let _ = now_nanos();
        for _ in 0..64 {
            let start_before = Instant::now();
            let start = now_nanos();
            let start_after = Instant::now();
            std::hint::black_box(start);
            let end_before = Instant::now();
            let end = now_nanos();
            let end_after = Instant::now();
            let delta = u128::from(end - start);
            // Portable clock conversions may round at different origins.
            let tolerance = 1_000;
            assert!(delta + tolerance >= end_before.duration_since(start_after).as_nanos());
            assert!(delta <= end_after.duration_since(start_before).as_nanos() + tolerance);
        }
    }

    #[test]
    fn timestamps_remain_ordered_across_thread_handoffs() {
        let (outgoing, incoming) = std::sync::mpsc::sync_channel(0);
        let (reply, replies) = std::sync::mpsc::sync_channel(0);
        std::thread::scope(|scope| {
            let worker = scope.spawn(move || {
                for previous in incoming {
                    let current = now_nanos();
                    assert!(current >= previous);
                    reply.send(current).expect("publish worker timestamp");
                }
            });
            for _ in 0..128 {
                outgoing.send(now_nanos()).expect("send timestamp");
                let previous = replies.recv().expect("receive worker timestamp");
                assert!(now_nanos() >= previous);
            }
            drop(outgoing);
            worker.join().expect("join timestamp worker");
        });
    }
}
