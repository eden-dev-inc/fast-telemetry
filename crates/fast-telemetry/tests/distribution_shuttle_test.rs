#![cfg(feature = "shuttle-tests")]

use fast_telemetry::Distribution;
use shuttle::sync::Arc;
use shuttle::thread;

#[test]
fn shuttle_concurrent_record_accounting() {
    shuttle::check_random(
        || {
            let dist = Arc::new(Distribution::new(1));

            let mut joins = Vec::new();
            for writer_id in 0..2u64 {
                let d = Arc::clone(&dist);
                joins.push(thread::spawn(move || {
                    for i in 0..3u64 {
                        d.record(writer_id * 10 + i);
                        thread::yield_now();
                    }
                }));
            }

            for j in joins {
                j.join().unwrap();
            }

            assert_eq!(dist.count(), 6);
            assert_eq!(dist.sum(), 36);
        },
        500,
    );
}
