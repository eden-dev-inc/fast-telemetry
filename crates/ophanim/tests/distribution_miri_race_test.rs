#![cfg(miri)]

use ophanim::Distribution;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn miri_detects_stats_read_write_race() {
    let dist = Arc::new(Distribution::new(1));
    let start = Arc::new(Barrier::new(3));
    let done = Arc::new(AtomicBool::new(false));

    let writer_dist = Arc::clone(&dist);
    let writer_start = Arc::clone(&start);
    let writer_done = Arc::clone(&done);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for i in 0..2_000u64 {
            writer_dist.record(i);
            if i % 8 == 0 {
                std::thread::yield_now();
            }
        }
        writer_done.store(true, Ordering::Release);
    });

    let reader_dist = Arc::clone(&dist);
    let reader_start = Arc::clone(&start);
    let reader_done = Arc::clone(&done);
    let reader = std::thread::spawn(move || {
        reader_start.wait();
        while !reader_done.load(Ordering::Acquire) {
            let _ = reader_dist.count();
            let _ = reader_dist.sum();
            let _ = reader_dist.min();
            let _ = reader_dist.max();
            std::thread::yield_now();
        }
    });

    start.wait();
    writer.join().unwrap();
    reader.join().unwrap();
}

#[test]
fn miri_detects_race_while_exporter_thread_reads_stats() {
    let dist = Arc::new(Distribution::new(1));
    let start = Arc::new(Barrier::new(3));
    let done = Arc::new(AtomicBool::new(false));

    let writer_dist = Arc::clone(&dist);
    let writer_start = Arc::clone(&start);
    let writer_done = Arc::clone(&done);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for i in 0..2_000u64 {
            writer_dist.record(i % 100);
            if i % 16 == 0 {
                std::thread::yield_now();
            }
        }
        writer_done.store(true, Ordering::Release);
    });

    let exporter_dist = Arc::clone(&dist);
    let exporter_start = Arc::clone(&start);
    let exporter_done = Arc::clone(&done);
    let exporter = std::thread::spawn(move || {
        exporter_start.wait();
        while !exporter_done.load(Ordering::Acquire) {
            let _ = exporter_dist.count();
            let _ = exporter_dist.sum();
            let _ = exporter_dist.buckets_snapshot();
            std::thread::yield_now();
        }
    });

    start.wait();
    writer.join().unwrap();
    exporter.join().unwrap();
}
