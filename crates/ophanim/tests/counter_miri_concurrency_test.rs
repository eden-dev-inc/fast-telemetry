#![cfg(miri)]

use ophanim::Counter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn miri_counter_add_vs_sum() {
    let counter = Arc::new(Counter::new(4));
    let start = Arc::new(Barrier::new(3));
    let done = Arc::new(AtomicBool::new(false));

    let writer_counter = Arc::clone(&counter);
    let writer_start = Arc::clone(&start);
    let writer_done = Arc::clone(&done);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for _ in 0..3_000 {
            writer_counter.inc();
            std::thread::yield_now();
        }
        writer_done.store(true, Ordering::Release);
    });

    let reader_counter = Arc::clone(&counter);
    let reader_start = Arc::clone(&start);
    let reader_done = Arc::clone(&done);
    let reader = std::thread::spawn(move || {
        reader_start.wait();
        while !reader_done.load(Ordering::Acquire) {
            let _ = reader_counter.sum();
            std::thread::yield_now();
        }
        let _ = reader_counter.sum();
    });

    start.wait();
    writer.join().unwrap();
    reader.join().unwrap();
    assert_eq!(counter.sum(), 3_000);
}

#[test]
fn miri_counter_add_vs_swap_accounting() {
    let counter = Arc::new(Counter::new(8));
    let start = Arc::new(Barrier::new(3));
    let done = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let swapped_total = Arc::new(AtomicU64::new(0));

    let writer_counter = Arc::clone(&counter);
    let writer_start = Arc::clone(&start);
    let writer_done = Arc::clone(&done);
    let writer_written = Arc::clone(&written);
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for _ in 0..4_000 {
            writer_counter.inc();
            writer_written.fetch_add(1, Ordering::Relaxed);
            std::thread::yield_now();
        }
        writer_done.store(true, Ordering::Release);
    });

    let swap_counter = Arc::clone(&counter);
    let swap_start = Arc::clone(&start);
    let swap_done = Arc::clone(&done);
    let swap_total = Arc::clone(&swapped_total);
    let swapper = std::thread::spawn(move || {
        swap_start.wait();
        while !swap_done.load(Ordering::Acquire) {
            let v = swap_counter.swap();
            if v > 0 {
                swap_total.fetch_add(v as u64, Ordering::Relaxed);
            }
            std::thread::yield_now();
        }
        let final_swap = swap_counter.swap();
        if final_swap > 0 {
            swap_total.fetch_add(final_swap as u64, Ordering::Relaxed);
        }
    });

    start.wait();
    writer.join().unwrap();
    swapper.join().unwrap();

    let accounted = swapped_total.load(Ordering::Relaxed) + counter.sum() as u64;
    assert_eq!(accounted, written.load(Ordering::Relaxed));
}
