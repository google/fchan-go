#![feature(test)]
extern crate fchan;
extern crate time;
extern crate crossbeam;
extern crate test;
use crossbeam::sync;
use crossbeam::mem::epoch;
use std::thread;
use std::sync::{Barrier, Arc};

const DBG_PRINT: bool = false;

macro_rules! dbg_print {
    ($($e:expr),*) => {
        if DBG_PRINT {
            println!($($e),*);
        }
    }
}

// Some traits allowing MsQueue and UnboundedChannel to be benchmarked using the same code.
trait MakesChan<'a, T> {
    type C: ChanLike<T>;
    fn new() -> Self;
    fn new_handle(&'a self) -> Self::C;
}

impl<'a, T: 'a> MakesChan<'a, T> for fchan::UnboundedChannel<T> {
    type C = fchan::Handle<'a, T>;
    fn new() -> Self {
        fchan::UnboundedChannel::new()
    }

    fn new_handle(&'a self) -> fchan::Handle<'a, T> {
        self.new_handle()
    }
}

impl<'a, T> MakesChan<'a, T> for Arc<sync::MsQueue<T>> {
    type C = Self;
    fn new() -> Self {
        Arc::new(sync::MsQueue::new())
    }
    fn new_handle(&self) -> Arc<sync::MsQueue<T>> {
        self.clone()
    }
}

trait ChanLike<T> {
    fn send(&mut self, T);
    fn receive(&mut self) -> T;
}

impl<'a, T> ChanLike<T> for fchan::Handle<'a, T> {
    fn send(&mut self, t: T) {
        self.send(t);
    }
    fn receive(&mut self) -> T {
        self.receive()
    }
}

impl<T> ChanLike<T> for Arc<sync::MsQueue<T>> {
    fn send(&mut self, t: T) {
        self.push(t);
    }
    fn receive(&mut self) -> T {
        self.pop()
    }
}


fn benchmark_chan<M: for<'a> MakesChan<'a, usize> + Send + Sync + 'static>(threads: usize,
                                                                           ops_per_thread: usize,
                                                                           iters: usize)
                                                                           -> f64 {
    let mut times = Vec::new();
    for _i in 0..iters {
        // try and free some spare memory
        for _i in 0..10 {
            let _ = epoch::pin();
        }
        let pair = Arc::new((M::new(), Barrier::new(threads + 1)));
        let mut thrs = Vec::new();
        for _t in 0..threads / 2 {
            let pair1 = pair.clone();
            thrs.push(thread::spawn(move || {
                let &(ref ch, ref bar) = &*pair1;
                bar.wait();
                dbg_print!("send {} started", _t);
                let mut handle = ch.new_handle();
                for j in 0..ops_per_thread {
                    handle.send(j);
                }
            }));
            let pair2 = pair.clone();
            thrs.push(thread::spawn(move || {
                let &(ref ch, ref bar) = &*pair2;
                bar.wait();
                dbg_print!("receive {} started", _t);
                let mut handle = ch.new_handle();
                for _j in 0..ops_per_thread {
                    let _ = test::black_box(handle.receive());
                }
            }));
        }
        pair.1.wait();
        dbg_print!("started");
        let start = time::precise_time_ns();
        for t in thrs.into_iter() {
            if let Err(e) = t.join() {
                println!("Thread paniced {:?}", e);
            }
        }
        times.push(time::precise_time_ns() - start);
        dbg_print!("done");
    }
    let mut res: f64 = 0.0;
    for t in times.iter() {
        res += *t as f64;
    }
    return res / (iters as f64);
}

fn render_benchmark<M: for<'a> MakesChan<'a, usize> + Send + Sync + 'static>(
                        threads: usize,
                        ops_per_thread: usize,
                        iters: usize,
                        header: &str) {
    let result_secs = benchmark_chan::<M>(threads, ops_per_thread, iters) / 1e9;
    let total_ops = (ops_per_thread * threads) as f64;
    println!("{}-n{}\t{} ops/s", header, threads, total_ops / result_secs);
}

fn render_bench_msqueue(threads: usize, ops_per_thread: usize, iters: usize) {
    render_benchmark::<Arc<sync::MsQueue<usize>>>(threads, ops_per_thread, iters, "MsQueue")
}

fn render_bench_fchan(threads: usize, ops_per_thread: usize, iters: usize) {
    render_benchmark::<fchan::UnboundedChannel<usize>>(threads, ops_per_thread, iters, "fchan")
}

fn main() {
    let per_thread = 1 << 20;
    let num_iters = 5;
    let num_threads = vec![2, 4, 8, 12, 16, 24, 32];
    for t in num_threads.iter() {
        render_bench_fchan(*t, per_thread, num_iters);
    }
    for t in num_threads.iter() {
        render_bench_msqueue(*t, per_thread, num_iters);
    }
}
