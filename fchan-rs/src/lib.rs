extern crate crossbeam;
use crossbeam::mem::epoch;
use crossbeam::mem::epoch::{Atomic, Guard, Owned, Shared};
use std::sync::atomic::{AtomicUsize, AtomicBool, fence};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};
use std::sync::{Mutex, Condvar};
use std::mem;
use std::ptr;

const SEG_SHIFT: usize = 12;
const SEG_SIZE: usize = 1 << SEG_SHIFT;
const ADJUST_EVERY: usize = SEG_SIZE >> 5;

/// A `Waiter` is an object that allows for one value to be sent on it, and for that value to later
/// be received. Any `send`s subsequent to the first one have no effect. Receive operations `wait`
/// for a value to be present.
struct Waiter<T> {
    val: T,
    mtx: Mutex<bool>,
    cv: Condvar,
    ctr: AtomicUsize,
}

impl<T> Waiter<T> {
    fn new() -> Waiter<T> {
        Waiter {
            val: unsafe { mem::uninitialized() },
            mtx: Mutex::new(false),
            cv: Condvar::new(),
            ctr: AtomicUsize::new(0),
        }
    }

    fn send(&self, t: T) {
        if self.ctr.compare_and_swap(0, 1, Relaxed) != 0 {
            return;
        }
        // we implicitly lock access to `val` by winning the CAS, so casting away immutability is
        // safe.
        unsafe {
            (self as *const Waiter<_> as *mut Waiter<_>).as_mut().unwrap().val = t;
        }
        // TODO: do we need a barrier here? or does mutex acquisition guarantee a write barrier?
        // ignoring poison errors for now...
        *(self.mtx.lock().unwrap()) = true;
        self.cv.notify_all();
    }

    // wait can only be performed once, as it transfers ownership of the value out of the waiter,
    // without modifying the memory itself.
    unsafe fn wait(&self) -> T {
        let mut started = self.mtx.lock().unwrap();
        while !*started {
            started = self.cv.wait(started).unwrap();
        }
        ptr::read_volatile(&self.val as *const T)
    }
}

/// The contents of the `data` array in a segment. May contain either a waiter or an actual value.
enum SegCell<T> {
    Val(T),
    Wait(Waiter<T>),
}

impl<T> SegCell<T> {
    pub fn waiter(&self) -> &Waiter<T> {
        if let &SegCell::Wait(ref t) = self {
            return t;
        }
        panic!("expected water, found value")
    }
    pub fn val(&self) -> &T {
        if let &SegCell::Val(ref t) = self {
            return t;
        }
        panic!("expected value, found waiter")
    }
}


struct Segment<T> {
    id: AtomicUsize,
    data: [Atomic<SegCell<T>>; SEG_SIZE],
    next: Atomic<Segment<T>>,
}

impl<T> Segment<T> {
    fn new(id: usize) -> Segment<T> {
        Segment {
            id: AtomicUsize::new(id),
            // XXX: assumes Atomic is literally a pointer, with null being invalid, though this
            // will fail to compile if Atomic is not pointer-size, if a version of atomic stored
            // additional metadata in low-order bits we would be in trouble...
            data: unsafe { mem::transmute([0 as usize; SEG_SIZE]) },
            next: Atomic::null(),
        }
    }

    fn find_cell<'b: 'a, 'a>(&'b self,
                             ix: usize,
                             g: &'a Guard,
                             cache: &mut Option<Owned<Segment<T>>>)
                             -> &'a Atomic<SegCell<T>> {
        let (id, seg_ix) = (ix >> SEG_SHIFT, ix & (SEG_SIZE - 1));
        let mut cur_seg = self;
        while cur_seg.id.load(Acquire) < id {
            if let Some(s) = cur_seg.next.load(Relaxed, g) {
                cur_seg = &*s;
                continue;
            }
            let new_seg = cache.take()
                .unwrap_or_else(|| Owned::new(Segment::new(0)));
            new_seg.id.store(cur_seg.id.load(Relaxed) + 1, Relaxed);
            fence(Acquire);
            match cur_seg.next.cas_and_ref(None, new_seg, Release, g) {
                Ok(shared) => cur_seg = &*shared,
                Err(n) => {
                    *cache = Some(n);
                }
            }
        }
        unsafe { cur_seg.data.get_unchecked(seg_ix) }
    }
}

/// Handle's are the standard mechanism for interacting with an `UnboundedChannel`. It includes
/// public `send` and `receive` methods implementing standard (unbounded, asynchronous) channel
/// operations.
pub struct Handle<'a, T: 'a> {
    ctr: usize,
    ch: &'a UnboundedChannel<T>,
    cache: Option<Owned<Segment<T>>>,
}

impl<'a, T: 'a> Handle<'a, T> {
    #[inline]
    fn maybe_adjust(&mut self) {
        if self.ctr & (ADJUST_EVERY - 1) == 0 {
            self.ch.adjust();
        }
        self.ctr += 1;
    }

    pub fn send(&mut self, t: T) {
        self.maybe_adjust();
        self.ch.send(t, &mut self.cache)
    }

    pub fn receive(&mut self) -> T {
        self.maybe_adjust();
        self.ch.receive(&mut self.cache)
    }
}

/// An `UnboundedChannel` is a multi-producer multi-consumer channel. It provides "asynchronous"
/// semantics in that `send`s on the channel may not block, though `receive`s will if there is no
/// available element to receive.
///
/// Callers interact with an `UnboundedChannel` through derivative `Handle`s, created
/// through the `new_handle` method.
pub struct UnboundedChannel<T> {
    can_gc: AtomicBool,
    h_index: AtomicUsize,
    t_index: AtomicUsize,
    head_ptr: Atomic<Segment<T>>,
    tail_ptr: Atomic<Segment<T>>,
}

/// `advance_to` moves `seg` to point as close to the segment with id corresponding to `ind` as
/// possible without allocating new segments. If `reclaim` is passed as true, then the skipped-over
/// segments are reclaimed. Only one thread may execute `advance_to` at a time. This is enforced by
/// acquiring the `can_gc` lock in `adjust`.
fn advance_to<T>(seg: &Atomic<Segment<T>>, ind: usize, reclaim: bool, guard: &Guard) {
    let mut cur = seg.load(Relaxed, guard).unwrap();
    let orig = seg.load(Relaxed, guard).unwrap();
    let seg_id = ind >> SEG_SHIFT;
    // unused if reclaim is false... should confirm doesn't require too many resources
    let mut v = Vec::new();
    while cur.id.load(Relaxed) < seg_id {
        match cur.next.load(Relaxed, guard) {
            Some(shared) => {
                if reclaim {
                    v.push(cur);
                }
                cur = shared
            }
            None => break,
        }
    }
    assert!(seg.cas_shared(Some(orig), Some(cur), Relaxed));
    if reclaim {
        for s in v.into_iter() {
            unsafe {
                unlink_seg(s, guard);
            }
        }
    }
}

/// `unlink_seg` iterates through the data in `s` and marks it as unlinked, then marks `s` itself
/// unlinked.
unsafe fn unlink_seg<'a, T>(s: Shared<'a, Segment<T>>, guard: &'a Guard) {
    for p in s.data.iter() {
        if let Some(dptr) = p.load(Relaxed, guard) {
            guard.unlinked(dptr);
        }
    }
    guard.unlinked(s);
}

impl<T> Drop for UnboundedChannel<T> {
    fn drop(&mut self) {
        assert!(self.can_gc.load(Relaxed));
        let guard = epoch::pin();
        let mut cur_seg = (if self.h_index.load(Relaxed) < self.t_index.load(Relaxed) {
                &self.head_ptr
            } else {
                &self.tail_ptr
            })
            .load(Relaxed, &guard);
        while let Some(s) = cur_seg {
            cur_seg = s.next.load(Relaxed, &guard);
            unsafe {
                unlink_seg(s, &guard);
            }
        }
    }
}

impl<T> UnboundedChannel<T> {
    // The primary reason to distinguish between a channel and a handle on that channel is to
    // manage thread-local state. The two major cases of this are a thread-local cache for
    // allocations and a thread-local counter to schedule `adjust` operations.
    pub fn new() -> UnboundedChannel<T> {
        let seg = Owned::new(Segment::new(0));
        let res = UnboundedChannel {
            can_gc: AtomicBool::new(true),
            h_index: AtomicUsize::new(0),
            t_index: AtomicUsize::new(0),
            head_ptr: Atomic::null(),
            tail_ptr: Atomic::null(),
        };
        let guard = epoch::pin();
        res.tail_ptr
            .store_shared(Some(res.head_ptr.store_and_ref(seg, Relaxed, &guard)),
                          Relaxed);
        res
    }

    pub fn new_handle(&self) -> Handle<T> {
        Handle {
            ctr: 0,
            ch: self,
            cache: None,
        }
    }

    fn receive(&self, cache: &mut Option<Owned<Segment<T>>>) -> T {
        let guard = epoch::pin();
        let cell = self.head_ptr
            .load(Relaxed, &guard)
            .expect("head pointer should never be null")
            .find_cell(self.h_index.fetch_add(1, Relaxed), &guard, cache);
        let waiter = Owned::new(SegCell::Wait(Waiter::new()));
        match cell.cas_and_ref(None, waiter, Relaxed, &guard) {
            Ok(shared) => return unsafe { shared.waiter().wait() },
            Err(_) => {
                // see the comment in `send` about this fence.
                fence(Acquire);
                let t = cell.load(Relaxed, &guard).unwrap().val();
                unsafe {
                    return ptr::read(t as *const T);
                }
            }
        }
    }

    fn adjust(&self) {
        // TODO is Acquire/Release needed here?
        if !self.can_gc.compare_and_swap(true, false, Acquire) {
            return;
        }
        let guard = epoch::pin();
        let my_h = self.h_index.load(Relaxed);
        let my_t = self.t_index.load(Relaxed);
        advance_to(&self.head_ptr, my_h, my_h <= my_t, &guard);
        advance_to(&self.tail_ptr, my_t, my_h > my_t, &guard);
        self.can_gc.store(true, Release);
    }

    fn send(&self, t: T, cache: &mut Option<Owned<Segment<T>>>) {
        let guard = epoch::pin();
        let cell = self.tail_ptr
            .load(Relaxed, &guard)
            .expect("tail pointer should never be null")
            .find_cell(self.t_index.fetch_add(1, Relaxed), &guard, cache);
        if let Err(ptr) = cell.cas(None, Some(Owned::new(SegCell::Val(t))), Relaxed) {
            // the load of the cell must happen after the CAS, I think this fence accomplishes that
            // but it is unclear.
            // TODO: confirm this is sufficient.
            fence(Acquire);
            // our CAS failed, so there must be a waiter there,
            if let SegCell::Val(t) = ptr.unwrap().into_inner() {
                (*cell.load(Relaxed, &guard).unwrap()).waiter().send(t);
                return;
            }
            unreachable!();
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate crossbeam;
    use super::*;
    use std::sync::mpsc;
    macro_rules! sizes { () => { vec![128, super::SEG_SIZE * 2, super::SEG_SIZE * 64] } }

    #[test]
    fn single_threaded() {
        for num_elements in sizes!().into_iter() {
            let chan = UnboundedChannel::new();
            let mut handle = chan.new_handle();
            for i in 0..num_elements {
                handle.send(i + 1);
            }
            let mut v = Vec::new();
            for _i in 0..num_elements {
                v.push(handle.receive());
            }
            for i in 0..num_elements {
                assert_eq!(v[i], i + 1)
            }
        }
    }

    #[test]
    fn two_threads() {
        for num_elements in sizes!().into_iter() {
            let chan = UnboundedChannel::new();
            let n_elements = num_elements;
            crossbeam::scope(|scope| {
                scope.spawn(|| {
                    let mut handle = chan.new_handle();
                    let mut v = Vec::new();
                    for _i in 0..n_elements {
                        v.push(handle.receive());
                    }
                    for i in 0..n_elements {
                        assert_eq!(v[i], i + 1)
                    }
                });
                scope.spawn(|| {
                    let mut handle = chan.new_handle();
                    for i in 0..n_elements {
                        handle.send(i + 1);
                    }
                });
            })
        }
    }

    #[test]
    fn many_threads() {
        let num_threads = 128;
        let num_ops = num_threads << 8;
        let per_thread = num_ops / num_threads;
        let num_elements = num_ops / 2;
        let chan = UnboundedChannel::new();
        crossbeam::scope(|scope| {
            // we use a stdlib channel to collect the results
            let (snd, recv) = mpsc::channel();
            for _i in 0..num_threads / 2 {
                let mut handle = chan.new_handle();
                let snd = snd.clone();
                scope.spawn(move || {
                    for _j in 0..per_thread {
                        let _ = snd.send(handle.receive());
                    }
                });
            }
            for i in 0..num_threads / 2 {
                let mut handle = chan.new_handle();
                scope.spawn(move || {
                    for j in 0..per_thread {
                        handle.send(i * per_thread + j);
                    }
                });
            }
            let mut v = Vec::new();
            for _i in 0..num_elements {
                v.push(recv.recv().unwrap());
            }
            v.sort();
            let v2: Vec<_> = (0..num_elements).collect();
            assert_eq!(v, v2);
        })
    }
}
