/// Epoch-based garbage collector.
///
/// # Examples
///
/// ```
/// use crossbeam_epoch::Collector;
///
/// let collector = Collector::new();
///
/// let handle = collector.handle();
/// drop(collector); // `handle` still works after dropping `collector`
///
/// handle.pin(|scope| {
///     scope.flush();
/// });
/// ```

use std::cell::Cell;
use std::ptr;
use std::sync::Arc;

use garbage::Garbage;
use internal::{Global, Local};

/// An epoch-based garbage collector.
pub struct Collector(Arc<Global>);

struct LocalBox {
    local: Local,
    num_guards: Cell<usize>,
    num_handles: Cell<usize>,
}

/// A handle to a garbage collector.
pub struct Handle {
    global: Arc<Global>,
    local: *const LocalBox, // !Send + !Sync
}

/// A witness that the current participant is pinned.
///
/// A `Guard` is a witness that the current participant is pinned. Lots of methods that interact
/// with [`Atomic`]s can safely be called only while the participant is pinned so they often require
/// a `&Guard`.
///
/// This data type is inherently bound to the thread that created it, therefore it does not
/// implement `Send` nor `Sync`.
///
/// [`Atomic`]: struct.Atomic.html
#[derive(Debug)]
pub struct Guard {
    /// A reference to the global data.
    global: *const Global,
    /// A reference to the thread-local data.
    local: *const LocalBox, // !Send + !Sync
}

impl Collector {
    /// Creates a new collector.
    pub fn new() -> Self {
        Self { 0: Arc::new(Global::new()) }
    }

    /// Creates a new handle for the collector.
    #[inline]
    pub fn handle(&self) -> Handle {
        let global = self.0.clone();
        let local = Local::new();
        local.num_handles.set(1);
        let local = Box::new(Local::new());
        local.register(&global);
        Handle { global, local: Box::into_raw(local) }
    }
}

impl Handle {
    /// Pin the current handle.
    #[inline]
    pub fn pin(&self) -> Guard {
        unsafe { (*self.local).pin(&self.global) }
    }

    /// Check if the current handle is pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        unsafe { (*self.local).is_pinned() }
    }
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        unsafe {
            let local = &*self.local;
            let num_handles = local.num_handles.get();
            local.num_handles.set(num_handles + 1);
            Self { global: self.global.clone(), local }
        }
    }
}

unsafe impl Send for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe {
            let local = &*self.local;
            let num_handles = local.num_handles.get();
            local.num_handles.set(num_handles - 1);

            if num_handles == 1 && local.num_guards.get() == 0 {
                // There are no more guards and handles. Unregister the local data.
                local.unregister(&self.global);
            }
        }
    }
}

impl Guard {
    unsafe fn defer_garbage(&self, mut garbage: Garbage) {
        self.global.as_ref().map(|global| {
            let bag = &mut *(*self.local).bag.get();
            while let Err(g) = bag.try_push(garbage) {
                global.push_bag(bag, self);
                garbage = g;
            }
        });
    }

    /// Deferred execution of an arbitrary function `f`.
    ///
    /// This function inserts the function into a thread-local [`Bag`]. When the bag becomes full,
    /// the bag is flushed into the globally shared queue of bags.
    ///
    /// If this function is destroying a particularly large object, it is wise to follow up with a
    /// call to [`flush`] so that it doesn't get stuck waiting in the thread-local bag for a long
    /// time.
    ///
    /// [`Bag`]: struct.Bag.html
    /// [`flush`]: fn.flush.html
    pub unsafe fn defer<R, F: FnOnce() -> R + Send>(&self, f: F) {
        self.defer_garbage(Garbage::new(|| drop(f())))
    }

    /// Flushes all garbage in the thread-local storage into the global garbage queue, attempts to
    /// advance the epoch, and collects some garbage.
    ///
    /// Even though flushing can be explicitly called, it is also automatically triggered when the
    /// thread-local storage fills up or when we pin the current participant a specific number of
    /// times.
    ///
    /// It is wise to flush the bag just after `defer`ring the deallocation of a very large object,
    /// so that it isn't sitting in the thread-local bag for a long time.
    ///
    /// [`defer`]: fn.defer.html
    pub fn flush(&self) {
        unsafe {
            self.global.as_ref().map(|global| {
                let bag = &mut *(*self.local).bag.get();

                if !bag.is_empty() {
                    global.push_bag(bag, &self);
                }

                global.collect(&self);
            });
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.local.is_null() {
            unsafe {
                let local = &*self.local;
                let num_guards = local.num_guards.get();
                local.num_guards.set(num_guards - 1);

                if num_guards == 1 {
                    // Mark this participant as unpinned.
                    local.local_epoch.get().set_unpinned();

                    if local.num_handles.get() == 0 {
                        // There are no more guards and handles. Unregister the local data.
                        local.unregister(&*self.global);
                    }
                }
            }
        }
    }
}

/// Returns a [`Scope`] without pinning any participant.
///
/// Sometimes, we'd like to have longer-lived scopes in which we know our thread can access atomics
/// without protection. This is true e.g. when deallocating a big data structure, or when
/// constructing it from a long iterator. In such cases we don't need to be overprotective because
/// there is no fear of other threads concurrently destroying objects.
///
/// Function `unprotected` is *unsafe* because we must promise that (1) the locations that we access
/// should not be deallocated by concurrent participants, and (2) the locations that we deallocate
/// should not be accessed by concurrent participants.
///
/// Just like with the safe epoch::pin function, unprotected use of atomics is enclosed within a
/// scope so that pointers created within it don't leak out or get mixed with pointers from other
/// scopes.
#[inline]
pub unsafe fn unprotected() -> Guard {
    Guard {
        global: ptr::null(),
        local: ptr::null(),
    }
}


#[cfg(test)]
mod tests {
    use std::mem;
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
    use std::sync::atomic::Ordering;
    use crossbeam_utils::scoped;

    use {Collector, Owned};

    const NUM_THREADS: usize = 8;

    #[test]
    fn pin_reentrant() {
        let collector = Collector::new();
        let handle = collector.handle();
        drop(collector);

        assert!(!handle.is_pinned());
        {
            let _guard = &handle.pin();
            assert!(handle.is_pinned());
            {
                let _guard = &handle.pin();
                assert!(handle.is_pinned());
            }
            assert!(handle.is_pinned());
        }
        assert!(!handle.is_pinned());
    }

    #[test]
    fn flush_local_bag() {
        let collector = Collector::new();
        let handle = collector.handle();
        drop(collector);

        for _ in 0..100 {
            let guard = &handle.pin();
            unsafe {
                let a = Owned::new(7).into_ptr(guard);
                guard.defer(move || a.into_owned());

                assert!(!(*(*guard.local).bag.get()).is_empty());

                while !(*(*guard.local).bag.get()).is_empty() {
                    guard.flush();
                }
            }
        }
    }

    #[test]
    fn garbage_buffering() {
        let collector = Collector::new();
        let handle = collector.handle();
        drop(collector);

        let guard = &handle.pin();
        unsafe {
            for _ in 0..10 {
                let a = Owned::new(7).into_ptr(guard);
                guard.defer(move || a.into_owned());
            }
            assert!(!(*(*guard.local).bag.get()).is_empty());
        }
    }

    #[test]
    fn pin_holds_advance() {
        let collector = Collector::new();

        let threads = (0..NUM_THREADS)
            .map(|_| {
                scoped::scope(|scope| {
                    scope.spawn(|| {
                        let handle = collector.handle();
                        for _ in 0..500_000 {
                            let guard = &handle.pin();

                            let before = collector.0.get_epoch();
                            collector.0.collect(guard);
                            let after = collector.0.get_epoch();

                            assert!(after.wrapping_sub(before) <= 2);
                        }
                    })
                })
            })
            .collect::<Vec<_>>();
        drop(collector);

        for t in threads {
            t.join();
        }
    }

    #[test]
    fn incremental() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = ATOMIC_USIZE_INIT;

        let collector = Collector::new();
        let handle = collector.handle();

        unsafe {
            let guard = &handle.pin();
            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_ptr(guard);
                guard.defer(move || {
                    drop(a.into_owned());
                    DESTROYS.fetch_add(1, Ordering::Relaxed);
                });
            }
            guard.flush();
        }

        let mut last = 0;

        while last < COUNT {
            let curr = DESTROYS.load(Ordering::Relaxed);
            assert!(curr - last <= 1024);
            last = curr;

            let guard = &handle.pin();
            collector.0.collect(guard);
        }
        assert!(DESTROYS.load(Ordering::Relaxed) == 100_000);
    }

    #[test]
    fn buffering() {
        const COUNT: usize = 10;
        static DESTROYS: AtomicUsize = ATOMIC_USIZE_INIT;

        let collector = Collector::new();
        let handle = collector.handle();

        unsafe {
            let guard = &handle.pin();
            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_ptr(guard);
                guard.defer(move || {
                    drop(a.into_owned());
                    DESTROYS.fetch_add(1, Ordering::Relaxed);
                });
            }
        }

        for _ in 0..100_000 {
            collector.0.collect(&handle.pin());
        }
        assert!(DESTROYS.load(Ordering::Relaxed) < COUNT);

        handle.pin().flush();

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.0.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn count_drops() {
        const COUNT: usize = 100_000;
        static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();
        let handle = collector.handle();

        unsafe {
            let guard = &handle.pin();

            for _ in 0..COUNT {
                let a = Owned::new(Elem(7i32)).into_ptr(guard);
                guard.defer(move || a.into_owned());
            }
            guard.flush();
        }

        while DROPS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.0.collect(guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn count_destroy() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = ATOMIC_USIZE_INIT;

        let collector = Collector::new();
        let handle = collector.handle();

        unsafe {
            let guard = &handle.pin();

            for _ in 0..COUNT {
                let a = Owned::new(7i32).into_ptr(guard);
                guard.defer(move || {
                    drop(a.into_owned());
                    DESTROYS.fetch_add(1, Ordering::Relaxed);
                });
            }
            guard.flush();
        }

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.0.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn drop_array() {
        const COUNT: usize = 700;
        static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();
        let handle = collector.handle();

        unsafe {
            let guard = &handle.pin();

            let mut v = Vec::with_capacity(COUNT);
            for i in 0..COUNT {
                v.push(Elem(i as i32));
            }

            let a = Owned::new(v).into_ptr(guard);
            guard.defer(move || a.into_owned());
            guard.flush();
        }

        while DROPS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.0.collect(guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn destroy_array() {
        const COUNT: usize = 100_000;
        static DESTROYS: AtomicUsize = ATOMIC_USIZE_INIT;

        let collector = Collector::new();
        let handle = collector.handle();

        unsafe {
            let guard = &handle.pin();

            let mut v = Vec::with_capacity(COUNT);
            for i in 0..COUNT {
                v.push(i as i32);
            }

            let ptr = v.as_mut_ptr() as usize;
            let len = v.len();
            guard.defer(move || {
                drop(Vec::from_raw_parts(ptr as *const u8 as *mut u8, len, len));
                DESTROYS.fetch_add(len, Ordering::Relaxed);
            });
            guard.flush();

            mem::forget(v);
        }

        while DESTROYS.load(Ordering::Relaxed) < COUNT {
            let guard = &handle.pin();
            collector.0.collect(guard);
        }
        assert_eq!(DESTROYS.load(Ordering::Relaxed), COUNT);
    }

    #[test]
    fn stress() {
        const THREADS: usize = 8;
        const COUNT: usize = 100_000;
        static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;

        struct Elem(i32);

        impl Drop for Elem {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        let collector = Collector::new();

        let threads = (0..THREADS)
            .map(|_| {
                scoped::scope(|scope| {
                    scope.spawn(|| {
                        let handle = collector.handle();
                        for _ in 0..COUNT {
                            let guard = &handle.pin();
                            unsafe {
                                let a = Owned::new(Elem(7i32)).into_ptr(guard);
                                guard.defer(move || a.into_owned());
                            }
                        }
                    })
                })
            })
            .collect::<Vec<_>>();

        for t in threads {
            t.join();
        }

        let handle = collector.handle();
        while DROPS.load(Ordering::Relaxed) < COUNT * THREADS {
            let guard = &handle.pin();
            collector.0.collect(guard);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), COUNT * THREADS);
    }

    #[test]
    fn send_handle() {
        let ref collector = Collector::new();
        let handle_global = collector.handle();

        let threads = (0..NUM_THREADS)
            .map(|_| {
                scoped::scope(|scope| {
                    let handle = handle_global.clone();
                    scope.spawn(move || handle.pin().flush())
                })
            })
            .collect::<Vec<_>>();
        drop(collector);

        for t in threads {
            t.join();
        }
    }
}


    // /// Total number of pinnings performed.
    // pin_count: Cell<usize>,
    // /// How many handles out there?
    // pub num_handles: Cell<usize>,
    // /// How many times it is pinned?
    // pub num_guards: Cell<usize>,


    //         pin_count: Cell::new(0),
    //         num_guards: Cell::new(0),
    //         num_handles: Cell::new(0),


    // /// Number of pinnings after which a participant will collect some global garbage.
    // const PINS_BETWEEN_COLLECT: usize = 128;



    // pub unsafe fn pin(&self, global: &Global) -> Guard {
    //     let guard = Guard {
    //         global,
    //         local: self,
    //     };

    //     let num_guards = self.num_guards.get(); 
    //     self.num_guards.set(num_guards + 1);

    //     if num_guards == 0 {
    //         // Increment the pin counter.
    //         let count = self.pin_count.get();
    //         self.pin_count.set(count.wrapping_add(1));

    //         // Mark this participant as pinned.
    //         let epoch = global.get_epoch();
    //         self.local_epoch.get().set_pinned(epoch);

    //         // If the counter progressed enough, try advancing the epoch and collecting garbage.
    //         if count % Self::PINS_BETWEEN_COLLECT << 1 == 0 {
    //             global.collect(&guard);
    //         }
    //     }

    //     guard
    // }



            
    // /// Returns `true` if the current participant is pinned.
    // #[inline]
    // pub fn is_pinned(&self) -> bool {
    //     self.num_guards.get() > 0
    // }
