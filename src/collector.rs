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
/// handle.pin().flush();
/// ```

use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use internal::{Global, Local};
use guard::Guard;
use internal::Node;
use unprotected;

/// An epoch-based garbage collector.
pub struct Collector(Arc<Global>);

/// A handle to a garbage collector.
pub struct Handle {
    global: Arc<Global>,
    node: *const Node,
}

impl Collector {
    /// Creates a new collector.
    pub fn new() -> Self {
        Self { 0: Arc::new(Global::new()) }
    }

    /// Creates a new handle for the collector.
    #[inline]
    pub fn handle(&self) -> Handle {
        Handle::new(self.0.clone())
    }
}

impl Handle {
    fn new(global: Arc<Global>) -> Self {
        let node = Local::new(&global);
        Self { global, node }
    }

    /// Pin the current handle.
    #[inline]
    pub fn pin(&self) -> Guard {
        unsafe { (*self.node).local.pin(&self.global) }
    }

    /// Check if the current handle is pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        unsafe {
            (*self.node).local.is_pinned()
        }
    }
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        Self::new(self.global.clone())
    }
}

unsafe impl Send for Handle {}

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe {
            let guard = &unprotected();
            (*self.node).next.fetch_or(1, SeqCst, guard);
        }
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
                    scope.spawn(move || handle.pin(|scope| scope.flush()))
                })
            })
            .collect::<Vec<_>>();
        drop(collector);

        for t in threads {
            t.join();
        }
    }
}
