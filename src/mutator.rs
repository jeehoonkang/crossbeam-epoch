//! Mutator: reference to a garbage collection collector
//!
//! # Pinning
//!
//! Every mutator contains an integer that tells whether the mutator is pinned and if so, what was the
//! global epoch at the time it was pinned. Mutators also hold a pin counter that aids in periodic
//! global epoch advancement.
//!
//! When a mutator is pinned, a `Scope` is returned as a witness that the mutator is pinned.  Scopes
//! are necessary for performing atomic operations, and for freeing/dropping locations.

use std::cell::{Cell, UnsafeCell};
use std::ptr;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, Release, SeqCst};

use atomic::Ptr;
use sync::list::Node;
use garbage::{Garbage, Bag};
use collector::Collector;


// FIXME(stjepang): Registries are stored in a linked list because linked lists are fairly easy to
// implement in a lock-free manner. However, traversal is rather slow due to cache misses and data
// dependencies. We should experiment with other data structures as well.
/// Local data for garbage collection.
pub struct Inner<'scope> {
    /// The local garbage objects that will be later freed.
    bag: UnsafeCell<Bag>,
    /// This mutator's entry in the local epoch list.
    local_epoch: &'scope Node<LocalEpoch>,
    /// Whether the mutator is currently pinned.
    is_pinned: Cell<bool>,
    /// Total number of pinnings performed.
    pin_count: Cell<usize>,
}

/// An entry in the linked list of the registered mutators.
#[derive(Default, Debug)]
pub struct LocalEpoch {
    /// The least significant bit is set if the mutator is currently pinned. The rest of the bits
    /// encode the current epoch.
    state: AtomicUsize,
}

/// A witness that the current mutator is pinned.
///
/// A reference to `Scope` is a witness that the current mutator is pinned. Lots of methods that
/// interact with [`Atomic`]s can safely be called only while the mutator is pinned so they often
/// require a reference to `Scope`.
///
/// This data type is inherently bound to the thread that created it, therefore it does not
/// implement `Send` nor `Sync`.
///
/// [`Atomic`]: struct.Atomic.html
#[derive(Debug)]
pub struct Scope<'scope> {
    /// A reference to the global data.
    collector: &'scope Collector,
    /// The local garbage bag.
    bag: *mut Bag, // !Send + !Sync
}

impl<'scope> Inner<'scope> {
    pub fn new(collector: &'scope Collector) -> Self {
        Self {
            bag: UnsafeCell::new(Bag::new()),
            local_epoch: collector.register_epoch(),
            is_pinned: Cell::new(false),
            pin_count: Cell::new(0),
        }
    }
}

pub trait Mutator<'scope> {
    /// Number of pinnings after which a mutator will collect some global garbage.
    const PINS_BETWEEN_COLLECT: usize = 128;

    #[inline]
    fn as_collector(&'scope self) -> &'scope Collector;

    #[inline]
    fn as_inner(&'scope self) -> &'scope Inner<'scope>;

    /// Pins the current mutator, executes a function, and unpins the mutator.
    ///
    /// The provided function takes a reference to a `Scope`, which can be used to interact with
    /// [`Atomic`]s. The scope serves as a proof that whatever data you load from an [`Atomic`] will
    /// not be concurrently deleted by another mutator while the scope is alive.
    ///
    /// Note that keeping a mutator pinned for a long time prevents memory reclamation of any newly
    /// deleted objects protected by [`Atomic`]s. The provided function should be very quick -
    /// generally speaking, it shouldn't take more than 100 ms.
    ///
    /// Pinning is reentrant. There is no harm in pinning a mutator while it's already pinned
    /// (repinning is essentially a noop).
    ///
    /// Pinning itself comes with a price: it begins with a `SeqCst` fence and performs a few other
    /// atomic operations. However, this mechanism is designed to be as performant as possible, so
    /// it can be used pretty liberally. On a modern machine pinning takes 10 to 15 nanoseconds.
    ///
    /// [`Atomic`]: struct.Atomic.html
    fn pin<F, R>(&'scope self, f: F) -> R
    where
        F: FnOnce(&Scope) -> R,
    {
        let local_epoch = self.as_inner().local_epoch.get();
        let scope = &Scope {
            collector: self.as_collector(),
            bag: self.as_inner().bag.get(),
        };

        let was_pinned = self.as_inner().is_pinned.get();
        if !was_pinned {
            // Increment the pin counter.
            let count = self.as_inner().pin_count.get();
            self.as_inner().pin_count.set(count.wrapping_add(1));

            // Pin the mutator.
            self.as_inner().is_pinned.set(true);
            let epoch = self.as_collector().get_epoch();
            local_epoch.set_pinned(epoch);

            // If the counter progressed enough, try advancing the epoch and collecting garbage.
            if count % Self::PINS_BETWEEN_COLLECT == 0 {
                self.as_collector().collect(scope);
            }
        }

        // This will unpin the mutator even if `f` panics.
        defer! {
            if !was_pinned {
                // Unpin the mutator.
                local_epoch.set_unpinned();
                self.as_inner().is_pinned.set(false);
            }
        }

        f(scope)
    }

    /// Returns `true` if the current mutator is pinned.
    #[inline]
    fn is_pinned(&'scope self) -> bool {
        self.as_inner().is_pinned.get()
    }

    /// Finalize the mutator.
    #[inline]
    fn finalize(&'scope self) {
        // Now that the mutator is exiting, we must move the local bag into the global garbage
        // queue. Also, let's try advancing the epoch and help free some garbage.

        self.pin(|scope| {
            // Spare some cycles on garbage collection.
            self.as_collector().collect(scope);

            // Unregister the mutator by marking this entry as deleted.
            self.as_inner().local_epoch.delete(scope);

            // Push the local bag into the global garbage queue.
            unsafe {
                self.as_collector().push_bag(&mut *self.as_inner().bag.get(), scope);
            }
        });
    }
}


/// Reference to a garbage collection collector
pub struct Impl<'scope> {
    /// A reference to the global data.
    collector: &'scope Collector,
    /// FIXME
    inner: Inner<'scope>,
}

impl<'scope> Impl<'scope> {
    pub fn new(collector: &'scope Collector) -> Self {
        Self { collector, inner: Inner::new(collector) }
    }
}

impl<'scope> Mutator<'scope> for Impl<'scope> {
    #[inline]
    fn as_collector(&'scope self) -> &'scope Collector {
        self.collector
    }

    #[inline]
    fn as_inner(&'scope self) -> &'scope Inner<'scope> {
        &self.inner
    }
}

impl<'scope> Drop for Impl<'scope> {
    fn drop(&mut self) {
        self.finalize()
    }
}

/// Returns a [`Scope`] without pinning any mutator.
///
/// Sometimes, we'd like to have longer-lived scopes in which we know our thread can access atomics
/// without protection. This is true e.g. when deallocating a big data structure, or when
/// constructing it from a long iterator. In such cases we don't need to be overprotective because
/// there is no fear of other threads concurrently destroying objects.
///
/// Function `unprotected` is *unsafe* because we must promise that (1) the locations that we access
/// should not be deallocated by concurrent mutators, and (2) the locations that we deallocate
/// should not be accessed by concurrent mutators.
///
/// Just like with the safe epoch::pin function, unprotected use of atomics is enclosed within a
/// scope so that pointers created within it don't leak out or get mixed with pointers from other
/// scopes.
#[inline]
pub unsafe fn unprotected<F, R>(f: F) -> R
where
    F: FnOnce(&Scope) -> R,
{
    let scope = &Scope {
        collector: mem::uninitialized(),
        bag: ptr::null_mut(),
    };
    f(scope)
}

impl LocalEpoch {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns if the mutator is pinned, and if so, the epoch at which it is pinned.
    #[inline]
    pub fn get_state(&self) -> (bool, usize) {
        let state = self.state.load(Relaxed);
        ((state & 1) == 1, state & !1)
    }

    /// Marks the mutator as pinned.
    ///
    /// Must not be called if the mutator is already pinned!
    #[inline]
    pub fn set_pinned(&self, epoch: usize) {
        let state = epoch | 1;

        // Now we must store `state` into `self.state`. It's important that any succeeding loads
        // don't get reordered with this store. In order words, this mutator's epoch must be fully
        // announced to other mutators. Only then it becomes safe to load from the shared memory.
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            // On x86 architectures we have a choice:
            // 1. `atomic::fence(SeqCst)`, which compiles to a `mfence` instruction.
            // 2. `compare_and_swap(_, _, SeqCst)`, which compiles to a `lock cmpxchg` instruction.
            //
            // Both instructions have the effect of a full barrier, but the second one seems to be
            // faster in this particular case.
            let result = self.state.compare_and_swap(0, state, SeqCst);
            debug_assert_eq!(0, result, "LocalEpoch::set_pinned()'s CAS should succeed.");
        } else {
            self.state.store(state, Relaxed);
            ::std::sync::atomic::fence(SeqCst);
        }
    }

    /// Marks the mutator as unpinned.
    #[inline]
    pub fn set_unpinned(&self) {
        // Clear the last bit.
        // We don't need to preserve the epoch, so just store the number zero.
        self.state.store(0, Release);
    }
}

impl<'scope> Scope<'scope> {
    unsafe fn defer_garbage(&self, mut garbage: Garbage) {
        self.bag.as_mut().map(|bag| {
            while let Err(g) = bag.try_push(garbage) {
                self.collector.push_bag(bag, self);
                garbage = g;
            }
        });
    }

    /// Deferred deallocation of heap-allocated object `ptr`.
    ///
    /// This function inserts the object into a mutator-local [`Bag`]. When the bag becomes full,
    /// the bag is flushed into the globally shared queue of bags.
    ///
    /// If the object is unusually large, it is wise to follow up with a call to [`flush`] so that
    /// it doesn't get stuck waiting in the local bag for a long time.
    ///
    /// [`Bag`]: struct.Bag.html
    /// [`flush`]: fn.flush.html
    pub unsafe fn defer_free<T>(&self, ptr: Ptr<T>) {
        self.defer_garbage(Garbage::new_free(ptr.as_raw() as *mut T, 1))
    }

    /// Deferred destruction and deallocation of heap-allocated object `ptr`.
    // FIXME(jeehoonkang): `T: 'static` may be too restrictive.
    pub unsafe fn defer_drop<T: Send + 'static>(&self, ptr: Ptr<T>) {
        self.defer_garbage(Garbage::new_drop(ptr.as_raw() as *mut T, 1))
    }

    /// Deferred execution of an arbitrary function `f`.
    pub unsafe fn defer<F: FnOnce() + Send + 'static>(&self, f: F) {
        self.defer_garbage(Garbage::new(f))
    }

    /// Flushes all garbage in the thread-local storage into the global garbage queue, attempts to
    /// advance the epoch, and collects some garbage.
    ///
    /// Even though flushing can be explicitly called, it is also automatically triggered when the
    /// thread-local storage fills up or when we pin the current thread a specific number of times.
    ///
    /// It is wise to flush the bag just after passing a very large object to [`defer_free`] or
    /// [`defer_drop`], so that it isn't sitting in the local bag for a long time.
    ///
    /// [`defer_free`]: fn.defer_free.html [`defer_drop`]: fn.defer_drop.html
    pub fn flush(&self) {
        unsafe {
            self.bag.as_mut().map(|bag| {
                if !bag.is_empty() {
                    self.collector.push_bag(bag, self);
                }

                self.collector.collect(self);
            });
        }
    }
}


#[cfg(test)]
mod tests {
    use crossbeam_utils::scoped;

    use super::*;

    const NUM_THREADS: usize = 8;

    #[test]
    fn pin_reentrant() {
        let collector = Collector::new();
        let mutator = Impl::new(&collector);

        assert!(!mutator.is_pinned());
        mutator.pin(|_| {
            mutator.pin(|_| {
                assert!(mutator.is_pinned());
            });
            assert!(mutator.is_pinned());
        });
        assert!(!mutator.is_pinned());
    }

    #[test]
    fn pin_holds_advance() {
        let collector = &Collector::new();

        let threads = (0..NUM_THREADS)
            .map(|_| {
                scoped::scope(|scope| {
                    scope.spawn(|| for _ in 0..100_000 {
                        let mutator = Impl::new(collector);
                        mutator.pin(|scope| {
                            let before = collector.get_epoch();
                            collector.collect(scope);
                            let after = collector.get_epoch();

                            assert!(after.wrapping_sub(before) <= 2);
                        });
                    })
                })
            })
            .collect::<Vec<_>>();

        for t in threads {
            t.join();
        }
    }

    mod logger {
        use std::sync::{Arc, Mutex};
        use std::cell::UnsafeCell;

        use super::*;
        use Collector;

        const MAX_LOCAL_LINES: usize = 8;
        const MAX_GLOBAL_LINES: usize = 32;

        struct Global {
            lines: Arc<Mutex<Vec<String>>>,
            collector: Collector,
        }

        impl Default for Global {
            fn default() -> Global {
                Global {
                    lines: Arc::new(Mutex::new(Vec::with_capacity(MAX_GLOBAL_LINES))),
                    collector: Collector::new(),
                }
            }
        }

        impl Global {
            fn push(&self, line: String) {
                let mut lines = self.lines.lock().unwrap();
                if lines.len() == MAX_GLOBAL_LINES {
                    Self::flush(&mut lines);
                }
                lines.push(line);
            }

            fn flush(lines: &mut Vec<String>) {
                while let Some(_line) = lines.pop() {
                    // println!("{}", line);
                }
            }

            fn extend(to: &mut Vec<String>, from: &mut Vec<String>) {
                while let Some(line) = from.pop() {
                    to.push(line);
                }
            }
        }

        /// A logger that buffers logged lines.
        pub struct Logger<'scope> {
            lines: Vec<String>,
            mutator: Inner<'scope>,
            global: &'scope Global,
        }

        impl<'scope> Mutator<'scope> for Logger<'scope> {
            #[inline]
            fn as_collector(&self) -> &Collector {
                &self.global.collector
            }

            #[inline]
            fn as_inner(&self) -> &Inner<'scope> {
                &self.mutator
            }
        }

        impl<'scope> Logger<'scope> {
            fn new(global: &'scope Global) -> Logger {
                Logger {
                    lines: Vec::with_capacity(MAX_LOCAL_LINES),
                    mutator: Inner::new(&global.collector),
                    global,
                }
            }

            /// Log a line.
            ///
            /// The logged line may be buffered. To force buffered lines to be printed, use `flush`.
            pub fn log(&mut self, line: String) {
                if self.lines.len() == MAX_LOCAL_LINES {
                    self.flush();
                }
                self.global.push(line);
            }

            /// Flush the buffer.
            ///
            /// Flushes any buffered log lines, printing them to stdout. These lines may be interpositioned
            /// arbitrarily with lines printed from other handles.
            pub fn flush(&mut self) {
                let mut lines = self.global.lines.lock().unwrap();
                Global::extend(&mut lines, &mut self.lines);
                Global::flush(&mut lines);
            }
        }

        impl<'scope> Clone for Logger<'scope> {
            fn clone(&self) -> Self {
                Self {
                    lines: Vec::with_capacity(MAX_LOCAL_LINES),
                    mutator: Inner::new(&self.global.collector),
                    global: self.global,
                }
            }
        }

        impl<'scope> Drop for Logger<'scope> {
            fn drop(&mut self) {
                self.finalize();
                let mut lines = self.global.lines.lock().unwrap();
                Global::extend(&mut lines, &mut self.lines);
            }
        }

        lazy_static!{ static ref GLOBAL_HANDLE: Global = Global::default(); }
        thread_local!{ static TLS_HANDLE: UnsafeCell<Logger<'static>> = UnsafeCell::new(Logger::new(&GLOBAL_HANDLE)); }

        /// Log a line.
        ///
        /// The logged line may be buffered. To force buffered lines to be printed, use `flush`.
        pub fn log(line: String) {
            let l = line.clone();
            TLS_HANDLE.with(|handle| unsafe { (&mut *handle.get()).log(l) })
        }

        /// Flush the buffer.
        ///
        /// Flushes any buffered log lines, printing them to stdout. These lines may be interpositioned
        /// arbitrarily with lines printed from other threads.
        pub fn flush() {
           TLS_HANDLE.with(|handle| unsafe { (&mut *handle.get()).flush() })
        }

    }

    #[test]
    fn composable() {
        let threads = (0..NUM_THREADS)
            .map(|i| {
                scoped::scope(|scope| {
                    scope.spawn(|| for j in 0..3 {
                        logger::log(format!("({}, {})", i, j));
                        if j == 2 {
                            logger::flush();
                        }
                    })
                })
            })
            .collect::<Vec<_>>();

        for t in threads {
            t.join();
        }

        logger::flush();
    }
}
