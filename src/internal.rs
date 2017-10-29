//! The global data and participant for garbage collection.
//!
//! # Registration
//!
//! In order to track all participants in one place, we need some form of participant
//! registration. When a participant is created, it is registered to a global lock-free
//! singly-linked list of registries; and when a participant is leaving, it is unregistered from the
//! list.
//!
//! # Pinning
//!
//! Every participant contains an integer that tells whether the participant is pinned and if so,
//! what was the global epoch at the time it was pinned. Participants also hold a pin counter that
//! aids in periodic global epoch advancement.
//!
//! When a participant is pinned, a `Scope` is returned as a witness that the participant is pinned.
//! Scopes are necessary for performing atomic operations, and for freeing/dropping locations.
//!
//! # Example
//!
//! `Global` is the global data for a garbage collector, and `Local` is a participant of a garbage
//! collector. Use `Global` and `Local` when you want to embed a garbage collector in another
//! systems library, e.g. memory allocator or thread manager.
//!
//! ```ignore
//! let global = Global::new();
//! let local = Local::new(&global);
//! unsafe {
//!     local.pin(&global, |scope| {
//!         scope.flush();
//!     });
//! }
//! ```

use std::cell::{Cell, UnsafeCell};
use std::cmp;
use std::ptr;
use std::sync::atomic::Ordering;

use garbage::{Garbage, Bag};
use epoch::{Epoch, LocalEpoch};
use sync::list::{List, Node};
use sync::queue::Queue;


/// The global data for a garbage collector.
#[derive(Debug)]
pub struct Global {
    /// The head pointer of the list of participant registries.
    registries: List<LocalEpoch>,
    /// A reference to the global queue of garbages.
    garbages: Queue<(usize, Bag)>,
    /// A reference to the global epoch.
    epoch: Epoch,
}

// FIXME(stjepang): Registries are stored in a linked list because linked lists are fairly easy to
// implement in a lock-free manner. However, traversal is rather slow due to cache misses and data
// dependencies. We should experiment with other data structures as well.
/// Participant for garbage collection
#[derive(Debug)]
pub struct Local {
    /// This participant's local epoch.  The least significant bit is set if the participant is
    /// currently pinned. The rest of the bits encode the current epoch.
    local_epoch: Node<LocalEpoch>,
    /// The local garbage objects that will be later freed.
    pub bag: UnsafeCell<Bag>,
    /// Total number of pinnings performed.
    pin_count: Cell<usize>,
    /// How many times it is pinned?
    pub num_guards: Cell<usize>,
    /// How many handles out there?
    pub num_handles: Cell<usize>,
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
    pub(crate) local: *const Local, // !Send + !Sync
}


impl Global {
    /// Number of bags to destroy.
    const COLLECT_STEPS: usize = 8;

    /// Creates a new global data for garbage collection.
    #[inline]
    pub fn new() -> Self {
        Self {
            registries: List::new(),
            garbages: Queue::new(),
            epoch: Epoch::new(),
        }
    }

    /// Returns the global epoch.
    #[inline]
    pub fn get_epoch(&self) -> usize {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Pushes the bag onto the global queue and replaces the bag with a new empty bag.
    pub fn push_bag(&self, bag: &mut Bag, guard: &Guard) {
        let epoch = self.epoch.load(Ordering::Relaxed);
        let bag = ::std::mem::replace(bag, Bag::new());
        ::std::sync::atomic::fence(Ordering::SeqCst);
        self.garbages.push((epoch, bag), guard);
    }

    /// Collect several bags from the global garbage queue and destroy their objects.
    ///
    /// Note: This may itself produce garbage and in turn allocate new bags.
    ///
    /// `pin()` rarely calls `collect()`, so we want the compiler to place that call on a cold
    /// path. In other words, we want the compiler to optimize branching for the case when
    /// `collect()` is not called.
    #[cold]
    pub fn collect(&self, scope: &Guard) {
        let epoch = self.epoch.try_advance(&self.registries, scope);

        let condition = |bag: &(usize, Bag)| {
            // A pinned participant can witness at most one epoch advancement. Therefore, any bag
            // that is within one epoch of the current one cannot be destroyed yet.
            let diff = epoch.wrapping_sub(bag.0);
            cmp::min(diff, 0usize.wrapping_sub(diff)) > 2
        };

        for _ in 0..Self::COLLECT_STEPS {
            match self.garbages.try_pop_if(&condition, scope) {
                None => break,
                Some(bag) => drop(bag),
            }
        }
    }
}

unsafe impl Sync for Local {}

impl Local {
    /// Number of pinnings after which a participant will collect some global garbage.
    const PINS_BETWEEN_COLLECT: usize = 128;

    /// Creates a participant to the garbage collection global data.
    #[inline]
    pub fn new() -> Self {
        Self {
            local_epoch: Node::new(LocalEpoch::new()),
            bag: UnsafeCell::new(Bag::new()),
            pin_count: Cell::new(0),
            num_guards: Cell::new(0),
            num_handles: Cell::new(0),
        }
    }

    /// Pins the current participant, executes a function, and unpins the participant.
    ///
    /// The provided function takes a `Guard`, which can be used to interact with [`Atomic`]s. The
    /// scope serves as a proof that whatever data you load from an [`Atomic`] will not be
    /// concurrently deleted by another participant while the scope is alive.
    ///
    /// Note that keeping a participant pinned for a long time prevents memory reclamation of any
    /// newly deleted objects protected by [`Atomic`]s. The provided function should be very quick -
    /// generally speaking, it shouldn't take more than 100 ms.
    ///
    /// Pinning is reentrant. There is no harm in pinning a participant while it's already pinned
    /// (repinning is essentially a noop).
    ///
    /// Pinning itself comes with a price: it begins with a `SeqCst` fence and performs a few other
    /// atomic operations. However, this mechanism is designed to be as performant as possible, so
    /// it can be used pretty liberally. On a modern machine pinning takes 10 to 15 nanoseconds.
    ///
    /// # Safety
    ///
    /// You should pass `global` that is used to create this `Local`. Otherwise, the behavior is
    /// undefined.
    ///
    /// [`Atomic`]: struct.Atomic.html
    pub unsafe fn pin(&self, global: &Global) -> Guard {
        let guard = Guard {
            global,
            local: self,
        };

        let num_guards = self.num_guards.get(); 
        self.num_guards.set(num_guards + 1);

        if num_guards == 0 {
            // Increment the pin counter.
            let count = self.pin_count.get();
            self.pin_count.set(count.wrapping_add(1));

            // Mark this participant as pinned.
            let epoch = global.get_epoch();
            self.local_epoch.get().set_pinned(epoch);

            // If the counter progressed enough, try advancing the epoch and collecting garbage.
            if count % Self::PINS_BETWEEN_COLLECT == 0 {
                global.collect(&guard);
            }
        }

        guard
    }

    /// Returns `true` if the current participant is pinned.
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.num_guards.get() > 0
    }

    /// FIXME
    pub fn register(&self, global: &Global) {
        unsafe {
            // Since we dereference no pointers in this block, it is safe to use `unprotected`.
            let guard = unprotected();
            global.registries.insert(&self.local_epoch, &guard);
        }
    }

    /// Unregisters itself from the garbage collector.
    ///
    /// # Safety
    ///
    /// You should pass `global` that is used to create this `Local`. Also, a `Local` should be
    /// unregistered once, and after it is unregistered it should not be `pin()`ned. Otherwise, the
    /// behavior is undefined.
    pub unsafe fn unregister(&self, global: &Global) {
        // Now that the participant is exiting, we must move the local bag into the global garbage
        // queue. Also, let's try advancing the epoch and help free some garbage.

        let guard = self.pin(global);

        // Spare some cycles on garbage collection.
        global.collect(&guard);

        // Unregister the participant by marking this entry as deleted.
        self.local_epoch.delete(&guard);

        // Push the local bag into the global garbage queue.
        global.push_bag(&mut *self.bag.get(), &guard);
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
