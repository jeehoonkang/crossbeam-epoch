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
//! When a participant is pinned, a `Guard` is returned as a witness that the participant is pinned.
//! Guards are necessary for performing atomic operations, and for freeing/dropping locations.
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
//!     local.pin(&global, |guard| {
//!         guard.flush();
//!     });
//! }
//! ```

use std::cell::{Cell, UnsafeCell};
use std::cmp;
use std::sync::atomic::{AtomicUsize, Ordering};
use atomic::{Atomic, Owned};
use guard::{Guard, unprotected};
use garbage::Bag;
use epoch::Epoch;
use sync::queue::Queue;

#[derive(Debug)]
pub struct Node {
    pub local: Local,
    pub next: Atomic<Node>,
}

/// The global data for a garbage collector.
#[derive(Debug)]
pub struct Global {
    /// The head pointer of the list of participant registries.
    pub registries: Atomic<Node>,
    /// A reference to the global queue of garbages.
    garbages: Queue<(usize, Bag)>,
    /// A reference to the global epoch.
    epoch: Epoch,
}

impl Global {
    /// Number of bags to destroy.
    const COLLECT_STEPS: usize = 8;

    /// Creates a new global data for garbage collection.
    #[inline]
    pub fn new() -> Self {
        Self {
            registries: Atomic::null(),
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
    pub fn collect(&self, guard: &Guard) {
        let epoch = self.epoch.try_advance(&self, guard);

        let condition = |bag: &(usize, Bag)| {
            // A pinned participant can witness at most one epoch advancement. Therefore, any bag
            // that is within one epoch of the current one cannot be destroyed yet.
            let diff = epoch.wrapping_sub(bag.0);
            cmp::min(diff, 0usize.wrapping_sub(diff)) > 2
        };

        for _ in 0..Self::COLLECT_STEPS {
            match self.garbages.try_pop_if(&condition, guard) {
                None => break,
                Some(bag) => drop(bag),
            }
        }
    }
}

// FIXME(stjepang): Registries are stored in a linked list because linked lists are fairly easy to
// implement in a lock-free manner. However, traversal is rather slow due to cache misses and data
// dependencies. We should experiment with other data structures as well.
/// Participant for garbage collection
#[derive(Debug)]
pub struct Local {
    /// The local garbage objects that will be later freed.
    pub bag: UnsafeCell<Bag>,
    /// This participant's entry in the local epoch list.  It points to a node in `Global`, so it is
    /// alive as far as the `Global` is alive.
    pub(crate) local_epoch: LocalEpoch,
    /// Total number of pinnings performed.
    pin_count: Cell<usize>,
    pub pin_depth: Cell<usize>,
}

unsafe impl Sync for Local {}

/// An entry in the linked list of the registered participants.
#[derive(Default, Debug)]
pub struct LocalEpoch {
    /// The least significant bit is set if the participant is currently pinned. The rest of the
    /// bits encode the current epoch.
    state: AtomicUsize,
}

impl Local {
    /// Number of pinnings after which a participant will collect some global garbage.
    const PINS_BETWEEN_COLLECT: usize = 128;

    /// Creates a participant to the garbage collection global data.
    #[inline]
    pub fn new(global: &Global) -> *const Node {
        unsafe {
            // Since we dereference no pointers in this block, it is safe to use `unprotected`.
            let guard = &unprotected();

            let mut new = Owned::new(Node {
                local: Local {
                    bag: UnsafeCell::new(Bag::new()),
                    local_epoch: LocalEpoch::new(),
                    pin_count: Cell::new(0),
                    pin_depth: Cell::new(0),
                },
                next: Atomic::null(),
            });

            let mut head = global.registries.load(Ordering::Acquire, guard);
            loop {
                new.next.store(head, Ordering::Relaxed);

                // Try installing this thread's entry as the new head.
                match global.registries.compare_and_set_weak_owned(head, new, Ordering::AcqRel, guard) {
                    Ok(n) => return n.as_raw(),
                    Err((h, n)) => {
                        head = h;
                        new = n;
                    }
                }
            }
        }
    }

    pub unsafe fn pin(&self, global: &Global) -> Guard {
        let guard = Guard {
            global,
            local: self,
        };

        let depth = self.pin_depth.get();
        self.pin_depth.set(depth + 1);

        if depth == 0 {
            // Increment the pin counter.
            let count = self.pin_count.get();
            self.pin_count.set(count.wrapping_add(1));

            // Pin the participant.
            let epoch = global.get_epoch();
            self.local_epoch.set_pinned(epoch);

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
        self.pin_depth.get() > 0
    }
}

impl LocalEpoch {
    /// Creates a new local epoch.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns if the participant is pinned, and if so, the epoch at which it is pinned.
    #[inline]
    pub fn get_state(&self) -> (bool, usize) {
        let state = self.state.load(Ordering::Relaxed);
        ((state & 1) == 1, state & !1)
    }

    /// Marks the participant as pinned.
    ///
    /// Must not be called if the participant is already pinned!
    #[inline]
    pub fn set_pinned(&self, epoch: usize) {
        let state = epoch | 1;

        // Now we must store `state` into `self.state`. It's important that any succeeding loads
        // don't get reordered with this store. In order words, this participant's epoch must be
        // fully announced to other participants. Only then it becomes safe to load from the shared
        // memory.
        if cfg!(any(target_arch = "x86", target_arch = "x86_64")) {
            // On x86 architectures we have a choice:
            // 1. `atomic::fence(SeqCst)`, which compiles to a `mfence` instruction.
            // 2. `compare_and_swap(_, _, SeqCst)`, which compiles to a `lock cmpxchg` instruction.
            //
            // Both instructions have the effect of a full barrier, but the second one seems to be
            // faster in this particular case.
            let result = self.state.compare_and_swap(0, state, Ordering::SeqCst);
            debug_assert_eq!(0, result, "LocalEpoch::set_pinned()'s CAS should succeed.");
        } else {
            self.state.store(state, Ordering::Relaxed);
            ::std::sync::atomic::fence(Ordering::SeqCst);
        }
    }

    /// Marks the participant as unpinned.
    #[inline]
    pub fn set_unpinned(&self) {
        // Clear the last bit.
        // We don't need to preserve the epoch, so just store the number zero.
        self.state.store(0, Ordering::Release);
    }
}
