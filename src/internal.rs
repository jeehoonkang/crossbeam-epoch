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

use std::cell::UnsafeCell;
use std::cmp;
use std::sync::atomic::Ordering;

use garbage::{Garbage, Bag};
use epoch::{Epoch, LocalEpoch};
use collector::{Guard, unprotected};
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
    bag: UnsafeCell<Bag>,
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
    pub fn collect(&self, guard: &Guard) {
        let epoch = self.epoch.try_advance(&self.registries, guard);

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

unsafe impl Sync for Local {}

impl Local {
    /// Creates a participant to the garbage collection global data.
    #[inline]
    pub fn new() -> Self {
        Self {
            local_epoch: Node::new(LocalEpoch::new()),
            bag: UnsafeCell::new(Bag::new()),
        }
    }

    /// FIXME
    pub unsafe fn register(&self, global: &Global) {
        // Since we dereference no pointers in this block, it is safe to use `unprotected`.
        global.registries.insert(&self.local_epoch, &unprotected());
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

        // Pin itself. FIXME
        self.pin(global);
        let guard = unprotected();

        // Spare some cycles on garbage collection.
        global.collect(&guard);

        // Unregister the participant by marking this entry as deleted.
        self.local_epoch.delete(&guard);

        // Push the local bag into the global garbage queue.
        global.push_bag(&mut *self.bag.get(), &guard);

        // Unpin itself.
        self.unpin(global);
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
    pub unsafe fn pin(&self, global: &Global) {
        // Mark this participant as pinned.
        let epoch = global.get_epoch();
        self.local_epoch.get().set_pinned(epoch);
    }

    /// FIXME
    pub unsafe fn unpin(&self, global: &Global) {
        // Mark this participant as unpinned.
        self.local_epoch.get().set_unpinned();
    }

    /// FIXME
    pub unsafe fn push_garbage(&self, global: &Global, mut garbage: Garbage) {
        let bag = &mut *self.bag.get();
        while let Err(g) = bag.try_push(garbage) {
            global.push_bag(bag, self);
            garbage = g;
        }
    }
}
