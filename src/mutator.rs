//! Mutator: an entity that changes shared locations
//!
//! # Registration
//!
//! In order to track all mutators in one place, we need some form of mutator registration. When a
//! mutator is created, it is registered to a global lock-free singly-linked list of registries; and
//! when a mutator is dropped, it is unregistered from the list.
//!
//! # Pinning
//!
//! Every mutator contains an integer that tells whether the mutator is pinned and if so, what was
//! the global epoch at the time it was pinned. Mutators also hold a pin counter that aids in
//! periodic global epoch advancement.
//!
//! When a mutator is pinned, a `EpochScope` is returned as a witness that the mutator is pinned.
//! `EpochScope`s are necessary for performing atomic operations, and for freeing/dropping
//! locations.

use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, Release, SeqCst};
use std::marker::PhantomData;

use garbage::{Garbage, Bag};
use realm::{Scope, Realm};
use sync::list::Node;


/// Number of pinnings after which a mutator will collect some global garbage.
const PINS_BETWEEN_COLLECT: usize = 128;


/// An entry in the linked list of the registered mutators.
#[derive(Default, Debug)]
pub struct LocalEpoch {
    /// The least significant bit is set if the mutator is currently pinned. The rest of the bits
    /// encode the current epoch.
    state: AtomicUsize,
}

/// Entity that changes shared locations.
pub struct Mutator<'scope, R>
where
    R: Realm<'scope>,
{
    /// The realm it belongs to.
    realm: R,
    /// The local garbage objects that will be later freed.
    bag: UnsafeCell<Bag>,
    /// This mutator's entry in the local epoch list.
    local_epoch: &'scope Node<LocalEpoch>,
    /// Whether the mutator is currently pinned.
    is_pinned: Cell<bool>,
    /// Total number of pinnings performed.
    pin_count: Cell<usize>,
}

/// A witness that the current mutator is pinned.
///
/// A reference to `EpochScope` is a witness that the current mutator is pinned. A reference to
/// `EpochScope` implements [`Scope`], which can be used to interact with [`Atomic`]s.
///
/// This data type is inherently bound to the thread that created it, therefore it does not
/// implement `Send` nor `Sync`.
///
/// [`Scope`]: trait.Scope.html
/// [`Atomic`]: struct.Atomic.html
#[derive(Debug)]
pub struct EpochScope<'scope, R>
where
    R: Realm<'scope>,
{
    realm: R,
    bag: *mut Bag, // !Send + !Sync
    _marker: PhantomData<&'scope Bag>,
}

/// An unsafe `Scope`.
///
/// See [`unprotected`] for more details.
///
/// [`unprotected`]: fn.unprotected.html
#[derive(Debug, Clone, Copy, Default)]
pub struct UnprotectedScope {}

impl<'scope, R> Mutator<'scope, R>
where
    R: Realm<'scope>,
{
    /// Create a mutator.
    pub fn new(realm: R) -> Self {
        Mutator {
            realm: realm,
            bag: UnsafeCell::new(Bag::new()),
            local_epoch: unsafe {
                // Since we dereference no pointers in this block, it is safe to call `unprotected`.
                unprotected(|scope| {
                    &*realm
                        .registries()
                        .insert_head(LocalEpoch::new(), scope)
                        .as_raw()
                })
            },
            is_pinned: Cell::new(false),
            pin_count: Cell::new(0),
        }
    }

    /// Pins the current mutator, executes a function, and unpins the mutator.
    ///
    /// The provided function takes a reference to a [`EpochScope`], which can be used to interact
    /// with [`Atomic`]s. The scope serves as a proof that whatever data you load from an [`Atomic`]
    /// will not be concurrently deleted by another mutator while the scope is alive.
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
    /// [`EpochScope`]: struct.EpochScope.html
    pub fn pin<F, V>(&self, f: F) -> V
    where
        F: FnOnce(&EpochScope<'scope, R>) -> V,
    {
        let local_epoch = self.local_epoch.get();
        let scope = &EpochScope {
            realm: self.realm,
            bag: self.bag.get(),
            _marker: PhantomData,
        };

        let was_pinned = self.is_pinned.get();
        if !was_pinned {
            // Increment the pin counter.
            let count = self.pin_count.get();
            self.pin_count.set(count.wrapping_add(1));

            // Pin the mutator.
            self.is_pinned.set(true);
            let epoch = self.realm.epoch().load(Relaxed);
            local_epoch.set_pinned(epoch);

            // If the counter progressed enough, try advancing the epoch and collecting garbage.
            if count % PINS_BETWEEN_COLLECT == 0 {
                self.realm.collect(scope);
            }
        }

        // This will unpin the mutator even if `f` panics.
        defer! {
            if !was_pinned {
                // Unpin the mutator.
                local_epoch.set_unpinned();
                self.is_pinned.set(false);
            }
        }

        f(scope)
    }

    /// Returns `true` if the current mutator is pinned.
    pub fn is_pinned<'s>(&'s self) -> bool
    where
        'scope: 's,
    {
        self.is_pinned.get()
    }
}

impl<'scope, R> Drop for Mutator<'scope, R>
where
    R: Realm<'scope>,
{
    fn drop(&mut self) {
        // Now that the mutator is exiting, we must move the local bag into the global garbage
        // queue. Also, let's try advancing the epoch and help free some garbage.

        self.pin(|scope| {
            // Spare some cycles on garbage collection.
            self.realm.collect(scope);

            // Unregister the mutator by marking this entry as deleted.
            self.local_epoch.delete(scope);

            // Push the local bag into the global garbage queue.
            unsafe {
                self.realm.push_bag(&mut *self.bag.get(), scope);
            }
        });
    }
}

/// Returns a [`Scope`] without pinning any mutator.
///
/// Sometimes, we'd like to have longer-lived scopes in which we know our thread is the only one
/// accessing atomics. This is true e.g. when destructing a big data structure, or when constructing
/// it from a long iterator. In such cases we don't need to be overprotective because there is no
/// fear of other threads concurrently destroying objects.
///
/// Function `unprotected` is *unsafe* because we must promise that no other thread is accessing the
/// Atomics and objects at the same time. The function is safe to use only if (1) the locations that
/// we access should not be deallocated by concurrent mutators, and (2) the locations that we
/// deallocate should not be accessed by concurrent mutators.
///
/// Just like with the safe `epoch::pin` function, unprotected use of atomics is enclosed within a
/// scope so that pointers created within it don't leak out or get mixed with pointers from other
/// scopes.
///
/// The garbages created inside an unprotected scope are immediately disposed.
#[inline]
pub unsafe fn unprotected<F, R>(f: F) -> R
where
    F: FnOnce(UnprotectedScope) -> R,
{
    f(UnprotectedScope::default())
}

impl LocalEpoch {
    // FIXME(stjepang): Registries are stored in a linked list because linked lists are fairly easy
    // to implement in a lock-free manner. However, traversal is rather slow due to cache misses and
    // data dependencies. We should experiment with other data structures as well.

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

impl<'s, 'scope, R> Scope<'s> for &'s EpochScope<'scope, R>
where
    'scope: 's,
    R: Realm<'scope>,
{
    /// Deferred disposal of garbage.
    ///
    /// This function inserts the garbage into a mutator-local [`Bag`]. When the bag becomes full,
    /// the bag is flushed into the globally shared queue of bags.
    ///
    /// [`Bag`]: struct.Bag.html
    unsafe fn defer_dispose(self, mut garbage: Garbage) {
        let bag = &mut *self.bag;

        while let Err(g) = bag.try_push(garbage) {
            self.realm.push_bag(bag, self);
            garbage = g;
        }
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
    /// [`defer_free`]: fn.defer_free.html
    /// [`defer_drop`]: fn.defer_drop.html
    fn flush(self) {
        unsafe {
            let bag = &mut *self.bag;
            if !bag.is_empty() {
                self.realm.push_bag(bag, self);
            }
        }

        self.realm.collect(self);
    }
}

impl<'scope> Scope<'scope> for UnprotectedScope {
    /// Disposes garbage immediately.
    unsafe fn defer_dispose(self, garbage: Garbage) {
        drop(garbage)
    }

    /// Does nothing.
    fn flush(self) {}
}


#[cfg(test)]
mod tests {}
