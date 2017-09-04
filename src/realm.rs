use std::cmp;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use atomic::Ptr;
use mutator::LocalEpoch;
use garbage::{Garbage, Bag};
use epoch::Epoch;
use sync::list::List;
use sync::queue::Queue;


/// Number of bags to destroy.
const COLLECT_STEPS: usize = 8;


/// An anchor that allows the access to the shared memory.
///
/// Lots of methods that interact with [`Atomic`]s can safely be called only while the mutator is
/// pinned so they often require a `Scope`.
///
/// [`Atomic`]: struct.Atomic.html
pub trait Scope<'scope>
where
    Self: Copy + Sized + 'scope,
{
    /// Defers disposal of a garbage.
    unsafe fn defer_dispose(self, garbage: Garbage);

    /// Flushes all garbage in the thread-local storage into the global garbage queue, attempts to
    /// advance the epoch, and collects some garbage.
    fn flush(self);

    /// Defers deallocation of the heap-allocated object `ptr`.
    ///
    /// If the object is unusually large, it is wise to follow up with a call to [`flush`] so that
    /// it doesn't get stuck waiting in the local bag for a long time.
    ///
    /// [`flush`]: fn.flush.html
    unsafe fn defer_free<T>(self, ptr: Ptr<'scope, T>) {
        self.defer_dispose(Garbage::new_free(ptr.as_raw() as *mut T, 1))
    }

    /// Defers destruction and deallocation of the heap-allocated object `ptr`.
    // FIXME(jeehoonkang): `T: 'static` may be too restrictive.
    unsafe fn defer_drop<T: Send + 'static>(self, ptr: Ptr<'scope, T>) {
        self.defer_dispose(Garbage::new_drop(ptr.as_raw() as *mut T, 1))
    }

    /// Deferred execution of an arbitrary function `f`.
    unsafe fn defer<F: FnOnce() + Send + 'static>(self, f: F) {
        self.defer_dispose(Garbage::new(f))
    }
}

/// An EBR-managed region of shared memory. The epoch advancement of a realm is not blocked by the
/// mutators of other realms.
pub trait Realm<'scope>
where
    Self: Copy + Sized + 'scope,
{
    /// The head pointer of the list of mutator registries.
    fn registries(self) -> &'scope List<LocalEpoch>;
    /// The reference to the global queue of garbages.
    fn garbages(self) -> &'scope Queue<(usize, Bag)>;
    /// The reference to the global epoch.
    fn epoch(self) -> &'scope Epoch;

    /// Pushes the bag onto the global queue and replaces the bag with a new empty bag.
    #[inline]
    fn push_bag<'s, S>(self, bag: &mut Bag, scope: S)
    where
        'scope: 's,
        S: Scope<'s>,
    {
        let epoch = self.epoch().load(Relaxed);
        let bag = ::std::mem::replace(bag, Bag::new());
        ::std::sync::atomic::fence(SeqCst);
        self.garbages().push((epoch, bag), scope);
    }

    /// Collect several bags from the global old garbage queue and destroys their objects.
    ///
    /// Note: This may itself produce garbage and in turn allocate new bags.
    fn collect<'s, S>(self, scope: S)
    where
        'scope: 's,
        S: Scope<'s>,
    {
        let epoch = self.epoch().try_advance(self.registries(), scope);

        let condition = |bag: &(usize, Bag)| {
            // A pinned thread can witness at most one epoch advancement. Therefore, any bag that is
            // within one epoch of the current one cannot be destroyed yet.
            let diff = epoch.wrapping_sub(bag.0);
            cmp::min(diff, 0usize.wrapping_sub(diff)) > 2
        };

        let garbages = &self.garbages();
        for _ in 0..COLLECT_STEPS {
            match garbages.try_pop_if(&condition, scope) {
                None => break,
                Some(bag) => drop(bag),
            }
        }
    }
}

/// Custom realm. `&'scope UserRealm` implements `Realm<'scope>`.
///
/// # Examples
///
/// ```
/// use crossbeam_epoch::{Scope, UserRealm, Mutator};
///
/// let realm = UserRealm::new();
/// let mutator = Mutator::new(&realm);
/// mutator.pin(|scope| {
///     unsafe {
///         scope.defer(|| {
///             println!("hello, world!");
///         });
///     }
/// });
/// ```
#[derive(Default, Debug)]
pub struct UserRealm {
    registries: List<LocalEpoch>,
    garbages: Queue<(usize, Bag)>,
    epoch: Epoch,
}

impl<'scope> Realm<'scope> for &'scope UserRealm {
    fn registries(self) -> &'scope List<LocalEpoch> {
        &self.registries
    }

    fn garbages(self) -> &'scope Queue<(usize, Bag)> {
        &self.garbages
    }

    fn epoch(self) -> &'scope Epoch {
        &self.epoch
    }
}

impl UserRealm {
    pub fn new() -> Self {
        Self::default()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use mutator::Mutator;

    #[test]
    fn user_realm() {
        let realm = UserRealm::new();
        let mutator = Mutator::new(&realm);
        mutator.pin(|_scope| {});
    }
}
