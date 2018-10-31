use core::sync::atomic::Ordering;

use {Atomic, Owned, Shared, Guard};

/// An `AtomicBox` is a shared, mutable, eventually consistent, lock-free data-structure.  This
/// allows multiple threads to communicate in a decoupled way by publishing data to the `AtomicBox`
/// which other threads can then read in an eventually consistent way.
///
/// This is not a silver bullet though, there are various limitations of `AtomicBox` that trade off
/// the nice behaviour described above.
///
/// * Eventual consistency:
///     * Writes from one thread are not guaranteed to be seen by reads from another thread
///     * Writes from one thread can overwrite writes from another thread
/// * No in-place mutation:
///     * The only write primitive completely overwrites the data on the `AtomicBox`
/// * Requires `Clone`:
///     * All reads return a clone of the data, decoupling the lifetime of the read value from the
///     data stored in the global reference.
///
/// # Examples
///
/// FIXME: copy-and-pasted from Pinboard
pub struct AtomicBox<T>(Atomic<T>);

impl<T> Default for AtomicBox<T> {
    fn default() -> Self {
        Self { 0: Atomic::null() }
    }
}

impl<T: Send + 'static> AtomicBox<T> {
    /// Create a new `AtomicBox` holding the given value.
    pub fn new(t: T) -> Self {
        Self { 0: Atomic::new(t) }
    }

    #[inline]
    fn swap_internal<'g>(&'g self, new: Shared<'g, T>, guard: &'g Guard) -> Shared<'g, T> {
        self.0.swap(new, Ordering::AcqRel, guard)
    }

    #[inline]
    fn set_internal<'g>(&'g self, new: Shared<'g, T>, guard: &'g Guard) {
        let previous = self.swap_internal(new, guard);
        if !previous.is_null() {
            unsafe { guard.defer_destroy(previous); }
        }
    }

    /// Set the value stored in the `AtomicBox`. The old value will be deallocated.
    pub fn set(&self, t: T, guard: &Guard) {
        self.set_internal(Owned::new(t).into_shared(guard), guard);
    }

    /// Clear the value stored in the `AtomicBox`. The old value will be deallocated.
    pub fn clear(&self, guard: &Guard) {
        self.set_internal(Shared::null(), guard);
    }
}

impl<T: Send + Sync + 'static> AtomicBox<T> {
    /// Get a reference to a version of the posted data.
    pub fn get<'g>(&'g self, guard: &'g Guard) -> Option<&'g T> {
        unsafe { self.0.load(Ordering::Acquire, guard).as_ref() }
    }

    /// Get a reference to a version of the posted data.
    pub fn swap<'g>(&'g self, t: T, guard: &'g Guard) -> Option<&'g T> {
        unsafe { self.swap_internal(Owned::new(t).into_shared(guard), guard).as_ref() }
    }

    pub fn take<'g>(&'g self, guard: &'g Guard) -> Option<&'g T> {
        unsafe { self.swap_internal(Shared::null(), guard).as_ref() }
    }
}

impl<T: Send + Sync + Clone + 'static> AtomicBox<T> {
    pub fn get_clone(&self, guard: &Guard) -> Option<T> {
        self.get(guard).map(|t| t.clone())
    }

    pub fn swap_clone_internal<'g>(&'g self, shared: Shared<'g, T>, guard: &'g Guard) -> Option<T> {
        let previous = self.swap_internal(shared, guard);
        unsafe { previous.as_ref() }.map(|previous_ref| {
            let result = previous_ref.clone();
            unsafe { guard.defer_destroy(previous); }
            result
        })
    }

    pub fn swap_clone(&self, t: T, guard: &Guard) -> Option<T> {
        self.swap_clone_internal(Owned::new(t).into_shared(guard), guard)
    }

    pub fn take_clone(&self, guard: &Guard) -> Option<T> {
        self.swap_clone_internal(Shared::null(), guard)
    }
}
