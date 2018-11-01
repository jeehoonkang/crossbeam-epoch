use core::cell::Cell;
use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering, fence};
use core::marker::PhantomData;

use internal::Local;
use atomic::{Shared, Pointer};

/// TODO
#[derive(Debug)]
pub struct HazardSet {
    /// Which pointers are hazardous?
    inuse: AtomicUsize,

    /// The array of hazard pointers.
    pointers: [AtomicUsize; HazardSet::NUM_SHIELDS],
}

impl HazardSet {
    /// Number of shields for each `Local`.
    const NUM_SHIELDS: usize = 16;

    pub fn new() -> Self {
        /// FIXME(@jeehoonkang): Currently `NUM_SHIELDS` should be <= `8 * size_of(usize)` because
        /// `inuse: AtomicUsize` is used as the valid bit.  We can relax it later.
        const_assert!(Self::NUM_SHIELDS <= 8 * mem::size_of::<usize>());

        Self {
            inuse: AtomicUsize::new(0),
            pointers: Default::default(),
        }
    }

    /// Acquires a shield.
    #[inline]
    pub fn acquire(&self, data: usize) -> Option<usize> {
        let inuse = self.inuse.load(Ordering::Relaxed);
        let index = (!inuse).trailing_zeros() as usize;

        if index >= Self::NUM_SHIELDS {
            return None;
        }

        self.inuse.store(inuse | (1 << index), Ordering::Relaxed);
        self.pointers[index].store(data, Ordering::Relaxed);
        Some(index)
    }

    /// Updates a shield.
    #[inline]
    pub fn update(&self, index: usize, data: usize) {
        self.pointers[index].store(data, Ordering::Release);
    }

    /// Releases a shield.
    #[inline]
    pub fn release(&self, index: usize) {
        fence(Ordering::Release);
        let inuse = self.inuse.load(Ordering::Relaxed);
        self.inuse.store(inuse & !(1 << index), Ordering::Relaxed);
        self.pointers[index].store(0, Ordering::Relaxed);
    }

    /// Returns an iterator for hazard pointers.
    pub fn iter(&self) -> HazardIter {
        let inuse = self.inuse.load(Ordering::Relaxed);
        HazardIter {
            indexes: inuse,
            pointers: &self.pointers as *const _,
        }
    }
}

/// TODO
#[derive(Debug)]
pub struct HazardIter {
    indexes: usize,
    pointers: *const [AtomicUsize; HazardSet::NUM_SHIELDS],
}

impl Iterator for HazardIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let index = self.indexes.trailing_zeros() as usize;

            if index >= mem::size_of::<usize>() {
                return None;
            } else {
                self.indexes &= !(1 << index);
                let pointer = unsafe { (*self.pointers)[index].load(Ordering::Relaxed) };
                if pointer != 0 {
                    return Some(pointer);
                }
            }
        }
    }
}

/// Shield of hazard pointers.
#[derive(Debug)]
pub struct Shield<T> {
    pub(crate) data: Cell<usize>,
    pub(crate) local: Cell<*const Local>, // !Sync + !Send
    pub(crate) index: Cell<usize>,
    pub(crate) _marker: PhantomData<*const T>,
}

impl<T> Shield<T> {
    #[inline]
    fn local(&self) -> &Local {
        unsafe { &*self.local.get() }
    }

    /// TODO
    #[inline]
    pub fn get<'g>(&'g self) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.get()) }
    }

    /// TODO
    pub fn defend<'g>(&self, shared: Shared<'g, T>) {
        let data = shared.into_usize();
        self.local().update_shield::<T>(self.index.get(), data);
        self.data.set(data);
    }    

    /// TODO
    #[inline]
    pub fn release(&self) {
        self.defend(Shared::null());
    }

    /// TODO
    pub fn swap(&self, rhs: &Self) {
        self.data.swap(&rhs.data);
        self.local.swap(&rhs.local);
        self.index.swap(&rhs.index);
    }
}

impl<T> Drop for Shield<T> {
    fn drop(&mut self) {
        self.local().release_shield::<T>(self.index.get());
    }
}
