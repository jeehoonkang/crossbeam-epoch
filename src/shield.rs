use core::mem;
use core::cell::Cell;
use core::marker::PhantomData;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering;
use core::sync::atomic::fence;

use atomic::{Pointer, Shared};
use internal::Local;

pub struct ShieldsIter {
    indexes: usize,
    pointers: *const [AtomicUsize; Shields::NUM_SHIELDS],
}

impl Iterator for ShieldsIter {
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

/// FIXME
pub struct Shields {
    inuse: AtomicUsize,
    pointers: [AtomicUsize; Shields::NUM_SHIELDS],
}

impl Shields {
    /// Number of shields for each `Local`.
    const NUM_SHIELDS: usize = 5;

    /// Creates a new Shields.
    pub fn new() -> Self {
        Self {
            inuse: AtomicUsize::new(0),
            pointers: Default::default(),
        }
    }

    /// Acquires a shield.
    pub fn acquire(&self) -> Option<usize> {
        debug_assert!(Self::NUM_SHIELDS <= mem::size_of::<usize>(),
                      "NUM_SHIELDS should be <= size_of(usize).");

        let inuse = self.inuse.load(Ordering::Relaxed);
        let index = (!inuse).trailing_zeros() as usize;

        if index >= Self::NUM_SHIELDS {
            return None;
        }

        self.inuse.store(inuse | (1 << index), Ordering::Relaxed);
        Some(index)
    }

    /// Releases a shield.
    pub fn release(&self, index: usize) {
        fence(Ordering::Release);
        let inuse = self.inuse.load(Ordering::Relaxed);
        self.inuse.store(inuse & !(1 << index), Ordering::Relaxed);
        self.pointers[index].store(0, Ordering::Relaxed);
    }

    /// FIXME
    pub fn iter(&self) -> ShieldsIter {
        let inuse = self.inuse.load(Ordering::Relaxed);
        ShieldsIter {
            indexes: inuse,
            pointers: &self.pointers as *const _,
        }
    }
}

/// FIXME
pub struct Shield<T> {
    data: Cell<usize>,
    local: Cell<*const Local>, // !Sync + !Send
    index: Cell<usize>,

    _marker: PhantomData<*const T>,
}

impl<T> Shield<T> {
    pub(crate) fn new(data: usize, local: *const Local, index: usize) -> Self {
        unsafe { (*local).acquire_handle(); }
        Self {
            data: Cell::new(data),
            local: Cell::new(local),
            index: Cell::new(index),
            _marker: PhantomData,
        }
    }

    fn local(&self) -> &Local {
        unsafe { &*self.local.get() }
    }

    pub fn get<'g>(&'g self) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data.get()) }
    }

    pub fn defend<'g>(&self, shared: Shared<'g, T>) {
        let data = shared.into_usize();
        self.local().shields.pointers[self.index.get()].store(data, Ordering::Release);
        self.data.set(data);
    }

    #[inline]
    pub fn release(&self) {
        self.defend(Shared::null());
    }

    pub fn swap(&self, rhs: &Self) {
        self.data.swap(&rhs.data);
        self.local.swap(&rhs.local);
        self.index.swap(&rhs.index);
    }
}

impl<T> Drop for Shield<T> {
    fn drop(&mut self) {
        self.local().shields.release(self.index.get());
        self.local().release_handle();
    }
}
