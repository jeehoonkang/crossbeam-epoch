use core::cell::Cell;
use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering, fence};
use core::marker::PhantomData;

use internal::Local;
use atomic::{Shared, Pointer};

/// TODO
pub struct WorkingSet {
    /// Which pointers are hazardous?
    hazard_bits: [AtomicUsize; WorkingSet::SIZE / (8 * mem::size_of::<usize>())],

    /// Reference counters.
    ref_counts: [Cell<usize>; WorkingSet::SIZE],

    /// The array of pointers in use.
    pointers: [AtomicUsize; WorkingSet::SIZE],
}

impl WorkingSet {
    /// The size of working set.
    const SIZE: usize = 64;

    pub fn new() -> Self {
        const_assert!(WorkingSet::SIZE % (8 * mem::size_of::<usize>()) == 0);

        unsafe { mem::zeroed() }
    }

    #[inline]
    fn set_hazard_bit(&self, index: usize) {
        let index1 = index / (8 * mem::size_of::<usize>());
        let index2 = index % (8 * mem::size_of::<usize>());
        let bits = self.hazard_bits[index1].load(Ordering::Relaxed);
        self.hazard_bits[index1].store(bits | (1 << index2), Ordering::Relaxed);
    }

    #[inline]
    fn unset_hazard_bit(&self, index: usize) {
        let index1 = index / (8 * mem::size_of::<usize>());
        let index2 = index % (8 * mem::size_of::<usize>());
        let bits = self.hazard_bits[index1].load(Ordering::Relaxed);
        self.hazard_bits[index1].store(bits & !(1 << index2), Ordering::Release);
    }

    /// Acquires a pointer entry.
    #[inline]
    pub fn acquire(&self, data: usize) -> Option<usize> {
        for index in 0..Self::SIZE {
            let ref_count = &self.ref_counts[index];
            if ref_count.get() == 0 {
                ref_count.set(1);
                self.pointers[index].store(data, Ordering::Relaxed);
                return Some(index);
            }
        }

        None
    }

    /// Increments a reference to a pointer entry.
    #[inline]
    pub fn increment(&self, index: usize) {
        let ref_count = &self.ref_counts[index];
        ref_count.set(ref_count.get() + 1);
    }

    /// Releases a reference to a pointer entry.
    #[inline]
    pub fn release(&self, index: usize) {
        let ref_count = &self.ref_counts[index];
        let value = ref_count.get();
        ref_count.set(value - 1);

        if value == 1 {
            fence(Ordering::Release);
            self.pointers[index].store(0, Ordering::Relaxed);
            self.unset_hazard_bit(index);
        }
    }


    /// Returns an iterator for hazard pointers.
    pub fn iter(&self) -> HazardIter {
        HazardIter {
            index1: 0,
            indexes2: 0,
            working_set: self as *const _,
        }
    }
}

/// TODO
#[derive(Debug)]
pub struct HazardIter {
    index1: usize,
    indexes2: usize,
    working_set: *const WorkingSet,
}

impl Iterator for HazardIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.indexes2 == 0 {
            self.indexes2 = unsafe { (*self.working_set).hazard_bits[self.index1].load(Ordering::Relaxed) };
            self.index1 += 1;

            if self.index1 >= WorkingSet::SIZE / (8 * mem::size_of::<usize>()) {
                return None;
            }
        }

        loop {
            let index2 = self.indexes2.trailing_zeros() as usize;

            if index2 >= 8 * mem::size_of::<usize>() {
                self.indexes2 = unsafe { (*self.working_set).hazard_bits[self.index1].load(Ordering::Relaxed) };
                self.index1 += 1;

                if self.index1 >= WorkingSet::SIZE / (8 * mem::size_of::<usize>()) {
                    return None;
                }
            } else {
                self.indexes2 &= !(1 << index2);
                let pointer = unsafe {
                    ((*self.working_set).pointers)
                        [self.index1 * (8 * mem::size_of::<usize>()) + index2]
                        .load(Ordering::Relaxed)
                };
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
        // TODO: self.local().update_shield::<T>(self.index.get(), data);
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
