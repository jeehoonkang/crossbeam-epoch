use core::cmp;
use core::ptr;
use core::cell::Cell;
use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering, fence};
use core::marker::PhantomData;

use internal::Local;
use tag::*;

/// TODO
#[derive(Debug)]
pub struct Root<T> {
    pub(crate) data: usize,
    pub(crate) local: *const Local,
    pub(crate) index: usize,
    pub(crate) _marker: PhantomData<(*const T)>, // !Sync + !Send
}

impl<T> Drop for Root<T> {
    fn drop(&mut self) {
        self.local().working_set.release_slot(self.index);
    }
}

impl<T> Root<T> {
    pub fn unprotected(data: usize) -> Self {
        Self {
            data,
            local: ptr::null(),
            index: 0,
            _marker: PhantomData,
        }
    }

    unsafe fn local(&self) -> &Local {
        &*self.local
    }

    pub fn as_raw(&self) -> *const T {
        let (raw, _) = decompose_data::<T>(self.data);
        raw
    }

    pub fn into_raw(self) -> usize {
        let result = self.data;
        drop(self);
        result            
    }

    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_data::<T>(self.data);
        tag
    }

    pub fn with_tag(mut self, tag: usize) -> Self {
        self.data = data_with_tag::<T>(self.data, tag);
        self
    }
}

impl<T> PartialEq<Root<T>> for Root<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T> Eq for Root<T> {}

impl<T> PartialOrd<Root<T>> for Root<T> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.data.partial_cmp(&other.data)
    }
}

impl<T> Ord for Root<T> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

/// TODO
pub struct RootSet {
    /// Which pointers are valid?
    valid_bits: [Cell<usize>; RootSet::SIZE],

    /// Which pointers are hazardous?
    hazard_bits: [AtomicUsize; RootSet::SIZE / (8 * mem::size_of::<usize>())],

    /// The array of pointers in use.
    pointers: [AtomicUsize; RootSet::SIZE],
}

impl RootSet {
    /// The size of working set.
    const SIZE: usize = 64;
    const WORD_SIZE: usize = 8 * mem::size_of::<usize>();
    const SIZE_IN_WORDS: usize = Self::SIZE / Self::WORD_SIZE;

    #[inline]
    fn decompose_bit_index(index: usize) -> (usize, usize) {
        (index / Self::SIZE_IN_WORDS, index % Self::SIZE_IN_WORDS)
    }

    pub fn new() -> Self {
        const_assert!(Self::SIZE % Self::WORD_SIZE == 0);
        unsafe { mem::zeroed() }
    }

    #[inline]
    fn set_valid_bit(&self, index: usize) {
        let (index1, index2) = Self::decompose_bit_index(index);
        let bits = self.valid_bits[index1].get();
        self.valid_bits[index1].set(bits | (1 << index2));
    }

    #[inline]
    fn unset_valid_bit(&self, index: usize) {
        let (index1, index2) = Self::decompose_bit_index(index);
        let bits = self.valid_bits[index1].get();
        self.valid_bits[index1].set(bits & !(1 << index2));
    }

    #[inline]
    fn set_hazard_bit(&self, index: usize) {
        let (index1, index2) = Self::decompose_bit_index(index);
        let bits = self.hazard_bits[index1].load(Ordering::Relaxed);
        self.hazard_bits[index1].store(bits | (1 << index2), Ordering::Relaxed);
    }

    #[inline]
    fn unset_hazard_bit(&self, index: usize) {
        let (index1, index2) = Self::decompose_bit_index(index);
        let bits = self.hazard_bits[index1].load(Ordering::Relaxed);
        self.hazard_bits[index1].store(bits & !(1 << index2), Ordering::Relaxed);
    }

    /// Acquires a pointer slot.
    #[inline]
    pub fn acquire_slot<T>(&self, data: usize) -> usize {
        for index1 in 0..Self::SIZE_IN_WORDS {
            let indexes2 = self.valid_bits[index1].get();
            let index2 = indexes2.trailing_zeros() as usize;

            if index2 < Self::WORD_SIZE {
                self.valid_bits[index1].set(indexes2 | (1 << index2));
                let index = index1 * Self::WORD_SIZE + index2;
                self.pointers[index].store(data_with_tag::<T>(data, 0), Ordering::Relaxed);
                return index;
            }
        }

        // TODO
        panic!("RootSet::acquire_slot: not enough slots");
    }

    /// Releases a pointer slot.
    #[inline]
    pub fn release_slot(&self, index: usize) {
        self.unset_valid_bit(index);

        fence(Ordering::Release);
        self.unset_hazard_bit(index);
        self.pointers[index].store(0, Ordering::Relaxed);
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
    working_set: *const RootSet,
}

impl Iterator for HazardIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let index2 = self.indexes2.trailing_zeros() as usize;

            if index2 >= RootSet::WORD_SIZE {
                self.indexes2 = unsafe { (*self.working_set).hazard_bits[self.index1].load(Ordering::Relaxed) };
                self.index1 += 1;

                if self.index1 >= RootSet::SIZE_IN_WORDS {
                    return None;
                }
            } else {
                self.indexes2 &= !(1 << index2);
                let pointer = unsafe {
                    ((*self.working_set).pointers)[self.index1 * RootSet::WORD_SIZE + index2].load(Ordering::Relaxed)
                };
                if pointer != 0 {
                    return Some(pointer);
                }
            }
        }
    }
}
