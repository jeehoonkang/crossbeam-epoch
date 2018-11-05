use core::ptr;
use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering, fence, compiler_fence};
use core::marker::PhantomData;

use atomic::{Owned, Shared, Pointer};
use internal::Local;
use tag::*;

/// TODO
#[derive(Debug)]
pub enum ShieldError {
    /// TODO
    Ejected,
}

/// TODO
#[derive(Debug)]
pub struct Shield<'g, T: 'g> {
    pub(crate) data: usize,
    pub(crate) local: *const Local,
    pub(crate) index: usize,
    pub(crate) _marker: PhantomData<(&'g T)>, // !Sync + !Send
}

impl<'g, T> Drop for Shield<'g, T> {
    fn drop(&mut self) {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.shield_set.release(self.index);
        }
    }
}

impl<'g, T> Shield<'g, T> {
    /// TODO
    pub fn defend<'s>(&mut self, ptr: Shared<'s, T>) -> Result<(), ShieldError>
    where
        'g: 's
    {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.shield_set.update::<T>(self.index, ptr.into_usize());
            compiler_fence(Ordering::SeqCst);
            if local.is_ejected() {
                return Err(ShieldError::Ejected);
            }
        }

        Ok(())
    }

    /// TODO
    pub fn release(&mut self) {
        let _ = self.defend(Shared::null());
    }

    /// TODO
    pub fn preserve(&mut self) -> Shield<'static, T> {
        if let Some(local) = unsafe { self.local.as_ref() } {
            local.shield_set.set_preserved_bit(self.index);
            Shield {
                data: self.data,
                local: self.local,
                index: self.index,
                _marker: PhantomData,
            }
        } else {
            Shield {
                data: self.data,
                local: self.local,
                index: self.index,
                _marker: PhantomData,
            }
        }
    }

    /// TODO
    pub fn as_raw(&self) -> *const T {
        let (raw, _) = decompose_data::<T>(self.data);
        raw
    }

    /// TODO: Dereferences the pointer.
    ///
    /// Returns a reference to the pointee that is valid during the lifetime `'g`.
    ///
    /// # Safety
    ///
    /// Dereferencing a pointer is unsafe because it could be pointing to invalid memory.
    ///
    /// Another concern is the possiblity of data races due to lack of proper synchronization.
    /// For example, consider the following scenario:
    ///
    /// 1. A thread creates a new object: `a.store(Owned::new(10), Relaxed)`
    /// 2. Another thread reads it: `*a.load(Relaxed, guard).as_ref().unwrap()`
    ///
    /// The problem is that relaxed orderings don't synchronize initialization of the object with
    /// the read from the second thread. This is a data race. A possible solution would be to use
    /// `Release` and `Acquire` orderings.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_epoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234);
    /// let guard = &epoch::pin();
    /// let p = a.load(SeqCst, guard);
    /// unsafe {
    ///     assert_eq!(p.deref(), &1234);
    /// }
    /// ```
    pub unsafe fn deref(&self) -> &'g T {
        &*self.as_raw()
    }

    /// TODO: Converts the pointer to a reference.
    ///
    /// Returns `None` if the pointer is null, or else a reference to the object wrapped in `Some`.
    ///
    /// # Safety
    ///
    /// Dereferencing a pointer is unsafe because it could be pointing to invalid memory.
    ///
    /// Another concern is the possiblity of data races due to lack of proper synchronization.
    /// For example, consider the following scenario:
    ///
    /// 1. A thread creates a new object: `a.store(Owned::new(10), Relaxed)`
    /// 2. Another thread reads it: `*a.load(Relaxed, guard).as_ref().unwrap()`
    ///
    /// The problem is that relaxed orderings don't synchronize initialization of the object with
    /// the read from the second thread. This is a data race. A possible solution would be to use
    /// `Release` and `Acquire` orderings.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_epoch::{self as epoch, Atomic};
    /// use std::sync::atomic::Ordering::SeqCst;
    ///
    /// let a = Atomic::new(1234);
    /// let guard = &epoch::pin();
    /// let p = a.load(SeqCst, guard);
    /// unsafe {
    ///     assert_eq!(p.as_ref(), Some(&1234));
    /// }
    /// ```
    pub unsafe fn as_ref(&self) -> Option<&'g T> {
        self.as_raw().as_ref()
    }

    /// TODO
    pub unsafe fn into_owned(self) -> Owned<T> {
        debug_assert!(
            self.as_raw() != ptr::null(),
            "converting a null `Shared` into `Owned`"
        );
        Owned::from_usize(self.data)
    }

    /// TODO
    pub fn shared(&self) -> Shared<'g, T> {
        unsafe { Shared::from_usize(self.data) }
    }

    /// TODO
    pub fn tag(&self) -> usize {
        let (_, tag) = decompose_data::<T>(self.data);
        tag
    }

    /// TODO
    pub fn with_tag(mut self, tag: usize) -> Self {
        self.data = data_with_tag::<T>(self.data, tag);
        self
    }
}

/// TODO
pub struct ShieldSet {
    /// Which pointers are valid?
    valid_bits: [AtomicUsize; ShieldSet::SIZE_IN_WORDS],

    /// Which pointers are preserved?
    preserved_bits: [AtomicUsize; ShieldSet::SIZE_IN_WORDS],

    /// The array of pointers in use.
    pointers: [AtomicUsize; ShieldSet::SIZE],
}

impl ShieldSet {
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
        let bits = self.valid_bits[index1].load(Ordering::Relaxed);
        self.valid_bits[index1].store(bits | (1 << index2), Ordering::Relaxed);
    }

    #[inline]
    fn unset_valid_bit(&self, index: usize) {
        let (index1, index2) = Self::decompose_bit_index(index);
        let bits = self.valid_bits[index1].load(Ordering::Relaxed);
        self.valid_bits[index1].store(bits & !(1 << index2), Ordering::Relaxed);
    }

    #[inline]
    fn set_preserved_bit(&self, index: usize) {
        let (index1, index2) = Self::decompose_bit_index(index);
        let bits = self.preserved_bits[index1].load(Ordering::Relaxed);
        self.preserved_bits[index1].store(bits | (1 << index2), Ordering::Relaxed);
    }

    #[inline]
    fn unset_preserved_bit(&self, index: usize) {
        let (index1, index2) = Self::decompose_bit_index(index);
        let bits = self.preserved_bits[index1].load(Ordering::Relaxed);
        self.preserved_bits[index1].store(bits & !(1 << index2), Ordering::Relaxed);
    }

    /// Acquires a pointer slot.
    #[inline]
    pub fn acquire<T>(&self) -> usize {
        for index1 in 0..Self::SIZE_IN_WORDS {
            let indexes2 = self.valid_bits[index1].load(Ordering::Relaxed);
            let index2 = indexes2.trailing_zeros() as usize;

            if index2 < Self::WORD_SIZE {
                self.valid_bits[index1].store(indexes2 | (1 << index2), Ordering::Relaxed);
                let index = index1 * Self::WORD_SIZE + index2;
                return index;
            }
        }

        panic!("ShieldSet::acquire(): out of slots");
    }

    /// Releases a pointer slot.
    #[inline]
    pub fn release(&self, index: usize) {
        fence(Ordering::Release);
        self.unset_valid_bit(index);
        self.unset_preserved_bit(index);
        self.pointers[index].store(0, Ordering::Relaxed);
    }

    /// Updates a pointer slot.
    #[inline]
    pub fn update<T>(&self, index: usize, data: usize) {
        self.pointers[index].store(data_with_tag::<T>(data, 0), Ordering::Release);
    }

    /// Returns an iterator for hazard pointers.
    pub fn iter(&self) -> HazardIter {
        HazardIter {
            index1: 0,
            indexes2: 0,
            root_set: self as *const _,
        }
    }
}

/// TODO
#[derive(Debug)]
pub struct HazardIter {
    index1: usize,
    indexes2: usize,
    root_set: *const ShieldSet,
}

impl Iterator for HazardIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let index2 = self.indexes2.trailing_zeros() as usize;

            if index2 >= ShieldSet::WORD_SIZE {
                self.indexes2 = unsafe { (*self.root_set).preserved_bits[self.index1].load(Ordering::Relaxed) };
                self.index1 += 1;

                if self.index1 >= ShieldSet::SIZE_IN_WORDS {
                    return None;
                }
            } else {
                self.indexes2 &= !(1 << index2);
                let pointer = unsafe {
                    ((*self.root_set).pointers)[self.index1 * ShieldSet::WORD_SIZE + index2].load(Ordering::Relaxed)
                };
                if pointer != 0 {
                    return Some(pointer);
                }
            }
        }
    }
}
