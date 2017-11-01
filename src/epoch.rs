//! The global epoch
//!
//! The last bit in this number is unused and is always zero. Every so often the global epoch is
//! incremented, i.e. we say it "advances". A pinned participant may advance the global epoch only
//! if all currently pinned participants have been pinned in the current epoch.
//!
//! If an object became garbage in some epoch, then we can be sure that after two advancements no
//! participant will hold a reference to it. That is the crux of safe memory reclamation.

use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use collector::Guard;
use sync::list::{List, IterError};
use crossbeam_utils::cache_padded::CachePadded;

/// The global epoch is a (cache-padded) integer.
#[derive(Default, Debug)]
pub struct Epoch {
    epoch: CachePadded<AtomicUsize>,
}

/// An entry in the linked list of the registered participants.
#[derive(Default, Debug)]
pub struct LocalEpoch {
    /// The least significant bit is set if the participant is currently pinned. The rest of the
    /// bits encode the current epoch.
    state: AtomicUsize,
}

impl Epoch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempts to advance the global epoch.
    ///
    /// The global epoch can advance only if all currently pinned participants have been pinned in
    /// the current epoch.
    ///
    /// Returns the current global epoch.
    ///
    /// `try_advance()` is annotated `#[cold]` because it is rarely called.
    #[cold]
    pub fn try_advance(&self, registries: &List<LocalEpoch>, guard: &Guard) -> usize {
        let epoch = self.epoch.load(Ordering::Relaxed);
        ::std::sync::atomic::fence(Ordering::SeqCst);

        // Traverse the linked list of participant registries.
        for participant in registries.iter(guard) {
            match participant {
                Err(IterError::LostRace) => {
                    // We leave the job to the participant that won the race, which continues to
                    // iterate the registries and tries to advance to epoch.
                    return epoch;
                }
                Ok(local) => {
                    let (participant_is_pinned, participant_epoch) = local.get().get();

                    // If the participant was pinned in a different epoch, we cannot advance the
                    // global epoch just yet.
                    if participant_is_pinned && participant_epoch != epoch {
                        return epoch;
                    }
                }
            }
        }
        ::std::sync::atomic::fence(Ordering::Acquire);

        // All pinned participants were pinned in the current global epoch.  Try advancing the
        // epoch. We increment by 2 and simply wrap around on overflow.
        let epoch_new = epoch.wrapping_add(2);
        self.epoch.store(epoch_new, Ordering::Release);
        epoch_new
    }
}

impl Deref for Epoch {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.epoch
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
    pub fn get(&self) -> (bool, usize) {
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


#[cfg(test)]
mod tests {}
