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
use std::sync::atomic::Ordering::{Relaxed, Acquire, AcqRel, Release, SeqCst};

use guard::Guard;
use internal::Global;
use crossbeam_utils::cache_padded::CachePadded;

/// The global epoch is a (cache-padded) integer.
#[derive(Default, Debug)]
pub struct Epoch {
    epoch: CachePadded<AtomicUsize>,
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
    pub fn try_advance(&self, global: &Global, guard: &Guard) -> usize {
        let epoch = self.epoch.load(Relaxed);
        ::std::sync::atomic::fence(SeqCst);

        let mut pred = &global.registries;
        let mut curr = pred.load(Acquire, guard);

        while let Some(c) = unsafe { curr.as_ref() } {
            let succ = c.next.load(Acquire, guard);

            if succ.tag() == 1 && !c.local.local_epoch.get_state().0 {
                // This thread has exited. Try unlinking it from the list.
                let succ = succ.with_tag(0);

                if pred.compare_and_set(curr, succ, AcqRel, guard).is_err() {
                    // We lost the race to unlink the thread. Usually that means we should traverse the
                    // list again from the beginning, but since another thread trying to advance the
                    // epoch has won the race, we leave the job to that one.
                    return epoch;
                }

                // The unlinked entry can later be freed.
                unsafe {
                    global.push_bag(&mut *c.local.bag.get(), guard);
                    global.collect(guard);
                    guard.defer(move || curr.into_owned())
                }

                // Move forward, but don't change the predecessor.
                curr = succ;
            } else {
                let (participant_is_pinned, participant_epoch) = c.local.local_epoch.get_state();

                // If the participant was pinned in a different epoch, we cannot advance the global
                // epoch just yet.
                if participant_is_pinned && participant_epoch != epoch {
                    return epoch;
                }

                // Move one step forward.
                pred = &c.next;
                curr = succ;
            }
        }

        ::std::sync::atomic::fence(Acquire);

        // All pinned participants were pinned in the current global epoch.  Try advancing the
        // epoch. We increment by 2 and simply wrap around on overflow.
        let epoch_new = epoch.wrapping_add(2);
        self.epoch.store(epoch_new, Release);
        epoch_new
    }
}

impl Deref for Epoch {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.epoch
    }
}


#[cfg(test)]
mod tests {}
