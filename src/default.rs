//! The global garbage collection realm
//!
//! This is the default realm for garbage collection.  For each thread, a mutator is lazily
//! initialized on its first use, thus registering the current thread.  If initialized, the
//! thread's mutator will get destructed on thread exit, which in turn unregisters the thread.
//!
//! `registries` is the list is the registered mutators, and `epoch` is the global epoch.

use garbage::Bag;
use epoch::Epoch;
use realm::Realm;
use mutator::{Mutator, LocalEpoch, EpochScope};
use sync::list::List;
use sync::queue::Queue;


// FIXME(jeehoonkang): accessing globals in `lazy_static!` is blocking.
lazy_static! {
    /// The default instance of registries.
    pub static ref REGISTRIES: List<LocalEpoch> = List::new();
    /// The default instance of the global queue of garbages.
    pub static ref GARBAGES: Queue<(usize, Bag)> = Queue::new();
    /// The default instance of the global epoch.
    pub static ref EPOCH: Epoch = Epoch::new();
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultRealm {}

const DEFAULT_REALM: DefaultRealm = DefaultRealm {};

impl Realm<'static> for DefaultRealm {
    fn registries(self) -> &'static List<LocalEpoch> {
        &REGISTRIES
    }

    fn garbages(self) -> &'static Queue<(usize, Bag)> {
        &GARBAGES
    }

    fn epoch(self) -> &'static Epoch {
        &EPOCH
    }
}

thread_local! {
    /// The per-thread mutator.
    static MUTATOR: Mutator<'static, DefaultRealm> = Mutator::new(DEFAULT_REALM);
}

/// Pin the current thread in the default realm.
pub fn pin<F, R>(f: F) -> R
where
    F: FnOnce(&EpochScope<'static, DefaultRealm>) -> R,
{
    MUTATOR.with(|mutator| mutator.pin(f))
}

/// Check if the current thread is pinned in the default realm.
pub fn is_pinned() -> bool {
    MUTATOR.with(|mutator| mutator.is_pinned())
}


#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::atomic::Ordering::Relaxed;

    use super::*;

    #[test]
    fn pin_reentrant() {
        assert!(!is_pinned());
        pin(|_| {
            pin(|_| {
                assert!(is_pinned());
            });
            assert!(is_pinned());
        });
        assert!(!is_pinned());
    }

    #[test]
    fn pin_holds_advance() {
        let threads = (0..8)
            .map(|_| {
                thread::spawn(|| for _ in 0..500_000 {
                    pin(|scope| {
                        let before = EPOCH.load(Relaxed);
                        EPOCH.try_advance(&REGISTRIES, scope);
                        let after = EPOCH.load(Relaxed);

                        assert!(after.wrapping_sub(before) <= 2);
                    });
                })
            })
            .collect::<Vec<_>>();

        for t in threads {
            t.join().unwrap();
        }
    }
}
