//! The default collector for garbage collection.
//!
//! For each thread, a mutator is lazily initialized on its first use, when the current thread is
//! registered in the default collector.  If initialized, the thread's mutator will get destructed
//! on thread exit, which in turn unregisters the thread.

use collector::Collector;
use mutator::{Mutator, Inner, Scope};

lazy_static! {
    /// The default global data.
    // FIXME(jeehoonkang): accessing globals in `lazy_static!` is blocking.
    static ref COLLECTOR: Collector = Collector::new();
}

thread_local! {
    /// The thread-local mutator for the default global data.
    static MUTATOR: MutatorImpl<'static> = {
        let collector = &COLLECTOR;
        MutatorImpl::new(collector, collector.add_mutator())
    };
}

struct MutatorImpl<'scope> {
    /// A reference to the global data.
    collector: &'scope Collector,
    /// FIXME
    inner: Inner<'scope>,
}

impl<'scope> MutatorImpl<'scope> {
    fn new(collector: &'scope Collector, inner: Inner<'scope>) -> Self {
        Self { collector, inner }
    }
}

impl<'scope> Mutator<'scope> for MutatorImpl<'scope> {
    #[inline]
    fn as_collector(&'scope self) -> &'scope Collector {
        self.collector
    }

    #[inline]
    fn as_inner(&'scope self) -> &'scope Inner<'scope> {
        &self.inner
    }
}

impl<'scope> Drop for MutatorImpl<'scope> {
    fn drop(&mut self) {
        self.finalize()
    }
}

/// Pin the current thread.
pub fn pin<F, R>(f: F) -> R
where
    F: FnOnce(&Scope) -> R,
{
    // FIXME(jeehoonkang): thread-local storage may be destructed at the time `pin()` is called. For
    // that case, we should use `MUTATOR.try_with()` instead.
    MUTATOR.with(|mutator| mutator.pin(f))
}

/// Check if the current thread is pinned.
pub fn is_pinned() -> bool {
    // FIXME(jeehoonkang): thread-local storage may be destructed at the time `pin()` is called. For
    // that case, we should use `MUTATOR.try_with()` instead.
    MUTATOR.with(|mutator| mutator.is_pinned())
}
