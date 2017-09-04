use atomic::Ptr;
use garbage::Garbage;

/// A witness that the current mutator is pinned.
pub trait Scope<'scope>
where
    Self: Copy + Sized,
{
    /// Deferred disposal of garbage.
    unsafe fn defer_dispose(self, garbage: Garbage);

    /// Flushes all garbage in the thread-local storage into the global garbage
    /// queue. `&EpochScope` also attempts to advance the epoch, and collects some garbage.
    fn flush(self);

    /// Deferred deallocation of heap-allocated object `ptr`.
    ///
    /// If the object is unusually large, it is wise to follow up with a call to [`flush`] so that
    /// it doesn't get stuck waiting in the local bag for a long time.
    ///
    /// [`flush`]: fn.flush.html
    unsafe fn defer_free<T>(self, ptr: Ptr<'scope, T>) {
        self.defer_dispose(Garbage::new_free(ptr.as_raw() as *mut T, 1))
    }

    /// Deferred destruction and deallocation of heap-allocated object `ptr`.
    // FIXME(jeehoonkang): `T: 'static` may be too restrictive.
    unsafe fn defer_drop<T: Send + 'static>(self, ptr: Ptr<'scope, T>) {
        self.defer_dispose(Garbage::new_drop(ptr.as_raw() as *mut T, 1))
    }

    /// Deferred execution of an arbitrary function `f`.
    unsafe fn defer<F: FnOnce() + Send + 'static>(self, f: F) {
        self.defer_dispose(Garbage::new(f))
    }
}
