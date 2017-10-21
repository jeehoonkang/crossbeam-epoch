use std::ptr;
use garbage::Garbage;
use internal::{Global, Local};

pub struct Guard {
    /// A reference to the global data.
    pub(crate) global: *const Global,
    /// A reference to the thread-local bag.
    pub(crate) local: *const Local, // !Send + !Sync
}

impl Guard {
    unsafe fn defer_garbage(&self, mut garbage: Garbage) {
        self.global.as_ref().map(|global| {
            let bag = &mut *(*self.local).bag.get();
            while let Err(g) = bag.try_push(garbage) {
                global.push_bag(bag, self);
                garbage = g;
            }
        });
    }

    pub unsafe fn defer<R, F: FnOnce() -> R + Send>(&self, f: F) {
        self.defer_garbage(Garbage::new(|| drop(f())))
    }

    pub fn flush(&self) {
        unsafe {
            self.global.as_ref().map(|global| {
                let bag = &mut *(*self.local).bag.get();

                if !bag.is_empty() {
                    global.push_bag(bag, &self);
                }

                global.collect(&self);
            });
        }
    }
}

impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        if !self.local.is_null() {
            unsafe {
                let local = &*self.local;
                let depth = local.pin_depth.get();
                local.pin_depth.set(depth - 1);

                if depth == 1 {
                    local.local_epoch.set_unpinned();
                }
            }
        }
    }
}

#[inline]
pub unsafe fn unprotected() -> Guard {
    Guard {
        global: ptr::null_mut(),
        local: ptr::null_mut(),
    }
}
