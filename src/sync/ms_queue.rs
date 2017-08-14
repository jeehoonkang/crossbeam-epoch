//! Michael-Scott lock-free queue.
//!
//! Usable with any number of producers and consumers.
//!
//! Michael and Scott.  Simple, Fast, and Practical Non-Blocking and Blocking Concurrent Queue
//! Algorithms.  PODC 1996.  http://dl.acm.org/citation.cfm?id=248106

use std::{mem, ptr};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};

use {Atomic, Owned, Ptr, Scope, pin, unprotected};
use util::cache_padded::CachePadded;

// The representation here is a singly-linked list, with a sentinel node at the front. In general
// the `tail` pointer may lag behind the actual tail. Non-sentinel nodes are either all `Data` or
// all `Blocked` (requests for data from blocked threads).
#[derive(Debug)]
pub struct Queue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Atomic<Node<T>>,
}

// Any particular `T` should never be accessed concurrently, so no need for Sync.
unsafe impl<T: Send> Sync for Queue<T> {}
unsafe impl<T: Send> Send for Queue<T> {}


impl<T> Queue<T> {
    /// Create a new, empty queue.
    pub fn new() -> Queue<T> {
        let q = Queue {
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        let sentinel = Owned::new(Node {
            data: unsafe { mem::uninitialized() },
            next: Atomic::null(),
        });
        unsafe {
            unprotected(|scope| {
                let sentinel = sentinel.into_ptr(scope);
                q.head.store(sentinel, Relaxed);
                q.tail.store(sentinel, Relaxed);
                q
            })
        }
    }

    #[inline(always)]
    /// Attempt to atomically place `n` into the `next` pointer of `onto`.
    ///
    /// If unsuccessful, returns ownership of `n`, possibly updating
    /// the queue's `tail` pointer.
    fn push_internal(
        &self,
        onto: Ptr<Node<T>>,
        new: Owned<Node<T>>,
        scope: &Scope,
    ) -> Result<(), Owned<Node<T>>> {
        // is `onto` the actual tail?
        let o = unsafe { onto.deref() };
        let next = o.next.load(Acquire, scope);
        if unsafe { next.as_ref().is_some() } {
            // if not, try to "help" by moving the tail pointer forward
            let _ = self.tail.compare_and_set(onto, next, Release, scope);
            Err(new)
        } else {
            // looks like the actual tail; attempt to link in `n`
            o.next
                .compare_and_set_owned(Ptr::null(), new, Release, scope)
                .map(|new| {
                    // try to move the tail pointer forward
                    let _ = self.tail.compare_and_set(onto, new, Release, scope);
                })
                .map_err(|(_, new)| new)
        }
    }

    /// Add `t` to the back of the queue, possibly waking up threads
    /// blocked on `pop`.
    pub fn push(&self, t: T, scope: &Scope) {
        let mut new = Owned::new(Node {
            data: t,
            next: Atomic::null(),
        });

        loop {
            // We push onto the tail, so we'll start optimistically by looking
            // there first.
            let tail = self.tail.load(Acquire, scope);

            // Attempt to push onto the `tail` snapshot; fails if
            // `tail.next` has changed, which will always be the case if the
            // queue has transitioned to blocking mode.
            match self.push_internal(tail, new, scope) {
                Ok(_) => break,
                Err(temp) => {
                    // retry
                    new = temp
                }
            }
        }
    }

    #[inline(always)]
    // Attempt to pop a data node. `Ok(None)` if queue is empty or in blocking
    // mode; `Err(())` if lost race to pop.
    fn pop_internal(&self, scope: &Scope) -> Result<Option<T>, ()> {
        let head = self.head.load(Acquire, scope);
        let h = unsafe { head.deref() };
        let next = h.next.load(Acquire, scope);
        match unsafe { next.as_ref() } {
            None => Ok(None),
            Some(n) => unsafe {
                if self.head
                    .compare_and_set(head, next, Release, scope)
                    .is_ok()
                {
                    scope.defer_free(head);
                    Ok(Some(ptr::read(&n.data)))
                } else {
                    Err(())
                }
            },
        }
    }

    /// Check if this queue is empty.
    pub fn is_empty(&self) -> bool {
        pin(|scope| {
            let head = self.head.load(Acquire, scope);
            let h = unsafe { head.deref() };
            h.next.load(Acquire, scope).is_null()
        })
    }

    /// Attempt to dequeue from the front.
    ///
    /// Returns `None` if the queue is observed to be empty.
    pub fn try_pop(&self, scope: &Scope) -> Option<T> {
        loop {
            if let Ok(r) = self.pop_internal(scope) {
                return r;
            }
        }
    }

    pub fn try_pop_if<F>(&self, condition: F, scope: &Scope) -> Option<T>
    where
        F: Fn(&T) -> bool,
    {
        loop {
            if let Ok(head) = self.pop_internal(scope) {
                match head {
                    None => return None,
                    Some(h) => {
                        if condition(&h) {
                            return Some(h);
                        } else {
                            mem::forget(h);
                            return None;
                        }
                    }
                }
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            unprotected(|scope| {
                while let Some(_) = self.try_pop(scope) {}

                // Destroy the remaining sentinel node.
                let sentinel = self.head.load(Relaxed, scope).as_raw() as *mut Node<T>;
                drop(Vec::from_raw_parts(sentinel, 0, 1));
            })
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use util::scoped;

    struct Queue<T> {
        queue: super::Queue<T>,
    }

    impl<T> Queue<T> {
        pub fn new() -> Queue<T> {
            Queue { queue: super::Queue::new() }
        }

        pub fn push(&self, t: T) {
            pin(|scope| self.queue.push(t, scope))
        }

        pub fn is_empty(&self) -> bool {
            self.queue.is_empty()
        }

        pub fn try_pop(&self) -> Option<T> {
            pin(|scope| self.queue.try_pop(scope))
        }

        pub fn pop(&self) -> T {
            loop {
                match self.try_pop() {
                    None => continue,
                    Some(t) => return t,
                }
            }
        }
    }

    const CONC_COUNT: i64 = 1000000;

    #[test]
    fn push_try_pop_1() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        q.push(37);
        assert!(!q.is_empty());
        assert_eq!(q.try_pop(), Some(37));
        assert!(q.is_empty());
    }

    #[test]
    fn push_try_pop_2() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        q.push(37);
        q.push(48);
        assert_eq!(q.try_pop(), Some(37));
        assert!(!q.is_empty());
        assert_eq!(q.try_pop(), Some(48));
        assert!(q.is_empty());
    }

    #[test]
    fn push_try_pop_many_seq() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        for i in 0..200 {
            q.push(i)
        }
        assert!(!q.is_empty());
        for i in 0..200 {
            assert_eq!(q.try_pop(), Some(i));
        }
        assert!(q.is_empty());
    }

    #[test]
    fn push_pop_1() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        q.push(37);
        assert!(!q.is_empty());
        assert_eq!(q.pop(), 37);
        assert!(q.is_empty());
    }

    #[test]
    fn push_pop_2() {
        let q: Queue<i64> = Queue::new();
        q.push(37);
        q.push(48);
        assert_eq!(q.pop(), 37);
        assert_eq!(q.pop(), 48);
    }

    #[test]
    fn push_pop_many_seq() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        for i in 0..200 {
            q.push(i)
        }
        assert!(!q.is_empty());
        for i in 0..200 {
            assert_eq!(q.pop(), i);
        }
        assert!(q.is_empty());
    }

    #[test]
    fn push_try_pop_many_spsc() {
        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());

        scoped::scope(|scope| {
            scope.spawn(|| {
                let mut next = 0;

                while next < CONC_COUNT {
                    if let Some(elem) = q.try_pop() {
                        assert_eq!(elem, next);
                        next += 1;
                    }
                }
            });

            for i in 0..CONC_COUNT {
                q.push(i)
            }
        });
    }

    #[test]
    fn push_try_pop_many_spmc() {
        fn recv(_t: i32, q: &Queue<i64>) {
            let mut cur = -1;
            for _i in 0..CONC_COUNT {
                if let Some(elem) = q.try_pop() {
                    assert!(elem > cur);
                    cur = elem;

                    if cur == CONC_COUNT - 1 {
                        break;
                    }
                }
            }
        }

        let q: Queue<i64> = Queue::new();
        assert!(q.is_empty());
        let qr = &q;
        scoped::scope(|scope| {
            for i in 0..3 {
                scope.spawn(move || recv(i, qr));
            }

            scope.spawn(|| for i in 0..CONC_COUNT {
                q.push(i);
            })
        });
    }

    #[test]
    fn push_try_pop_many_mpmc() {
        enum LR {
            Left(i64),
            Right(i64),
        }

        let q: Queue<LR> = Queue::new();
        assert!(q.is_empty());

        scoped::scope(|scope| for _t in 0..2 {
            scope.spawn(|| for i in CONC_COUNT - 1..CONC_COUNT {
                q.push(LR::Left(i))
            });
            scope.spawn(|| for i in CONC_COUNT - 1..CONC_COUNT {
                q.push(LR::Right(i))
            });
            scope.spawn(|| {
                let mut vl = vec![];
                let mut vr = vec![];
                for _i in 0..CONC_COUNT {
                    match q.try_pop() {
                        Some(LR::Left(x)) => vl.push(x),
                        Some(LR::Right(x)) => vr.push(x),
                        _ => {}
                    }
                }

                let mut vl2 = vl.clone();
                let mut vr2 = vr.clone();
                vl2.sort();
                vr2.sort();

                assert_eq!(vl, vl2);
                assert_eq!(vr, vr2);
            });
        });
    }

    #[test]
    fn push_pop_many_spsc() {
        let q: Queue<i64> = Queue::new();

        scoped::scope(|scope| {
            scope.spawn(|| {
                let mut next = 0;
                while next < CONC_COUNT {
                    assert_eq!(q.pop(), next);
                    next += 1;
                }
            });

            for i in 0..CONC_COUNT {
                q.push(i)
            }
        });
        assert!(q.is_empty());
    }

    #[test]
    fn is_empty_dont_pop() {
        let q: Queue<i64> = Queue::new();
        q.push(20);
        q.push(20);
        assert!(!q.is_empty());
        assert!(!q.is_empty());
        assert!(q.try_pop().is_some());
    }
}
