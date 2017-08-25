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


impl<T> Default for Queue<T> {
    fn default() -> Queue<T> {
        Self::new()
    }
}

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
    /// Atomically place `new_head` into the `next` pointer of `self.tail`, and try to assign
    /// `new_tail` to `self.tail`.
    fn push_internal(&self, new_head: Ptr<Node<T>>, new_tail: Ptr<Node<T>>, scope: &Scope) {
        loop {
            // We push onto the tail, so we'll start optimistically by looking there first.
            let tail = self.tail.load(Acquire, scope);

            // is `tail` the actual tail?
            let t = unsafe { tail.deref() };
            let next = t.next.load(Acquire, scope);
            if unsafe { next.as_ref().is_some() } {
                // if not, try to "help" by moving the tail pointer forward, and retry.
                let _ = self.tail.compare_and_set(tail, next, Release, scope);
            } else {
                // looks like the actual tail; attempt to link in `n`
                if t.next.compare_and_set(Ptr::null(), new_head, Release, scope).is_ok() {
                    // `new` is successfully pushed. Try to move the tail pointer forward.
                    let _ = self.tail.compare_and_set(tail, new_tail, Release, scope);
                    break;
                }
            }
        }
    }

    /// Add `t` to the back of the queue, possibly waking up threads
    /// blocked on `pop`.
    pub fn push(&self, t: T, scope: &Scope) {
        let new = Owned::new(Node {
            data: t,
            next: Atomic::null(),
        });
        let new = Owned::into_ptr(new, scope);
        self.push_internal(new, new, scope);
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
            let head = self.head.load(Acquire, scope);
            let h = unsafe { head.deref() };
            let next = h.next.load(Acquire, scope);
            match unsafe { next.as_ref() } {
                None => {
                    return None;
                },
                Some(n) => unsafe {
                    if self.head
                        .compare_and_set(head, next, Release, scope)
                        .is_ok()
                    {
                        scope.defer_free(head);
                        return Some(ptr::read(&n.data));
                    }

                    // We lost the race. Try again.
                },
            }
        }
    }

    pub fn swap(&self, new: &mut Self, scope: &Scope) {
        let head = new.head.load(Relaxed, scope);
        let tail = new.tail.load(Relaxed, scope);
        ::std::sync::atomic::fence(Release);

        let head = self.head.swap(head, Relaxed, scope);
        let tail = self.tail.swap(tail, Relaxed, scope);
        ::std::sync::atomic::fence(Acquire);
        
        new.head.store(head, Relaxed);
        new.tail.store(tail, Relaxed);
    }

    pub fn append(&self, other: &Self, scope: &Scope) {
        // replace `other` with a new queue.
        let mut new = Self::new();
        other.swap(&mut new, scope);

        // get the `head` and `tail` of `other` at the beginning.
        let head = new.head.load(Relaxed, scope);
        let tail = new.tail.load(Relaxed, scope);

        // push `head` at the tail of the queue.
        self.push_internal(head, tail, scope);

        // Forget `new`, since the ownership of `new.head` and `new.tail` is transferred to `self.
        mem::forget(new);
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
