//! Epoch-based memory reclamation.
//!
//! # Pointers
//!
//! Concurrent collections are built using atomic pointers. This module provides [`Atomic`], which
//! is just a shared atomic pointer to a heap-allocated object. Loading an [`Atomic`] yields a
//! [`Ptr`], which is an epoch-protected pointer through which the loaded object can be safely read.
//!
//! # Pinning
//!
//! Before an [`Atomic`] can be loaded, a participant must be [`pin`]ned. By pinning a participant
//! we declare that any object that gets removed from now on must not be destructed just
//! yet. Garbage collection of newly removed objects is suspended until the participant gets
//! unpinned.
//!
//! # Garbage
//!
//! Objects that get removed from concurrent collections must be stashed away until all currently
//! pinned participants get unpinned. Such objects can be stored into a [`Garbage`], where they are
//! kept until the right time for their destruction comes.
//!
//! There is a global shared instance of garbage queue, which can deallocate ([`defer_free`]) or
//! drop ([`defer_drop`]) objects, or even run arbitrary destruction procedures ([`defer`]).
//!
//! # APIs
//!
//! For majority of use cases, just use the default garbage collector by invoking [`pin`]. If you
//! want to create your own garbage collector, use the [`Collector`] API. When you want to embed a
//! garbage collector in another systems library, e.g. memory allocator or thread manager, use the
//! internal API (see [`Global`] and [`Participant`]).
//!
//! [`Atomic`]: struct.Atomic.html
//! [`Collector`]: struct.Collector.html
//! [`Global`]: struct.Global.html
//! [`Participant`]: struct.Participant.html
//! [`Ptr`]: struct.Ptr.html
//! [`pin`]: fn.pin.html
//! [`defer_free`]: fn.defer_free.html
//! [`defer_drop`]: fn.defer_drop.html
//! [`defer`]: fn.defer.html

#![cfg_attr(feature = "nightly", feature(const_fn))]

#[macro_use(defer)]
extern crate scopeguard;
#[macro_use]
extern crate lazy_static;
extern crate arrayvec;
extern crate crossbeam_utils;

mod atomic;
mod deferred;
mod epoch;
mod garbage;
mod internal;
mod collector;
mod scope;
mod default;
mod sync;

pub use self::atomic::{Atomic, CompareAndSetOrdering, Owned, Ptr};
pub use self::scope::{Scope, unprotected};
pub use self::internal::{Global, Participant};
pub use self::default::{pin, is_pinned};
pub use self::collector::{Collector, Handle};
