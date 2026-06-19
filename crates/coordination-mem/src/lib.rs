//! In-memory [`Coordination`]: the single-process backend for dev and the
//! single-binary profile (ADR-0006). etcd is the production backend behind the
//! same trait; openraft is reserved as a future embedded one — choosing between
//! them is composition in `server` (ADR-0010), not a refactor here.
//!
//! In one process there is nothing to coordinate *between*, so the semantics are
//! deliberately trivial (architecture §5, L5): discovery just remembers what was
//! registered, leadership is always granted (a lone process is always the
//! leader), and a lock is granted immediately. What is real from day one is the
//! **trait shape** — leased registration, fencing tokens — so etcd drops in
//! later without touching any caller.
//!
//! Everything is a deterministic counter (no clock, no randomness), so a run
//! under the DST harness (ADR-0009) is reproducible.
//!
//! ## Deferred until the trait grows (and two backends pin the semantics)
//!
//! - **Lease expiry / renewal.** A registration lives for the process lifetime;
//!   there is no `renew`/`revoke` on the trait yet, and a single process has no
//!   crashed peer whose lease should lapse.
//! - **Lock contention.** [`LockGuard`] is `Copy` with no release hook, so a
//!   lock cannot block or be held — it is granted with a rising fencing token.
//!   Mutual exclusion arrives when the trait gains a releasable guard.
//! - **Config mutation and watch.** Config is seeded at construction and read
//!   back; change notification is a later refinement on this seam.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_traits::{Coordination, Leadership, Lease, LockGuard, Result};

/// A [`Coordination`] backed entirely by in-process state.
pub struct MemCoordination {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Leased registrations, keyed by discovery key, in registration order.
    registrations: HashMap<String, Vec<(u64, Bytes)>>,
    /// Zone-wide config, seeded at construction.
    config: HashMap<String, Bytes>,
    /// Source of lease ids; monotonic, starts at 1 (0 is reserved as "none").
    next_lease: u64,
    /// Source of fencing tokens, shared by leadership and locks so every grant
    /// is strictly greater than every grant before it (the fencing property).
    next_token: u64,
}

impl MemCoordination {
    /// Create an empty coordinator (no config).
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Create a coordinator with zone-wide config seeded from `config`. There is
    /// no setter on the trait yet, so config is provided here.
    pub fn with_config(config: impl IntoIterator<Item = (String, Bytes)>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                config: config.into_iter().collect(),
                ..Inner::default()
            }),
        }
    }

    /// Take the state lock, mapping a poisoned mutex to the trait's boxed error
    /// rather than panicking. Named to avoid clashing with the trait's `lock`.
    fn guard(&self) -> Result<std::sync::MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| "coordination state poisoned".into())
    }
}

impl Default for MemCoordination {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Coordination for MemCoordination {
    async fn register(&self, key: &str, value: Bytes) -> Result<Lease> {
        let mut inner = self.guard()?;
        inner.next_lease += 1;
        let id = inner.next_lease;
        inner
            .registrations
            .entry(key.to_owned())
            .or_default()
            .push((id, value));
        Ok(Lease { id })
    }

    async fn discover(&self, key: &str) -> Result<Vec<Bytes>> {
        let inner = self.guard()?;
        Ok(inner
            .registrations
            .get(key)
            .map(|members| members.iter().map(|(_, value)| value.clone()).collect())
            .unwrap_or_default())
    }

    async fn elect_leader(&self, _key: &str) -> Result<Leadership> {
        // A lone process is always the leader; the token still rises so a later
        // term fences an earlier one.
        let mut inner = self.guard()?;
        inner.next_token += 1;
        Ok(Leadership {
            token: inner.next_token,
        })
    }

    async fn lock(&self, _key: &str) -> Result<LockGuard> {
        // No contention in one process: grant immediately with a rising token.
        let mut inner = self.guard()?;
        inner.next_token += 1;
        Ok(LockGuard {
            token: inner.next_token,
        })
    }

    async fn get_config(&self, key: &str) -> Result<Option<Bytes>> {
        let inner = self.guard()?;
        Ok(inner.config.get(key).cloned())
    }
}
