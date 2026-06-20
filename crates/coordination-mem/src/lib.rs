//! In-memory [`Coordination`]: the single-process backend for dev and the
//! single-binary profile (ADR-0006). etcd is the production backend behind the
//! same trait; openraft is reserved as a future embedded one — choosing between
//! them is composition in `server` (ADR-0010), not a refactor here.
//!
//! The semantics are real where a single process can exercise them: leased
//! registrations **expire** against an injected [`Clock`] and can be renewed or
//! revoked; locks provide genuine **mutual exclusion** (try-acquire — a held key
//! refuses contenders) and are released through the trait; config is mutable and
//! carries a monotonic **revision**. Leadership is always granted (a lone process
//! is always the leader), with a rising fencing token. Time is the only input;
//! drive it with a [`ManualClock`](wyrd_testkit::ManualClock) for a reproducible
//! run under the DST harness (ADR-0009).
//!
//! ## Deferred until a second backend (etcd) pins the semantics
//!
//! - **Blocking lock acquisition.** [`lock`](Coordination::lock) is non-blocking
//!   (returns `None` when held); awaiting until a lock frees needs an async
//!   notification primitive and a networked backend to validate against.
//! - **Push config watch.** Change notification here is the pollable
//!   [`config_revision`](Coordination::config_revision); a real stream is an etcd
//!   refinement (the trait already shapes it as revision-based).

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_testkit::{Clock, SystemClock};
use wyrd_traits::{Coordination, FencingToken, Leadership, Lease, LockGuard, Result};

/// A [`Coordination`] backed entirely by in-process state, generic over the
/// [`Clock`] that drives lease expiry (a real [`SystemClock`] by default; a
/// manual clock in tests).
pub struct MemCoordination<C: Clock = SystemClock> {
    inner: Mutex<Inner>,
    clock: C,
}

#[derive(Default)]
struct Inner {
    /// Leased registrations, keyed by discovery key, in registration order.
    registrations: HashMap<String, Vec<Registration>>,
    /// Reverse index from lease id to its registration, so a lease can be
    /// renewed or revoked without knowing its key.
    leases: HashMap<u64, LeaseInfo>,
    /// Keys currently locked, to the fencing token of the holder.
    held_locks: HashMap<String, FencingToken>,
    /// Zone-wide config.
    config: HashMap<String, Bytes>,
    /// Bumped on every config write so a watcher can detect changes.
    config_revision: u64,
    /// Source of lease ids; monotonic, starts at 1 (0 is reserved as "none").
    next_lease: u64,
    /// Source of fencing tokens, shared by leadership and locks so every grant
    /// is strictly greater than every grant before it (the fencing property).
    next_token: FencingToken,
}

/// A single registration's mutable expiry, kept in `registrations`.
struct Registration {
    lease_id: u64,
    value: Bytes,
    expiry_millis: u64,
}

/// What `leases` remembers so a lease can be located and re-stamped.
struct LeaseInfo {
    key: String,
    ttl_millis: u64,
}

impl MemCoordination<SystemClock> {
    /// Create an empty coordinator driven by the real wall clock.
    pub fn new() -> Self {
        Self::with_clock(SystemClock)
    }
}

impl<C: Clock> MemCoordination<C> {
    /// Create an empty coordinator driven by `clock` — inject a manual clock to
    /// exercise lease expiry deterministically.
    pub fn with_clock(clock: C) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            clock,
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

impl Default for MemCoordination<SystemClock> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<C: Clock + Send + Sync> Coordination for MemCoordination<C> {
    async fn register(&self, key: &str, value: Bytes, ttl: Duration) -> Result<Lease> {
        let now = self.clock.now_millis();
        let ttl_millis = ttl.as_millis() as u64;
        let mut inner = self.guard()?;

        inner.next_lease += 1;
        let lease_id = inner.next_lease;
        inner.leases.insert(
            lease_id,
            LeaseInfo {
                key: key.to_owned(),
                ttl_millis,
            },
        );
        inner
            .registrations
            .entry(key.to_owned())
            .or_default()
            .push(Registration {
                lease_id,
                value,
                expiry_millis: now + ttl_millis,
            });
        Ok(Lease { id: lease_id })
    }

    async fn renew(&self, lease: Lease) -> Result<()> {
        let now = self.clock.now_millis();
        let mut inner = self.guard()?;

        let Some(info) = inner.leases.get(&lease.id) else {
            return Err("renew: unknown or expired lease".into());
        };
        let (key, ttl_millis) = (info.key.clone(), info.ttl_millis);
        let registration = inner
            .registrations
            .get_mut(&key)
            .and_then(|regs| regs.iter_mut().find(|r| r.lease_id == lease.id));
        match registration {
            Some(r) if r.expiry_millis > now => {
                r.expiry_millis = now + ttl_millis;
                Ok(())
            }
            // Expired (or already swept): treat as gone.
            _ => Err("renew: unknown or expired lease".into()),
        }
    }

    async fn revoke(&self, lease: Lease) -> Result<()> {
        let mut inner = self.guard()?;
        if let Some(info) = inner.leases.remove(&lease.id) {
            if let Some(regs) = inner.registrations.get_mut(&info.key) {
                regs.retain(|r| r.lease_id != lease.id);
            }
        }
        Ok(())
    }

    async fn discover(&self, key: &str) -> Result<Vec<Bytes>> {
        let now = self.clock.now_millis();
        let inner = self.guard()?;
        Ok(inner
            .registrations
            .get(key)
            .map(|members| {
                members
                    .iter()
                    .filter(|r| r.expiry_millis > now)
                    .map(|r| r.value.clone())
                    .collect()
            })
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

    async fn lock(&self, key: &str) -> Result<Option<LockGuard>> {
        // Genuine mutual exclusion: a held key refuses contenders (try-acquire).
        let mut inner = self.guard()?;
        if inner.held_locks.contains_key(key) {
            return Ok(None);
        }
        inner.next_token += 1;
        let token = inner.next_token;
        inner.held_locks.insert(key.to_owned(), token);
        Ok(Some(LockGuard { token }))
    }

    async fn unlock(&self, guard: LockGuard) -> Result<()> {
        // Release by fencing token; idempotent if already released.
        let mut inner = self.guard()?;
        inner
            .held_locks
            .retain(|_, &mut token| token != guard.token);
        Ok(())
    }

    async fn set_config(&self, key: &str, value: Bytes) -> Result<()> {
        let mut inner = self.guard()?;
        inner.config.insert(key.to_owned(), value);
        inner.config_revision += 1;
        Ok(())
    }

    async fn get_config(&self, key: &str) -> Result<Option<Bytes>> {
        let inner = self.guard()?;
        Ok(inner.config.get(key).cloned())
    }

    async fn config_revision(&self) -> Result<u64> {
        Ok(self.guard()?.config_revision)
    }
}
