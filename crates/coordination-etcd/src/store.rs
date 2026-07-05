//! The etcd-backed [`Coordination`] store.
//!
//! Compiled from ONE source two ways (see the crate docs): `--features etcd`
//! against the real `etcd-client`, and `--cfg madsim` against the
//! `madsim-etcd-client` simulator. The `etcd_client` / `tokio` paths resolve to
//! whichever the build selected, so the logic below is identical on both.
//!
//! ## Correctness of the holds (the crate's reason to exist)
//!
//! Leadership and locks are backed by an etcd **lease** kept alive by a background
//! task ([`spawn_keepalive`]). The task's handle lives in a [`KeepAlive`] guard:
//!
//! - **No orphaned campaign.** `elect_leader` spawns the keep-alive BEFORE the
//!   campaign (so a long wait for a busy leader does not let our candidacy lapse),
//!   but the guard lives on the campaign future's stack until we win. If the
//!   campaign future is cancelled, the guard drops, the task is signalled, and the
//!   granted lease + any candidate key it created are **revoked at once** — a
//!   cancelled campaign never leaves a detached task renewing a lease forever.
//! - **Prompt release on drop.** Dropping a hold (clean shutdown, or `unlock`)
//!   revokes its lease, so leadership/locks are released immediately rather than
//!   lingering until TTL expiry — and even a lost revoke self-heals within
//!   `HOLD_TTL_SECS` (etcd-idiomatic crash failover).
//! - **Conditional release.** `unlock` never deletes a lock key *by key*; it
//!   revokes only OUR lease, which atomically removes the key bound to that lease
//!   and nothing else — so it can never release a newer holder's reacquired lock.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use etcd_client::{
    Client, Compare, CompareOp, GetOptions, LeaderKey, ProclaimOptions, PutOptions, Txn, TxnOp,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use wyrd_traits::{Coordination, FencingToken, Leadership, Lease, LockGuard, Result};

use crate::fencing::token_from_revision;
use crate::hold::keepalive_interval;
use crate::keyspace;

/// The fixed non-empty marker a held lock key carries. A key is "held" IFF its
/// value is exactly `LOCK_HELD` (it is written with this value and no other, and a
/// release revokes the lease, deleting the key). The lock acquires by testing
/// `Compare::value(key, Equal, LOCK_HELD)` in the txn's `when` and putting in its
/// `or_else`: held ⇒ compare true ⇒ no-op (refuse); NOT held (crucially, ABSENT)
/// ⇒ compare false ⇒ `or_else` put (acquire).
///
/// The guard tests *held*, never *absent*, on purpose: on REAL etcd a value
/// comparison against a **missing** key is defined to return `false` for every
/// operator ("no value to compare"), so an `absent`-phrased `NotEqual` guard would
/// NEVER fire and the lock could never be taken on a real cluster — a fidelity trap
/// the simulator hides, because it treats an absent key as `None != LOCK_HELD` ⇒
/// `true`. Phrasing the guard as `== LOCK_HELD` + `or_else` reads identically on
/// real etcd AND the simulator, keeping ONE code path (their `Txn` supports only
/// value comparisons, not create-revision compares).
const LOCK_HELD: &[u8] = b"1";

/// TTL (seconds) of the lease behind a leadership or lock hold. A background
/// keep-alive renews it for the life of the hold; on release the lease is revoked
/// so the hold lapses at once. Small, so a lost-revoke failover gap is short.
const HOLD_TTL_SECS: i64 = 6;

/// The leader value proclaimed for every term (leadership carries its fencing
/// token out-of-band, in the returned [`Leadership`], not in this value).
const LEADER_VALUE: &[u8] = b"leader";

/// A running lease keep-alive task, whose lifetime IS the hold's lifetime.
struct KeepAlive {
    /// Sending (or dropping) this asks the task to revoke its lease and exit — the
    /// prompt, best-effort release on cancellation / clean drop.
    stop_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    /// Set by the keep-alive task — and ONLY by it — the instant it observes the
    /// lease is genuinely gone (the server refused a renewal / the stream closed /
    /// a renewal reported TTL 0). This is the AUTHORITATIVE loss signal: the
    /// re-election path concludes "we no longer lead" solely from this flag, never
    /// by inferring loss from a proclaim RPC error (a transient blip must not churn
    /// a still-valid leadership — the earlier iterations' lease-leak / stall bug).
    lost: Arc<AtomicBool>,
}

impl KeepAlive {
    /// Stop the keep-alive WITHOUT the graceful revoke, because the caller
    /// (`unlock`, or a lapsed-leadership cleanup) revokes the lease synchronously
    /// and does not want a second racing revoke of a possibly-reused id.
    fn stop_without_revoke(mut self) {
        self.stop_tx = None; // defuse the drop-revoke
        self.handle.abort();
    }

    /// Has the keep-alive observed the lease genuinely lost? (Distinct from any
    /// RPC error the caller may hit — see the field docs.)
    fn is_lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst)
    }
}

impl Drop for KeepAlive {
    fn drop(&mut self) {
        // Cancellation or clean drop: signal the task to revoke its lease and
        // exit, releasing the hold promptly. If the task is already gone this is a
        // no-op and the lease self-expires within its TTL.
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Spawn a background task that renews `lease_id` for the life of the returned
/// guard, and revokes it when the guard is dropped (unless
/// [`KeepAlive::stop_without_revoke`] is used).
fn spawn_keepalive(client: &Client, lease_id: i64) -> KeepAlive {
    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
    let lost = Arc::new(AtomicBool::new(false));
    let lost_task = Arc::clone(&lost);
    let mut lease = client.lease_client();
    let period = keepalive_interval(HOLD_TTL_SECS);
    let handle = tokio::spawn(async move {
        let (mut keeper, mut stream) = match lease.keep_alive(lease_id).await {
            Ok(pair) => pair,
            // Could not even open the keep-alive: the lease never took / is already
            // gone — record the loss so a re-election re-campaigns rather than
            // proclaiming on a dead key.
            Err(_) => {
                lost_task.store(true, Ordering::SeqCst);
                return;
            }
        };
        loop {
            // Wait a renewal period, but wake early if asked to stop. `timeout`
            // resolving `Ok` means the stop signal fired (or its sender dropped):
            // revoke and exit. `Err` (elapsed) means it is time to renew.
            match tokio::time::timeout(period, &mut stop_rx).await {
                Ok(_) => {
                    let _ = lease.revoke(lease_id).await;
                    return;
                }
                Err(_) => {
                    if keeper.keep_alive().await.is_err() {
                        lost_task.store(true, Ordering::SeqCst);
                        return;
                    }
                    // A renewal that reports TTL 0 (or a closed stream) means the
                    // lease has lapsed server-side — record the loss authoritatively.
                    match stream.message().await {
                        Ok(Some(resp)) if resp.ttl() > 0 => {} // renewed; keep going
                        _ => {
                            lost_task.store(true, Ordering::SeqCst);
                            return;
                        }
                    }
                }
            }
        }
    });
    KeepAlive {
        stop_tx: Some(stop_tx),
        handle,
        lost,
    }
}

/// A held leadership term: the leader key we can re-proclaim on, and the
/// keep-alive task (which owns the lease and revokes it on release).
struct LeaderHold {
    leader_key: LeaderKey,
    keepalive: KeepAlive,
}

/// A held lock: the lease keeping it alive, and the keep-alive task. Keyed by
/// fencing token so `unlock(guard)` finds exactly this hold.
struct LockHold {
    lease_id: i64,
    keepalive: KeepAlive,
}

#[derive(Default)]
struct LocalState {
    /// Leadership held by this instance, by election key.
    leaders: HashMap<String, LeaderHold>,
    /// Locks held by this instance, by fencing token.
    locks: HashMap<FencingToken, LockHold>,
}

/// etcd-backed [`Coordination`]. Cheap to clone the underlying `Client`; each
/// operation borrows a fresh sub-client.
pub struct EtcdCoordination {
    client: Client,
    /// Key namespace prefix, so several coordinators share one cluster without
    /// colliding (the shared suite scopes a fresh namespace per clause).
    ns: String,
    state: Mutex<LocalState>,
}

impl EtcdCoordination {
    /// Connect to an etcd cluster at `endpoints` (e.g. `["http://127.0.0.1:2379"]`).
    pub async fn connect<E, S>(endpoints: S) -> Result<Self>
    where
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let client = Client::connect(endpoints, None).await?;
        Ok(Self {
            client,
            ns: String::new(),
            state: Mutex::new(LocalState::default()),
        })
    }

    /// Scope every key under `ns` (a trailing separator is recommended, e.g.
    /// `"wyrd/coord/gatewayA/"`), so this instance shares a cluster without
    /// colliding with others.
    pub fn with_namespace(mut self, ns: impl Into<String>) -> Self {
        self.ns = ns.into();
        self
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, LocalState>> {
        self.state
            .lock()
            .map_err(|_| "coordination-etcd state poisoned".into())
    }
}

#[async_trait]
impl Coordination for EtcdCoordination {
    async fn register(&self, key: &str, value: Bytes, ttl: Duration) -> Result<Lease> {
        let ttl_secs = (ttl.as_secs() as i64).max(1);
        let id = self.client.lease_client().grant(ttl_secs, None).await?.id();
        let member = keyspace::registration_member(&self.ns, key, id);
        self.client
            .kv_client()
            .put(
                member.into_bytes(),
                value.to_vec(),
                Some(PutOptions::new().with_lease(id)),
            )
            .await?;
        Ok(Lease { id: id as u64 })
    }

    async fn renew(&self, lease: Lease) -> Result<()> {
        let id = lease.id as i64;
        // Open a keep-alive and inspect the first response. On the simulator an
        // unknown lease errors here; on real etcd the stream opens and the first
        // response carries TTL 0 for a revoked lease. Both map to "cannot renew".
        let (mut keeper, mut stream) = self.client.lease_client().keep_alive(id).await?;
        keeper.keep_alive().await?;
        match stream.message().await? {
            Some(resp) if resp.ttl() > 0 => Ok(()),
            _ => Err("renew: unknown or expired lease".into()),
        }
    }

    async fn revoke(&self, lease: Lease) -> Result<()> {
        // Idempotent, like the in-memory backend: revoking an already-gone lease
        // is success (a no-op on the cluster).
        let _ = self.client.lease_client().revoke(lease.id as i64).await;
        Ok(())
    }

    async fn discover(&self, key: &str) -> Result<Vec<Bytes>> {
        let prefix = keyspace::registration_prefix(&self.ns, key);
        let resp = self
            .client
            .kv_client()
            .get(prefix.into_bytes(), Some(GetOptions::new().with_prefix()))
            .await?;
        Ok(resp
            .kvs()
            .iter()
            .map(|kv| Bytes::copy_from_slice(kv.value()))
            .collect())
    }

    async fn elect_leader(&self, key: &str) -> Result<Leadership> {
        // Already leading this key on this instance, and the keep-alive has NOT
        // observed the lease lost? Then we still hold the term: re-proclaim to open
        // a new fenced term (mem's "each term fences the last"), returning a higher
        // token — never blocking behind our own prior candidacy.
        //
        // Crucially, we decide "still leading" from the keep-alive's authoritative
        // `is_lost()` signal, NOT from whether the proclaim below succeeds: a
        // proclaim RPC error is treated as TRANSIENT and propagated to the caller,
        // leaving our hold (and its live lease) intact. Inferring loss from a
        // proclaim error is what made earlier iterations churn their own valid
        // leadership and leak a lease behind an orphaned re-campaign.
        let still_leading = self
            .state()?
            .leaders
            .get(key)
            .filter(|h| !h.keepalive.is_lost())
            .map(|h| h.leader_key.clone());
        if let Some(leader_key) = still_leading {
            let resp = self
                .client
                .election_client()
                .proclaim(
                    LEADER_VALUE.to_vec(),
                    Some(ProclaimOptions::new().with_leader(leader_key)),
                )
                .await?; // transient error: propagate, keep the hold
            let rev = resp.header().map(|h| h.revision()).unwrap_or(0);
            return Ok(Leadership {
                token: token_from_revision(rev),
            });
        }

        // Either we never led this key, or the keep-alive reported the lease lost:
        // drop any (now-dead) hold and campaign fresh, so a lapsed leader re-earns a
        // new fenced term instead of proclaiming on a key etcd has already deleted.
        if let Some(stale) = self.state()?.leaders.remove(key) {
            stale.keepalive.stop_without_revoke();
        }

        // Fresh campaign. Grant the lease and start renewing it BEFORE we campaign
        // (a busy leader can make us wait past a TTL). The guard lives on this
        // future's stack: a cancelled campaign drops it, revoking the lease and any
        // candidate key it created — no orphan.
        let id = self
            .client
            .lease_client()
            .grant(HOLD_TTL_SECS, None)
            .await?
            .id();
        let keepalive = spawn_keepalive(&self.client, id);
        let name = keyspace::election_name(&self.ns, key);
        let resp = self
            .client
            .election_client()
            .campaign(name.into_bytes(), LEADER_VALUE.to_vec(), id)
            .await?;
        let leader_key = resp
            .leader()
            .cloned()
            .ok_or("elect_leader: campaign returned no leader key")?;
        let token = token_from_revision(leader_key.rev());
        // Won: retain the hold so the keep-alive persists for the term.
        self.state()?.leaders.insert(
            key.to_owned(),
            LeaderHold {
                leader_key,
                keepalive,
            },
        );
        Ok(Leadership { token })
    }

    async fn lock(&self, key: &str) -> Result<Option<LockGuard>> {
        let id = self
            .client
            .lease_client()
            .grant(HOLD_TTL_SECS, None)
            .await?
            .id();
        let keepalive = spawn_keepalive(&self.client, id);
        let lock_key = keyspace::lock_key(&self.ns, key);
        // Acquire iff the lock key is NOT already held (see `LOCK_HELD`). ONE atomic
        // txn phrased as a HELD-test so it reads the same on real etcd and the
        // simulator: `when value == LOCK_HELD` (held) does nothing; the `or_else`
        // (not held — including absent, which real etcd's value-compare reports as
        // `false`) puts our key-with-lease. `succeeded()` therefore means "was
        // already held" (we lose); `!succeeded()` means the `or_else` put ran (we
        // won). etcd serializes txns by revision, so two racing acquirers can never
        // both see "not held" — mutual exclusion holds.
        let txn = Txn::new()
            .when(vec![Compare::value(
                lock_key.clone().into_bytes(),
                CompareOp::Equal,
                LOCK_HELD.to_vec(),
            )])
            .or_else(vec![TxnOp::put(
                lock_key.into_bytes(),
                LOCK_HELD.to_vec(),
                Some(PutOptions::new().with_lease(id)),
            )]);
        let resp = self.client.kv_client().txn(txn).await?;
        if resp.succeeded() {
            // The key already carries LOCK_HELD — held by someone else: release the
            // lease we speculatively granted.
            keepalive.stop_without_revoke();
            let _ = self.client.lease_client().revoke(id).await;
            return Ok(None);
        }
        let token = token_from_revision(resp.header().map(|h| h.revision()).unwrap_or(0));
        self.state()?.locks.insert(
            token,
            LockHold {
                lease_id: id,
                keepalive,
            },
        );
        Ok(Some(LockGuard { token }))
    }

    async fn unlock(&self, guard: LockGuard) -> Result<()> {
        let hold = self.state()?.locks.remove(&guard.token);
        if let Some(hold) = hold {
            // Stop the keep-alive, then revoke OUR lease synchronously. Revoking
            // the lease atomically deletes the key bound to it — never another
            // holder's reacquired key (a stale token's lease is already gone, so
            // this is a no-op on their hold). Release is conditional-by-construction.
            hold.keepalive.stop_without_revoke();
            let _ = self.client.lease_client().revoke(hold.lease_id).await;
        }
        // Idempotent: an already-released token is a no-op.
        Ok(())
    }

    async fn set_config(&self, key: &str, value: Bytes) -> Result<()> {
        let ck = keyspace::config_key(&self.ns, key);
        self.client
            .kv_client()
            .put(ck.into_bytes(), value.to_vec(), None)
            .await?;
        Ok(())
    }

    async fn get_config(&self, key: &str) -> Result<Option<Bytes>> {
        let ck = keyspace::config_key(&self.ns, key);
        let resp = self.client.kv_client().get(ck.into_bytes(), None).await?;
        Ok(resp
            .kvs()
            .first()
            .map(|kv| Bytes::copy_from_slice(kv.value())))
    }

    async fn config_revision(&self) -> Result<u64> {
        // The config revision must advance on config writes ONLY — a watcher polling
        // it must wake when config changes, not on every unrelated registration /
        // lock / election in the cluster. etcd's *header* revision is the global mvcc
        // counter (bumped by every write), so it would leak unrelated traffic into
        // the config watch; instead we take the maximum `mod_revision` over the
        // config keyspace, which advances IFF a config key is (re)written. That
        // matches `coordination-mem`'s per-config counter semantics (config-only
        // advancement), differing only in absolute values — exactly what the shared
        // contract asserts (a strictly rising, config-scoped revision).
        let prefix = keyspace::config_prefix(&self.ns);
        let resp = self
            .client
            .kv_client()
            .get(prefix.into_bytes(), Some(GetOptions::new().with_prefix()))
            .await?;
        let rev = resp
            .kvs()
            .iter()
            .map(|kv| kv.mod_revision())
            .max()
            .unwrap_or(0);
        Ok(token_from_revision(rev))
    }
}
