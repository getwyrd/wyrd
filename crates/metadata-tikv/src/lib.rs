//! TiKV-backed [`MetadataStore`](wyrd_traits::MetadataStore): the distributed,
//! **production** metadata backend (ADR-0008), behind the *unchanged*
//! `MetadataStore` trait. Choosing it over embedded redb is composition in
//! `server` (ADR-0010), not a refactor here — the milestone's whole thesis
//! (proposal 0007).
//!
//! This is the **M4.1 skeleton** (proposal 0007 §"Suggested PR sequence" item 1):
//! the basic `get` / `scan` / `commit` shapes over TiKV's transactional API, so
//! the **shared** conformance suite that redb passes also passes against a real
//! TiKV. The rigorous atomic-commit conflict semantics (`get_for_update` locking
//! discipline hardening, write-conflict → `Conflict` classification,
//! version-CAS-under-contention) are **M4.2** (#253); the native paged prefix
//! scan + read-consistency doc are **M4.3** (#254) — a whole-range shortcut is
//! acceptable in the skeleton.
//!
//! The real backend is compiled only under the `tikv` feature. It is **off by
//! default** so a machine with no TiKV (a laptop or a PDCA worktree) builds this
//! crate as an empty skeleton, never pulls in the pre-1.0 `tikv-client` tree, and
//! keeps `cargo xtask ci` green; the dedicated `xtask tikv-conformance` job turns
//! it on and drives the shared suite against the throwaway `deploy/` TiKV.

#![forbid(unsafe_code)]

/// Keyspace math shared by the store's read/scan/commit paths — kept free of any
/// `tikv-client` dependency so it compiles and is unit-tested on every machine,
/// TiKV or not (the load-light production unit).
pub mod keyspace {
    /// Prefix a logical key with the store's `namespace`, yielding the physical
    /// key actually stored in TiKV. An empty namespace is the identity (the
    /// production default); a per-test namespace gives the isolated "fresh store"
    /// each shared-suite clause expects.
    pub fn physical(namespace: &[u8], key: &[u8]) -> Vec<u8> {
        let mut physical = Vec::with_capacity(namespace.len() + key.len());
        physical.extend_from_slice(namespace);
        physical.extend_from_slice(key);
        physical
    }

    /// Strip the `namespace` back off a physical key read from TiKV, recovering
    /// the logical key the trait exposes. Returns `None` if the physical key is
    /// not under this namespace (so a foreign key is never misattributed).
    pub fn logical(namespace: &[u8], physical: &[u8]) -> Option<Vec<u8>> {
        physical.strip_prefix(namespace).map(<[u8]>::to_vec)
    }

    /// The **exclusive** upper bound of the half-open range that covers every key
    /// beginning with `prefix`: the prefix with its last non-`0xff` byte
    /// incremented and the trailing `0xff`s dropped. `None` means "no upper bound"
    /// — an empty prefix, or an all-`0xff` prefix, scans to the end of the
    /// keyspace. This turns `scan(prefix)` into a bounded TiKV range
    /// `[prefix, upper)` instead of a whole-table filter.
    pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut end = prefix.to_vec();
        while let Some(last) = end.last_mut() {
            if *last < 0xff {
                *last += 1;
                return Some(end);
            }
            end.pop();
        }
        None
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn physical_and_logical_round_trip() {
            let ns = b"conformance/42/".as_slice();
            let phys = physical(ns, b"inode:7");
            assert!(phys.starts_with(ns));
            assert_eq!(logical(ns, &phys).as_deref(), Some(&b"inode:7"[..]));
            // The empty namespace is the identity used in production.
            assert_eq!(physical(b"", b"k"), b"k");
            assert_eq!(logical(b"", b"k").as_deref(), Some(&b"k"[..]));
        }

        #[test]
        fn logical_rejects_a_foreign_key() {
            // A key not under this namespace must not be misattributed to it.
            assert_eq!(logical(b"ns-a/", b"ns-b/inode:1"), None);
        }

        #[test]
        fn prefix_upper_bound_is_the_exclusive_end_of_the_prefix_range() {
            // Ordinary prefix: last byte incremented.
            assert_eq!(prefix_upper_bound(b"p:").as_deref(), Some(&b"p;"[..]));
            // `b':'` (0x3a) -> `b';'` (0x3b), so [b"p:", b"p;") covers p:1, p:2, …
            assert!(b"p:1".as_slice() >= b"p:".as_slice());
            assert!(b"p:1".as_slice() < b"p;".as_slice());
            assert!(b"q:1".as_slice() >= b"p;".as_slice());
        }

        #[test]
        fn prefix_upper_bound_carries_over_trailing_0xff() {
            // Trailing 0xff bytes are dropped and the prior byte is bumped.
            assert_eq!(
                prefix_upper_bound(&[0x01, 0xff]).as_deref(),
                Some(&[0x02][..])
            );
            // Empty prefix and all-0xff prefix scan to the end of the keyspace.
            assert_eq!(prefix_upper_bound(b""), None);
            assert_eq!(prefix_upper_bound(&[0xff, 0xff]), None);
        }
    }
}

#[cfg(feature = "tikv")]
mod store {
    use async_trait::async_trait;
    use bytes::Bytes;
    use tikv_client::{BoundRange, Transaction, TransactionClient};
    use wyrd_traits::{BoxError, CommitOutcome, MetadataStore, Result, WriteBatch};

    use crate::keyspace;

    /// Best-effort-roll back a still-active transaction before surfacing `err`.
    ///
    /// tikv-client 0.4.0 drops an unfinished `Transaction` with the default
    /// `CheckLevel::Panic`, so returning a backend error with a bare `?` — while
    /// the pessimistic txn is still open — would abort the process instead of
    /// yielding `Err`. Finishing the txn with a best-effort rollback (a secondary
    /// rollback error is deliberately ignored so it can't mask the original error)
    /// both preserves the caller's error and leaves no active txn to panic on drop.
    /// The terminal `commit()` needs no such guard: it finalizes the txn whether it
    /// succeeds or fails, so its drop is already safe.
    async fn rollback_then(txn: &mut Transaction, err: tikv_client::Error) -> BoxError {
        let _ = txn.rollback().await;
        err.into()
    }

    /// Is `err` a TiKV **write-write conflict** — a lost race — rather than a
    /// genuine fault?
    ///
    /// The load-bearing partition of the commit contract (`crates/traits/src/lib.rs`
    /// `CommitOutcome`; proposal 0007 §"The semantic translation — two conflict
    /// signals, one outcome, faults stay faults"): a losing writer is
    /// `Ok(Conflict)`, everything else (network, region-unavailable, PD-timeout,
    /// lock-resolution, deadlock, undetermined) stays `Err`. The single TiKV signal
    /// for a write-write race is `Error::KeyError` carrying a `conflict:
    /// Some(WriteConflict)` (kvrpcpb `KeyError.conflict`, tikv-client 0.4.0). Under
    /// a pessimistic txn that key error can arrive wrapped: `commit()` prewrite
    /// batches surface it as `ExtractedErrors`/`MultipleKeyErrors`, and a
    /// `get_for_update` lock failure as `PessimisticLockError`. We recurse into the
    /// wrappers — **any** wrapped write-conflict makes the whole error a conflict —
    /// but we deliberately do **not** treat any other `KeyError` (`locked`,
    /// `deadlock`, `abort`, …) as a conflict: only `conflict.is_some()`.
    fn is_write_conflict(err: &tikv_client::Error) -> bool {
        use tikv_client::Error;
        match err {
            Error::KeyError(ke) => ke.conflict.is_some(),
            Error::MultipleKeyErrors(errs) | Error::ExtractedErrors(errs) => {
                errs.iter().any(is_write_conflict)
            }
            Error::PessimisticLockError { inner, .. } => is_write_conflict(inner),
            _ => false,
        }
    }

    /// Finish `txn` and classify `err` as either a lost race (`Ok(Conflict)`) or a
    /// fault (`Err`).
    ///
    /// A write-write conflict becomes `Ok(Conflict)` **only for a `conditional` batch**
    /// — one carrying preconditions, i.e. a CAS the caller re-reads and retries on
    /// `Conflict`. A precondition-**free** (blind) batch has no precondition to have
    /// failed, and `CommitOutcome::Conflict` is *defined* as "a precondition did not
    /// hold" (`crates/traits/src/lib.rs`); reporting `Conflict` there would let the many
    /// blind writers that use `?` and ignore the `CommitOutcome` (e.g.
    /// `core::repair::enqueue_repair`, custodian desired-state set/clear) read success
    /// while their write was silently dropped. So an unconditional conflict stays `Err`
    /// — the caller *sees* the lost race. Non-conflict faults are always `Err`.
    ///
    /// Best-effort roll back first so no active txn is left to panic on drop and any
    /// prewrite locks release promptly (a secondary rollback error is ignored so it
    /// can't mask the original). Drop-safety holds on every arm it guards (proposal
    /// 0007 / verified backend facts): after a `get_for_update`/`put`/`delete` error the
    /// txn is still `Active`, so the rollback is what makes it drop-safe; after a failed
    /// `commit()` the txn is already past `Active` (it moves to `StartedCommit` before
    /// its RPC), so it is drop-safe regardless and `rollback` accepts `StartedCommit`,
    /// releasing the prewrite locks a losing writer left.
    async fn conflict_or_err(
        txn: &mut Transaction,
        err: tikv_client::Error,
        conditional: bool,
    ) -> Result<CommitOutcome> {
        let _ = txn.rollback().await;
        if conditional && is_write_conflict(&err) {
            Ok(CommitOutcome::Conflict)
        } else {
            Err(err.into())
        }
    }

    /// A [`MetadataStore`] backed by a TiKV cluster, reached through its
    /// transactional API (Percolator 2PC coordinated by PD). Metadata keys/values
    /// are stored **byte-identically** — TiKV never interprets a key or value —
    /// so the application-level version CAS (a full-value precondition) is exact.
    pub struct TikvMetadataStore {
        client: TransactionClient,
        /// Prepended to every key. Empty in production; a per-test value gives the
        /// isolated keyspace each shared-suite clause needs.
        namespace: Vec<u8>,
    }

    impl TikvMetadataStore {
        /// Connect to TiKV via its PD (Placement Driver) endpoints.
        ///
        /// NOTE (proposal 0007 Open questions; research issue #260): the exact
        /// `tikv-client` 0.4.x entry points (`TransactionClient::new`,
        /// `begin_pessimistic`, `get_for_update`, the `commit` write-conflict
        /// error path) are reconfirmed at build time against the pinned version —
        /// this crate compiles only under `--features tikv`.
        pub async fn connect(pd_endpoints: Vec<String>) -> Result<Self> {
            let client = TransactionClient::new(pd_endpoints).await?;
            Ok(Self {
                client,
                namespace: Vec::new(),
            })
        }

        /// Scope this store to an isolated `namespace` (used by the conformance
        /// suite to give each clause a fresh keyspace against one shared cluster).
        #[must_use]
        pub fn with_namespace(mut self, namespace: impl Into<Vec<u8>>) -> Self {
            self.namespace = namespace.into();
            self
        }

        fn physical(&self, key: &[u8]) -> Vec<u8> {
            keyspace::physical(&self.namespace, key)
        }
    }

    #[async_trait]
    impl MetadataStore for TikvMetadataStore {
        async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
            let mut txn = self.client.begin_pessimistic().await?;
            let value = match txn.get(self.physical(key)).await {
                Ok(value) => value,
                Err(e) => return Err(rollback_then(&mut txn, e).await),
            };
            txn.rollback().await?;
            Ok(value.map(Bytes::from))
        }

        async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
            // M4.1 skeleton: one bounded range scan `[prefix, upper)` materialized
            // into the trait's owned Vec (order stays unspecified). Native PAGED
            // scan + the read-consistency doc are M4.3 (#254).
            let start = self.physical(prefix);
            let range: BoundRange = match keyspace::prefix_upper_bound(&start) {
                Some(end) => (start..end).into(),
                None => (start..).into(),
            };
            let mut txn = self.client.begin_pessimistic().await?;
            let pairs = match txn.scan(range, u32::MAX).await {
                Ok(pairs) => pairs,
                Err(e) => return Err(rollback_then(&mut txn, e).await),
            };
            let mut out = Vec::new();
            for pair in pairs {
                let physical: Vec<u8> = pair.0.into();
                if let Some(logical) = keyspace::logical(&self.namespace, &physical) {
                    out.push((logical, Bytes::from(pair.1)));
                }
            }
            txn.rollback().await?;
            Ok(out)
        }

        async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
            // A batch WITH preconditions is a CAS: a write-write conflict is a lost race
            // the caller re-reads and retries (`Ok(Conflict)`). A precondition-FREE
            // (blind) batch has no precondition to fail, so a conflict must NOT surface
            // as `Conflict` — the blind writers that use `?` and ignore `CommitOutcome`
            // would silently drop the write; it stays `Err` (see `conflict_or_err`).
            let conditional = !batch.preconditions.is_empty();
            let mut txn = self.client.begin_pessimistic().await?;

            // Read + byte-compare every precondition INSIDE the one transaction, so
            // preconditions and mutations are all-or-nothing across keys.
            // `get_for_update` is a LOCKING read, so a precondition on a key the
            // batch only reads is still conflict-checked (the proposal's locking
            // rule); the rigorous write-conflict CLASSIFICATION under contention is
            // hardened in M4.2 (#253).
            for pc in &batch.preconditions {
                // A `get_for_update` lock failure can itself be a write-write race
                // (the concurrent winner already holds/moved the lock), so classify
                // it — a losing writer here is a `Conflict`, not a fault. (`conditional`
                // is necessarily true in this loop.)
                let current = match txn.get_for_update(self.physical(&pc.key)).await {
                    Ok(current) => current,
                    Err(e) => return conflict_or_err(&mut txn, e, conditional).await,
                };
                let holds = match &pc.expected {
                    Some(expected) => current.as_deref() == Some(expected.as_ref()),
                    None => current.is_none(),
                };
                if !holds {
                    // Best-effort cleanup: a rollback fault here must not mask the
                    // legitimate precondition-miss `Conflict` we are returning.
                    let _ = txn.rollback().await;
                    return Ok(CommitOutcome::Conflict);
                }
            }

            // A pessimistic `put`/`delete` does NOT merely buffer: it eagerly acquires
            // the pessimistic lock (an RPC) before buffering. So a mutated key that no
            // precondition already locked can lose a write-write race right here. For a
            // CAS batch (`conditional`) that is the trait's `Conflict`; for a blind batch
            // it stays `Err` (a blind write must never be silently reported `Conflict`).
            // A genuine backend fault always falls through to `Err`.
            for (key, value) in &batch.puts {
                if let Err(e) = txn.put(self.physical(key), value.to_vec()).await {
                    return conflict_or_err(&mut txn, e, conditional).await;
                }
            }
            for key in &batch.deletes {
                if let Err(e) = txn.delete(self.physical(key)).await {
                    return conflict_or_err(&mut txn, e, conditional).await;
                }
            }

            // Prewrite can lose a write-write race to a concurrent committer; for a CAS
            // batch that is the trait's `Conflict`, for a blind batch it stays `Err`. Any
            // other commit error stays `Err` (faults, undetermined) — the delicate
            // partition proposal 0007 calls out.
            if let Err(e) = txn.commit().await {
                return conflict_or_err(&mut txn, e, conditional).await;
            }
            Ok(CommitOutcome::Committed)
        }
    }
}

#[cfg(feature = "tikv")]
pub use store::TikvMetadataStore;
