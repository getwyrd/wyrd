//! redb-backed [`MetadataStore`]: the embedded, single-process metadata backend
//! for dev and the NAS profile (ADR-0008). TiKV is the production backend behind
//! the same trait; choosing between them is composition in `server` (ADR-0010),
//! not a refactor here.
//!
//! redb gives multi-key atomicity directly: a [`commit`](MetadataStore::commit)
//! is one redb write transaction, and because redb serializes write
//! transactions, checking preconditions *inside* that transaction is what makes
//! version compare-and-set correct — a second writer sees the first's committed
//! state and its precondition fails.
//!
//! **Completeness-or-fail-loud (#262, ADR-0011; #516).** A `scan` returns the
//! complete matching set or `Err` — never a silently truncated `Vec`, because a
//! short `inode:` listing shrinks GC's never-reclaim safety set (data loss). The
//! two distributed backends enforce that with a shared [`SCAN_CAP`] above which
//! they fail loud; this backend had *neither* a cap nor a truncation — it could
//! not silently truncate (so it never violated the clause), but it did not
//! enforce it either, and would happily materialize an unbounded `Vec` where FDB
//! or TiKV had returned a loud [`ScanCapExceeded`]. It now raises the **same**
//! seam-crate error at the **same** cap, so the local/dev backend behaves like
//! production at the boundary.
//!
//! **Every operation terminates (#517)** — the contract's liveness clause — is
//! satisfied here *vacuously*, and that is a finding, not an omission. The other
//! two backends need an explicit deadline because they can wait on a network that
//! never answers: FoundationDB's client retries an unreachable cluster forever, and
//! tikv-client leaves both connection establishment and the TSO stream unbounded.
//! redb is an **embedded, single-process** store — every operation is a local
//! memory or file read under a redb transaction, with no network, no retry loop and
//! nothing to wait on that is not the OS's own I/O. There is no unbounded wait to
//! bound, so this backend adds no timeout: one would be a knob with nothing behind
//! it. (Contention is not a hazard either: redb serializes write transactions
//! internally, so a `commit` waits on a lock held by an in-process writer that is
//! itself bounded.)

#![forbid(unsafe_code)]

use async_trait::async_trait;
use bytes::Bytes;
use redb::{backends::InMemoryBackend, Database, ReadableDatabase, ReadableTable, TableDefinition};
use wyrd_traits::{BoxError, CommitOutcome, MetadataStore, Result, WriteBatch};

/// The shared per-`scan` ceiling and its fail-loud error, re-exported from the
/// seam crate so a caller can name them without depending on `wyrd-traits`
/// directly — the same courtesy `metadata-fdb` and `metadata-tikv` extend.
pub use wyrd_traits::{ScanCapExceeded, SCAN_CAP};

/// All metadata lives in one keyspace; the model namespaces keys by prefix
/// (`inode:`, `dirent:`, `pending:`, `meta:`).
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("wyrd.metadata.v0");

/// A [`MetadataStore`] backed by an embedded redb database.
pub struct RedbMetadataStore {
    db: Database,
    scan_cap: usize,
}

impl RedbMetadataStore {
    /// Open (creating if needed) a redb database at `path`.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::from_db(Database::create(path)?)
    }

    /// Create a store backed by an in-memory redb database — for tests and
    /// ephemeral dev use. Deterministic and disk-free.
    pub fn in_memory() -> Result<Self> {
        Self::from_db(Database::builder().create_with_backend(InMemoryBackend::new())?)
    }

    /// Lower this store's per-`scan` cap, so the fail-loud arm is reachable in a
    /// test without materializing 2^20 keys.
    ///
    /// It **refuses to raise** the cap — `min` with [`SCAN_CAP`], exactly as
    /// `FdbMetadataStore::with_scan_cap` does: the cap is a correctness constraint
    /// (#262), not a knob a caller may loosen.
    #[must_use]
    pub fn with_scan_cap(mut self, cap: usize) -> Self {
        self.scan_cap = cap.min(SCAN_CAP);
        self
    }

    /// This store's effective per-`scan` cap — so the clamp above is observable, and
    /// therefore testable, rather than merely asserted around.
    #[must_use]
    pub fn scan_cap(&self) -> usize {
        self.scan_cap
    }

    fn from_db(db: Database) -> Result<Self> {
        // Materialize the table up front so the read paths never race a
        // not-yet-created table.
        let txn = db.begin_write()?;
        txn.open_table(TABLE)?;
        txn.commit()?;
        Ok(Self {
            db,
            scan_cap: SCAN_CAP,
        })
    }
}

#[async_trait]
impl MetadataStore for RedbMetadataStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(TABLE)?;
        Ok(table.get(key)?.map(|v| Bytes::copy_from_slice(v.value())))
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(TABLE)?;
        let mut out = Vec::new();
        for entry in table.iter()? {
            let (k, v) = entry?;
            let key = k.value();
            if key.starts_with(prefix) {
                out.push((key.to_vec(), Bytes::copy_from_slice(v.value())));
                // Fail loud the moment the accumulated set passes the cap, and drop
                // the partial `Vec` — never return a truncated result (#262,
                // ADR-0011). `>` not `>=`, so a scan returning exactly `cap` keys is
                // a legal complete result, matching the boundary the other two
                // backends already agreed on (`metadata-tikv`'s `after_page`).
                if out.len() > self.scan_cap {
                    return Err(BoxError::from(ScanCapExceeded {
                        cap: self.scan_cap,
                        prefix: prefix.to_vec(),
                    }));
                }
            }
        }
        Ok(out)
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let txn = self.db.begin_write()?;
        let mut table = txn.open_table(TABLE)?;

        // Every precondition is read and checked within this serialized write
        // transaction; any mismatch aborts (txn drops on return) with no writes.
        for pc in &batch.preconditions {
            let current = table.get(pc.key.as_slice())?;
            let holds = match &pc.expected {
                Some(expected) => current.as_ref().map(|g| g.value()) == Some(expected.as_ref()),
                None => current.is_none(),
            };
            if !holds {
                return Ok(CommitOutcome::Conflict);
            }
        }

        for (key, value) in &batch.puts {
            table.insert(key.as_slice(), value.as_ref())?;
        }
        for key in &batch.deletes {
            table.remove(key.as_slice())?;
        }

        drop(table);
        txn.commit()?;
        Ok(CommitOutcome::Committed)
    }
}

/// #577 — this backend's faults all classify **terminal** at the seam, and that is a
/// finding rather than an omission (the same shape as the `#517` liveness note above).
///
/// The transient class is "unreachable / timed out / busy", and redb is an **embedded,
/// single-process** store: there is no network to drop, no peer to be unreachable, no
/// deadline to expire. It has nothing to raise a `wyrd_traits::TransientFault` *about*, so
/// it raises none — and its errors reach `Terminal` through the seam's fail-safe default
/// rather than through any redb-specific mapping. These tests exist so that "redb produces
/// no transient class" is an asserted property instead of an assumption: a caller that
/// swaps redb for a distributed backend must find the transient class appearing, and a
/// caller on redb must never see it and start retrying a permanent fault.
#[cfg(test)]
mod error_class_tests {
    use super::*;
    use wyrd_traits::{classify, ErrorClass};

    /// A real cap breach from a real store — the fail-loud error this backend raises most
    /// deliberately (#262: a truncated scan is data loss). Retrying it is futile: the same
    /// scan returns the same too-large set, forever.
    #[test]
    fn a_scan_cap_breach_classifies_terminal() {
        let store = RedbMetadataStore::in_memory()
            .expect("in-memory store")
            .with_scan_cap(1);
        pollster::block_on(async {
            store
                .commit(WriteBatch::new().put(b"k:1".to_vec(), "a").put(b"k:2", "b"))
                .await
                .expect("seed");
            let err = store
                .scan(b"k:")
                .await
                .expect_err("two keys past a cap of one must fail loud");
            assert!(
                err.downcast_ref::<ScanCapExceeded>().is_some(),
                "the fail-loud error stays the seam type callers downcast: {err}"
            );
            assert_eq!(
                classify(err.as_ref()),
                ErrorClass::Terminal,
                "a cap breach is permanent — the identical scan breaches it again: {err}"
            );
            assert!(
                !classify(err.as_ref()).is_transient(),
                "an embedded store must never offer a retry that cannot help: {err}"
            );
        });
    }

    /// A genuine redb-native fault (an unopenable database), not a synthesized one: the
    /// backend's own error type reaches the seam and classifies terminal by the fail-safe
    /// default, with no per-backend mapping needed.
    #[test]
    fn a_native_redb_fault_classifies_terminal() {
        let err = RedbMetadataStore::open("/nonexistent-directory-577/metadata.redb")
            .err()
            .expect("opening a database under a nonexistent directory must fail");
        assert_eq!(
            classify(err.as_ref()),
            ErrorClass::Terminal,
            "an unclassified backend fault defaults to terminal, never transient: {err}"
        );
    }

    /// The negative half, stated directly: nothing this backend can raise is transient.
    #[test]
    fn this_backend_has_no_transient_class_to_produce() {
        let store = RedbMetadataStore::in_memory().expect("in-memory store");
        pollster::block_on(async {
            // A successful path raises nothing at all; the failing paths above are terminal.
            // What is pinned here is that a *healthy* embedded op has no transient outcome
            // to report — there is no network for one to come from.
            assert!(store
                .get(b"absent")
                .await
                .expect(
                    "a get on an empty store \
                cannot fail transiently — there is nothing to be unreachable"
                )
                .is_none());
        });
    }
}
