//! **Startup recovery is total over the content it cannot read** (issue #652).
//!
//! `Gateway::recover()` is what the composition root runs *before the gateway serves
//! anything* (`crates/server/src/lib.rs:133-141`; `cli::serve_s3`). An `Err` from it is
//! therefore not one damaged object's failure — the gateway does not start, so **every
//! healthy object** loses its availability, and nothing but manual repair leaves that state
//! (`docs/principles.md` §5 C-1). On `origin/main` three arrangements of ordinary stored
//! bytes produce exactly that:
//!
//! 1. one `inode:` value that does not decode — `metadata::high_water_marks` decoded every
//!    value with `?` (`crates/core/src/metadata.rs:2081` pre-fix);
//! 2. one structurally valid **segmented** root — the same walk refused it outright, because
//!    it had no resolver for the chunk ids it wanted (`metadata.rs:2082-2087` pre-fix); and
//! 3. a `meta:next_inode` counter whose bytes are not a number —
//!    `cli::seed_next_inode_floor` parsed it with `std::str::from_utf8(bytes)?.parse()?`
//!    (`crates/server/src/cli.rs:1696` pre-fix).
//!
//! Each test therefore holds a **healthy** object alongside the damaged record and asserts
//! the whole chain the gateway needs: `recover()` returns `Ok(())`, the healthy object still
//! reads back byte-identically, the damaged record still **raises** the allocator floor (its
//! id comes from its key, which is readable even when its value is not — an id mark an
//! allocator trusts may never be a quiet under-approximation), a following new-key PUT
//! commits with an id strictly above it, and the fault is **attributed** rather than
//! swallowed. That is the containment doctrine this repo already applies to the *same*
//! `inode:` namespace in the custodian's GC walk (`crates/custodian/src/gc.rs:22-31,378-382`).
//!
//! The fourth test covers the one path recovery now **writes** on: repairing an unreadable
//! counter. It pins the interleaving that decides whether that write is safe — a peer
//! allocator winning the race between recovery's read and its compare-and-set — and asserts
//! the repair yields to it rather than rewinding a live allocator.
//!
//! Criteria 1-3 are driven through `Gateway::recover() -> Result<()>`, whose signature the
//! fix does not change, rather than through `high_water_marks`, whose signature it does — a
//! test naming the latter would fail to *compile* against the pre-fix tree, which is not a
//! red assertion. The composition (`RedbMetadataStore` + `FsChunkStore` + `MemCoordination`)
//! and the seed-then-damage technique mirror the precedent this file must not regress,
//! `crates/server/tests/s3_http_wire.rs:665-751`.

#![forbid(unsafe_code)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::inode_key;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::logging::{dispatch, LogConfig};
use wyrd_server::Gateway;
use wyrd_traits::{CommitOutcome, MetadataStore, ScanPage, WriteBatch};

type Backend = Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>;

/// `cli::NEXT_INODE_KEY` is crate-private; the on-disk key it single-sources is
/// `meta:next_inode` (`crates/server/src/cli.rs:62`, mirroring `s3_http_wire.rs:671`).
const NEXT_INODE_KEY: &[u8] = b"meta:next_inode";

/// `lib::ROOT` is crate-private; every object key binds under inode 0
/// (`crates/server/src/lib.rs:47`). Naming it here is what lets the test read back the id a
/// PUT actually minted, rather than inferring it from the counter.
const ROOT: u64 = 0;

/// The audit seam recovery attributes an unaccountable record on
/// (`crates/core/src/metadata.rs:2069-2083`, `crates/server/src/cli.rs:1844-1860`). Counting its
/// occurrences is how a test says "exactly these records were named, and no others".
const AUDIT_TARGET: &str = "wyrd.metadata.recovery.audit";

/// The two `outcome` values recovery reports for an unreadable `meta:next_inode`. Crate-private
/// in `wyrd-server`, so mirrored here exactly as `NEXT_INODE_KEY` above is. They are what keeps
/// the attribution honest under contention: the event must say whether **this** recovery
/// replaced the bytes it names or a concurrent writer got there first — an audit record that
/// claims a destructive repair which did not happen sends an operator after a write no one made.
const COUNTER_REPLACED: &str = "replaced-by-this-recovery";
const COUNTER_SUPERSEDED: &str = "superseded-by-a-concurrent-writer";

/// How long a recovery call may take before this test calls it hung.
///
/// Criterion 3 is "total **in bounded time**": a `seed_next_inode_floor` that answered an
/// unreadable counter with a retry loop that never commits would satisfy "returns `Ok(())`"
/// vacuously by never returning at all. A test that simply awaited it would hang the suite
/// instead of failing, so recovery runs on its own thread and a missed deadline is an
/// assertion failure. Generous (a redb open plus a handful of point reads) so it can only
/// fire on a genuine non-termination, never on a slow machine.
///
/// A **harness deadline, not a lifecycle clock** (ADR-0009): nothing under test is stamped
/// against it or compared to it — it only decides how long this test waits before calling a
/// non-returning call hung.
const RECOVER_BUDGET: Duration = Duration::from_secs(60);

/// A structurally valid **segmented** chunk-map root, byte-for-byte the `SEGMENTED_ROOT_OK`
/// fixture the core unit tests use (`crates/core/src/metadata.rs:2764`) — two segments tiling
/// `size` under one group. That const is `#[cfg(test)]` inside `wyrd-core`, so it cannot be
/// imported; its bytes are pasted instead. Pre-fix, `high_water_marks` refused this shape
/// outright (`metadata.rs:2082-2087`), which is the whole of criterion 2.
const SEGMENTED_ROOT_OK: &[u8] = br#"{"size":12,"chunk_map":{"group":{"nonce":"0123456789abcdef0123456789abcdef","epoch":1},"segment_count":2,"segments":[{"index":0,"byte_offset":0,"byte_len":5},{"index":1,"byte_offset":5,"byte_len":7}]},"state":"Committed","version":1}"#;

/// A `MakeWriter` that appends every emitted line into a shared buffer, so a test can read
/// back exactly what a subscriber wrote. Mirrors
/// `crates/server/tests/log_span_correlation.rs:21-45`.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'w> tracing_subscriber::fmt::MakeWriter<'w> for Capture {
    type Writer = Self;
    fn make_writer(&'w self) -> Self::Writer {
        self.clone()
    }
}

/// A gateway over the real backends at `db_path` / `frags`. redb takes an exclusive lock on
/// its file, so every caller opens one at a time and drops it before the next.
fn open_gateway(db_path: &Path, frags: &Path) -> Backend {
    Gateway::new(
        RedbMetadataStore::open(db_path).expect("open the redb metadata store"),
        FsChunkStore::open(frags).expect("open the fs chunk store"),
        MemCoordination::new(),
    )
}

/// Store one healthy object through the production PUT path and return the store to disk.
fn put_healthy_object(db_path: &Path, frags: &Path, key: &str, body: &[u8]) {
    let gateway = open_gateway(db_path, frags);
    pollster::block_on(gateway.put_object(key, body)).expect("PUT the healthy object");
}

/// Write a raw metadata value, bypassing every production writer — the one artificial step,
/// exactly as `s3_http_wire.rs:686-696` strips a counter to model a legacy store.
fn write_raw(db_path: &Path, key: Vec<u8>, value: &'static [u8]) {
    let meta = RedbMetadataStore::open(db_path).expect("reopen redb to seed the damaged record");
    pollster::block_on(meta.commit(WriteBatch::new().put(key, Bytes::from_static(value))))
        .expect("seed the damaged record");
}

/// The persisted allocator counter as a number, or `None` when it is absent or unreadable.
fn next_inode(db_path: &Path) -> Option<u64> {
    let meta = RedbMetadataStore::open(db_path).expect("reopen redb to read the counter");
    let bytes = pollster::block_on(meta.get(NEXT_INODE_KEY)).expect("get meta:next_inode")?;
    std::str::from_utf8(&bytes).ok()?.parse().ok()
}

/// The inode id a committed object key actually resolves to, through core's own dirent
/// resolver — the direct reading of "the PUT committed *above* the damaged record's id".
fn inode_of(db_path: &Path, key: &str) -> u64 {
    let meta = RedbMetadataStore::open(db_path).expect("reopen redb to resolve the object");
    pollster::block_on(wyrd_core::read::resolve(&meta, ROOT, key))
        .expect("resolve the object key")
        .expect("the object key is bound")
}

/// Run one recovery `job` with the audit seam captured, failing rather than hanging if it has
/// not returned within [`RECOVER_BUDGET`].
///
/// The job runs on its own thread — so a non-returning call is an assertion failure instead
/// of a stalled suite — and the capturing subscriber is installed *there*, because
/// `tracing::dispatcher::with_default` is thread-local: a subscriber installed on the test
/// thread would see nothing. Every redb handle is opened and dropped inside the job, so none
/// is held across the exclusive file lock.
fn within<T: Send + 'static>(job: impl FnOnce() -> T + Send + 'static) -> (T, String) {
    let capture = Capture::default();
    let sink = capture.clone();
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let dispatch = dispatch(
            &LogConfig::new(Some("warn"), None).expect("log config"),
            sink,
            tracing_subscriber::layer::Identity::new(),
        );
        let outcome = tracing::dispatcher::with_default(&dispatch, job);
        let _ = tx.send(outcome);
    });
    let outcome = match rx.recv_timeout(RECOVER_BUDGET) {
        Ok(outcome) => outcome,
        Err(RecvTimeoutError::Timeout) => panic!(
            "recovery did not return within {RECOVER_BUDGET:?} — startup recovery must be \
             total over the store's contents IN BOUNDED TIME; a retry loop that never \
             commits is not an answer",
        ),
        Err(RecvTimeoutError::Disconnected) => panic!("the recovery worker thread panicked"),
    };
    worker.join().expect("join the recovery worker thread");
    (outcome, capture.contents())
}

/// Run `Gateway::recover()` over `db_path` / `frags` under [`within`].
fn recover_within(db_path: &Path, frags: &Path) -> (Result<(), String>, String) {
    let (db, fr): (PathBuf, PathBuf) = (db_path.to_path_buf(), frags.to_path_buf());
    within(move || {
        pollster::block_on(open_gateway(&db, &fr).recover()).map_err(|err| err.to_string())
    })
}

/// How many records recovery named on the audit seam.
fn attributed_records(audit: &str) -> usize {
    audit.matches(AUDIT_TARGET).count()
}

/// **Criterion 1 — total over an unreadable `inode:` value.** A raw, undecodable record sits
/// beside a healthy committed object, at a *higher* id than the healthy one, so the floor
/// asserted below can only come from the damaged record's own key.
///
/// Pre-fix RED: `high_water_marks` decodes every `inode:` value with `decode(&value)?`
/// (`crates/core/src/metadata.rs:2081`), so the damaged record makes the whole walk `Err`,
/// `Gateway::recover()` propagates it, and the first assertion fails.
#[test]
fn recover_is_total_over_an_undecodable_inode_record() {
    const DAMAGED: u64 = 50;
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.redb");
    let frags = dir.path().join("frags");

    put_healthy_object(&db_path, &frags, "bucket/healthy", b"still readable");
    write_raw(&db_path, inode_key(DAMAGED), b"not a metadata record");
    // A second damaged row, unreadable in BOTH halves: its key is not `inode:<id>` and its
    // value is not a record either. One stored row must yield one repair obligation.
    write_raw(
        &db_path,
        b"inode:not-an-id".to_vec(),
        b"not a record either",
    );

    let (outcome, audit) = recover_within(&db_path, &frags);
    outcome.expect(
        "recover() must return Ok(()) over a store holding one undecodable inode: record — \
         refusing to start costs every HEALTHY object its availability over one damaged one",
    );

    assert!(
        audit.contains("undecodable-inode-record") && audit.contains("inode:50"),
        "the unreadable record must be ATTRIBUTED (its key and fault named on the audit \
         seam), not swallowed — contained means named, so an operator can repair it; \
         captured: {audit}",
    );
    assert!(
        audit.contains("unparsable-inode-key") && audit.contains("inode:not-an-id"),
        "the row whose KEY names no id must be attributed too — it is a row this walk \
         cannot account for; captured: {audit}",
    );
    // Exactly TWO records were named, one per damaged ROW: attribution that fired for the
    // healthy record would be noise an operator cannot act on, and a row named once per
    // fault rather than once per row would inflate the counter this event feeds and hand an
    // operator two repair obligations for one stored row.
    assert_eq!(
        attributed_records(&audit),
        2,
        "one repair obligation per damaged row: the healthy record decoded and is accounted \
         for, and the row whose key AND value are both unreadable is named ONCE, not twice; \
         captured: {audit}",
    );

    // The floor is above the damaged record's id even though its value never decoded: the id
    // is recovered from the KEY. A mark an allocator trusts may never under-approximate.
    let counter = next_inode(&db_path).expect("recover() must persist a readable meta:next_inode");
    assert!(
        counter > DAMAGED,
        "recover() must seed the allocator strictly above the damaged record's key id \
         ({DAMAGED}); got meta:next_inode={counter}",
    );

    let gateway = open_gateway(&db_path, &frags);
    assert_eq!(
        pollster::block_on(gateway.get_object("bucket/healthy"))
            .expect("GET the healthy object")
            .as_deref(),
        Some(&b"still readable"[..]),
        "the healthy object must survive recovery over a store that also holds a damaged \
         record, byte-identical",
    );
    pollster::block_on(gateway.put_object("bucket/after-recover", b"new object")).expect(
        "a new-key PUT after recover() must COMMIT — the allocator was seeded above every id \
         whose bytes are still on disk, so create's require_absent cannot collide",
    );
    assert_eq!(
        pollster::block_on(gateway.get_object("bucket/after-recover"))
            .expect("GET the new object")
            .as_deref(),
        Some(&b"new object"[..]),
    );
    drop(gateway);

    let minted = inode_of(&db_path, "bucket/after-recover");
    assert!(
        minted > DAMAGED,
        "the id the new-key PUT committed under ({minted}) must be strictly greater than the \
         unreadable record's id ({DAMAGED}) — otherwise recovery handed out an id whose bytes \
         are still on disk",
    );
}

/// **Criterion 2 — total over a segmented root.** Same shape, with a *structurally valid*
/// segmented chunk-map root in place of the undecodable bytes: a record this walk has no
/// resolver for is contained exactly as unreadable bytes are — and, because it is a record
/// the walk could not fully account for, **named on the same seam**. It decodes, so nothing
/// on the decode-failure path would ever mention it.
///
/// Pre-fix RED: `high_water_marks` refuses the shape outright —
/// `.as_flat().ok_or(ChunkMapError::SegmentedMapUnsupported { operation: "high_water_marks" })?`
/// (`crates/core/src/metadata.rs:2082-2087`) — so `recover()` is `Err`.
#[test]
fn recover_is_total_over_a_segmented_root() {
    const SEGMENTED: u64 = 37;
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.redb");
    let frags = dir.path().join("frags");

    put_healthy_object(&db_path, &frags, "bucket/healthy", b"flat and readable");
    write_raw(&db_path, inode_key(SEGMENTED), SEGMENTED_ROOT_OK);

    let (outcome, audit) = recover_within(&db_path, &frags);
    outcome.expect(
        "recover() must return Ok(()) over a store holding a structurally valid SEGMENTED \
         root — a shape this walk cannot resolve chunk ids from is still a record it must \
         contain, not a reason to refuse to start",
    );

    assert!(
        audit.contains("unresolved-segmented-inode-root") && audit.contains("inode:37"),
        "the segmented root must be ATTRIBUTED by key: it decodes, so the walk knows its id, \
         but it has no resolver for what is inside it — a record accounted for only in part \
         is named, never silently passed over; captured: {audit}",
    );
    assert_eq!(
        attributed_records(&audit),
        1,
        "only the segmented root may be named; the healthy flat record is fully accounted \
         for; captured: {audit}",
    );

    let counter = next_inode(&db_path).expect("recover() must persist a readable meta:next_inode");
    assert!(
        counter > SEGMENTED,
        "the mark must still be >= the segmented record's key-derived id ({SEGMENTED}), so \
         the seeded floor is above it; got meta:next_inode={counter}",
    );

    let gateway = open_gateway(&db_path, &frags);
    assert_eq!(
        pollster::block_on(gateway.get_object("bucket/healthy"))
            .expect("GET the healthy object")
            .as_deref(),
        Some(&b"flat and readable"[..]),
        "the healthy object must survive recovery over a store that also holds a segmented \
         root, byte-identical",
    );
    pollster::block_on(gateway.put_object("bucket/after-recover", b"new object"))
        .expect("a new-key PUT after recover() must COMMIT");
    drop(gateway);

    let minted = inode_of(&db_path, "bucket/after-recover");
    assert!(
        minted > SEGMENTED,
        "the id the new-key PUT committed under ({minted}) must be strictly greater than the \
         segmented record's id ({SEGMENTED})",
    );
}

/// **Criterion 3 — total over a corrupt counter, in bounded time.** `meta:next_inode` holds
/// bytes that are not a number; recovery must neither refuse nor spin, and must leave the
/// counter at or above the floor it recovered from the `inode:` records — otherwise the next
/// PUT fails on the same unreadable key and the gateway serves nothing new.
///
/// Pre-fix RED: `seed_next_inode_floor` parses it with
/// `std::str::from_utf8(bytes)?.parse()?` (`crates/server/src/cli.rs:1696`), so `recover()`
/// is `Err`. A hypothetical never-committing retry loop fails this test on
/// [`RECOVER_BUDGET`] rather than hanging it.
#[test]
fn recover_is_total_over_a_corrupt_next_inode_counter() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.redb");
    let frags = dir.path().join("frags");

    put_healthy_object(
        &db_path,
        &frags,
        "bucket/healthy",
        b"committed under inode 1",
    );
    let committed = inode_of(&db_path, "bucket/healthy");
    write_raw(&db_path, NEXT_INODE_KEY.to_vec(), b"not-a-number");

    let (outcome, audit) = recover_within(&db_path, &frags);
    outcome.expect(
        "recover() must return Ok(()) with an unreadable meta:next_inode — a counter whose \
         bytes cannot be read must not stop the gateway from starting",
    );
    assert!(
        audit.contains("unreadable-next-inode-counter") && audit.contains("not-a-number"),
        "the unreadable counter must be ATTRIBUTED on the audit seam, quoting the bytes the \
         repair replaces — a destructive repair that records nothing leaves an operator with \
         no way to tell what the store said; captured: {audit}",
    );
    assert!(
        audit.contains(COUNTER_REPLACED) && !audit.contains(COUNTER_SUPERSEDED),
        "and the attribution must report what the store actually did: this recovery's \
         compare-and-set won, so the event says `{COUNTER_REPLACED}`; captured: {audit}",
    );

    let counter = next_inode(&db_path).expect(
        "recover() must leave meta:next_inode READABLE and at least the recovered floor — \
         leaving the corrupt bytes in place would fail every later alloc_inode instead",
    );
    assert!(
        counter > committed,
        "the repaired counter ({counter}) must be above every committed inode id \
         ({committed}), which is the floor recovered from the inode: records",
    );

    let gateway = open_gateway(&db_path, &frags);
    pollster::block_on(gateway.put_object("bucket/after-recover", b"new object")).expect(
        "after recover() repaired the counter, a new-key PUT must COMMIT — a recovery that \
         returned Ok while leaving the counter unreadable would fail here",
    );
    assert_eq!(
        pollster::block_on(gateway.get_object("bucket/healthy"))
            .expect("GET the healthy object")
            .as_deref(),
        Some(&b"committed under inode 1"[..]),
        "the object committed before the counter was corrupted survives byte-identical",
    );
    drop(gateway);

    let minted = inode_of(&db_path, "bucket/after-recover");
    assert!(
        minted > committed,
        "the id the new-key PUT committed under ({minted}) must be strictly greater than \
         every committed inode id ({committed})",
    );
}

/// A `MetadataStore` that forwards every call to the **real** redb store beneath it and, on
/// the first read of `meta:next_inode`, lets a peer allocator win the race that decides
/// whether the counter repair is safe: the peer's value lands *after* this read returned the
/// damaged bytes and *before* the caller's compare-and-set reaches the store.
///
/// Only the interleaving is injected; every read and write is the production backend's, and
/// the code under test is the production `cli::seed_next_inode_floor` that
/// `Gateway::recover` calls (`crates/server/src/lib.rs:141`).
struct PeerWinsTheCas<'store> {
    inner: &'store RedbMetadataStore,
    peer_value: Vec<u8>,
    fired: AtomicBool,
}

#[async_trait::async_trait]
impl MetadataStore for PeerWinsTheCas<'_> {
    async fn get(&self, key: &[u8]) -> wyrd_traits::Result<Option<Bytes>> {
        let read = self.inner.get(key).await?;
        if key == NEXT_INODE_KEY && !self.fired.swap(true, Ordering::SeqCst) {
            self.inner
                .commit(WriteBatch::new().put(
                    NEXT_INODE_KEY.to_vec(),
                    Bytes::from(self.peer_value.clone()),
                ))
                .await?;
        }
        Ok(read)
    }

    async fn scan(&self, prefix: &[u8]) -> wyrd_traits::Result<Vec<(Vec<u8>, Bytes)>> {
        self.inner.scan(prefix).await
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> wyrd_traits::Result<ScanPage> {
        self.inner.scan_page(prefix, after, limit).await
    }

    async fn commit(&self, batch: WriteBatch) -> wyrd_traits::Result<CommitOutcome> {
        self.inner.commit(batch).await
    }
}

/// **The counter repair never rewinds an allocator that won the race.** Repairing an
/// unreadable `meta:next_inode` is the one thing recovery now *writes* over damage, so the
/// decisive interleaving is pinned deterministically rather than left to chance: a peer
/// writes a good counter far above the floor between this recovery's read and its
/// compare-and-set.
///
/// The compare-and-set is guarded on the exact bytes that were read
/// (`crates/server/src/cli.rs:1774-1779`), so the peer's write makes it conflict; the retry
/// re-reads, finds a readable counter already at or above the floor, and leaves it. Replace
/// that guard with an unconditional put and the repair overwrites the peer's counter with the
/// much lower floor — handing out ids the peer has already spent — which is exactly what this
/// test's final assertion catches.
///
/// Drives `cli::seed_next_inode_floor` directly, the function `Gateway::recover` delegates
/// its second half to, because the interleaving has to be injected at the store the recovery
/// reads through. Its signature is unchanged by this patch, so this test compiles — and fails
/// on the pre-fix parse error — against the base tree exactly as criteria 1-3 do.
#[test]
fn the_counter_repair_yields_to_an_allocator_that_won_the_race() {
    const PEER_COUNTER: u64 = 900;
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.redb");
    let frags = dir.path().join("frags");

    put_healthy_object(&db_path, &frags, "bucket/healthy", b"committed");
    let committed = inode_of(&db_path, "bucket/healthy");
    let floor = committed + 1;
    write_raw(&db_path, NEXT_INODE_KEY.to_vec(), b"\x00torn counter");

    let db = db_path.clone();
    let (outcome, audit) = within(move || {
        let meta = RedbMetadataStore::open(&db).expect("reopen redb for the raced recovery");
        let raced = PeerWinsTheCas {
            inner: &meta,
            peer_value: PEER_COUNTER.to_string().into_bytes(),
            fired: AtomicBool::new(false),
        };
        pollster::block_on(wyrd_server::cli::seed_next_inode_floor(&raced, floor))
            .map_err(|err| err.to_string())
    });
    outcome.expect(
        "seeding over an unreadable counter must return Ok(()) even when it loses the \
         compare-and-set — losing a race to a live allocator is not a reason to refuse to \
         start",
    );
    assert!(
        audit.contains("unreadable-next-inode-counter"),
        "the unreadable counter is still attributed when the repair loses the race; \
         captured: {audit}",
    );
    assert!(
        audit.contains(COUNTER_SUPERSEDED) && !audit.contains(COUNTER_REPLACED),
        "and it must NOT claim a repair it did not perform: the peer won the \
         compare-and-set, so this recovery replaced nothing and the event must say \
         `{COUNTER_SUPERSEDED}`, not `{COUNTER_REPLACED}` — an audit record that over-states \
         what the store did sends an operator looking for a write no one made; captured: \
         {audit}",
    );

    assert_eq!(
        next_inode(&db_path),
        Some(PEER_COUNTER),
        "the peer allocator's counter must stand: a repair that rewound it to the floor \
         ({floor}) would re-hand ids the peer has already spent — the repair is allowed to \
         replace bytes nobody can read, never a value a live allocator wrote",
    );
}
