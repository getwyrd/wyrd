//! Issue #839 (809.1) — the post-restore pass **reports** what it kept or held on staged grounds
//! (the restore row of `docs/design/proposals/draft/0016-multipart-commit-protocol.md`, `:823`).
//! On `main` a fragment only the staged class keeps is skipped uncounted, and an untrusted staged
//! record holds its chunk on the audit seam alone, while `is_clean()` certifies the run.
//!
//! - **E** staged skips are counted, each kept fragment once, by the FIRST protection that keeps
//!   it (committed readings, staged class, displaced check, pending lease).
//! - **H-iii** an untrusted staged record is named by key, needs no human, and the run is not
//!   clean; none of its chunk's fragments is marked, and the audit seam still names it.
//!
//! Both legs drive the production `reconcile_after_restore` over in-memory doubles in this file;
//! every staged record is seeded (no client creates a session before #508). The fields this slice
//! adds are read through `Debug`, so the file builds on the base and fails there by assertion.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;

use async_trait::async_trait;
use bytes::Bytes;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use wyrd_core::metadata::{
    self, inode_key, orphan_key, pending_key, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState,
    PendingEntry,
};
use wyrd_core::multipart::{mpu_key, part_key, PartNumber, UploadId};
use wyrd_custodian::{reconcile_after_restore, ExpiredPendingPolicy, GcContext, RestoreReport};
use wyrd_traits::{
    page_cursor, page_limit, page_start, BoxError, ChunkId, ChunkStore, CommitOutcome, DServerId,
    FragmentId, Health, MetadataStore, PageStart, Result, ScanCapExceeded, ScanPage, WriteBatch,
    SCAN_CAP,
};

/// The reader-safe grace window the pass runs with.
const GRACE: u64 = 50;
/// The instant the pass runs at.
const NOW: u64 = 10_000;
/// The bucket and object every seeded session targets.
const PARENT: InodeId = 42;
const OBJECT: &str = "staged/object";
/// Every seeded session's epoch, which its part records carry too.
const EPOCH: u64 = 3;

const RS_2_1: EcScheme = EcScheme::ReedSolomon { k: 2, m: 1 };

// ---- the metadata double ------------------------------------------------------------------------

/// An in-memory `MetadataStore` over an ordered map. `scan` refuses a result past the seam's cap
/// and `scan_page` clamps a page to it through the seam's own `page_limit` / `page_start` /
/// `page_cursor`, as every backend does.
#[derive(Default)]
struct Meta {
    kv: Mutex<BTreeMap<Vec<u8>, Bytes>>,
}

impl Meta {
    /// Put a fixture record in place — not a pass's write.
    fn seed(&self, key: impl Into<Vec<u8>>, value: impl Into<Bytes>) {
        self.kv.lock().unwrap().insert(key.into(), value.into());
    }

    fn holds(&self, key: &[u8]) -> bool {
        self.kv.lock().unwrap().contains_key(key)
    }

    /// Apply `batch` atomically: every precondition holds, or nothing changes.
    fn apply(&self, batch: WriteBatch) -> CommitOutcome {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key) != pre.expected.as_ref() {
                return CommitOutcome::Conflict;
            }
        }
        for key in batch.deletes {
            kv.remove(&key);
        }
        for (key, value) in batch.puts {
            kv.insert(key, value);
        }
        CommitOutcome::Committed
    }
}

#[async_trait]
impl MetadataStore for Meta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        let hits: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if hits.len() > SCAN_CAP {
            return Err(BoxError::from(ScanCapExceeded {
                cap: SCAN_CAP,
                prefix: prefix.to_vec(),
            }));
        }
        Ok(hits)
    }

    async fn scan_page(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<ScanPage> {
        let limit = page_limit(limit, SCAN_CAP, prefix)?;
        let lower = match page_start(prefix, after) {
            PageStart::After(cursor) => Bound::Excluded(cursor),
            PageStart::Prefix => Bound::Included(prefix),
            PageStart::PastPrefix => return Ok((Vec::new(), None)),
        };
        let items: Vec<(Vec<u8>, Bytes)> = self
            .kv
            .lock()
            .unwrap()
            .range::<[u8], _>((lower, Bound::Unbounded))
            .take_while(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let next = page_cursor(&items, limit);
        Ok((items, next))
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        Ok(self.apply(batch))
    }
}

// ---- the D-server double and the fleet ----------------------------------------------------------

/// One D server's fragments.
#[derive(Default)]
struct Disk {
    frags: Mutex<HashMap<FragmentId, Bytes>>,
}

#[async_trait]
impl ChunkStore for Disk {
    async fn put_fragment(
        &self,
        id: FragmentId,
        fragment: Bytes,
        _deadline_millis: Option<u64>,
    ) -> Result<()> {
        self.frags.lock().unwrap().insert(id, fragment);
        Ok(())
    }

    async fn get_fragment(&self, id: FragmentId) -> Result<Option<Bytes>> {
        Ok(self.frags.lock().unwrap().get(&id).cloned())
    }

    async fn list_fragments(&self) -> Result<Vec<FragmentId>> {
        Ok(self.frags.lock().unwrap().keys().copied().collect())
    }

    async fn delete_fragment(&self, id: FragmentId) -> Result<()> {
        self.frags.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn health(&self) -> Result<Health> {
        Ok(Health::Healthy)
    }
}

fn disks() -> [Disk; 4] {
    Default::default()
}

fn fleet(d: &[Disk; 4]) -> [(DServerId, &dyn ChunkStore); 4] {
    [(0, &d[0]), (1, &d[1]), (2, &d[2]), (3, &d[3])]
}

fn frag(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// Put `frag` on `dserver`. The post-restore pass lists fragments and never reads their bytes.
fn place(d: &[Disk; 4], dserver: DServerId, frag: FragmentId) {
    d[dserver as usize]
        .frags
        .lock()
        .unwrap()
        .insert(frag, Bytes::from_static(b"staged"));
}

/// One post-restore pass at [`NOW`], over the whole fleet.
async fn restore_pass(meta: &Meta, d: &[Disk; 4]) -> RestoreReport {
    let fleet = fleet(d);
    let ctx = GcContext {
        meta,
        fleet: &fleet,
        grace_window_millis: GRACE,
        expired_pending: ExpiredPendingPolicy::Defer,
    };
    reconcile_after_restore(&ctx, NOW)
        .await
        .expect("the post-restore pass runs")
}

// ---- the records --------------------------------------------------------------------------------

/// An upload id: 32 lowercase-hex characters from a 2-character pair.
fn upload(pair: &str) -> UploadId {
    UploadId::new(pair.repeat(16)).expect("32 lowercase-hex characters")
}

fn part_no(n: u32) -> PartNumber {
    PartNumber::new(n).expect("a part number in range")
}

/// An `Open` session record. EVERY session this file seeds is built here, and only here.
///
/// No segment nonce, and deliberately no check through the session decoder: this base's codec
/// refuses the nonce, and this pass never decodes a session value. The slice that first decodes
/// one here adds the nonce to this helper; a decode check would instead refuse every nonce-less
/// record once the nonce lands, and fail the file for a reason neither leg is about.
fn open_session() -> Bytes {
    Bytes::from(format!(
        "{{\"parent\":{PARENT},\"object\":\"{OBJECT}\",\"created_at_millis\":100,\
         \"clock_source\":\"wall\",\"epoch\":{EPOCH},\"attempts\":1,\
         \"state\":{{\"kind\":\"Open\"}}}}"
    ))
}

fn chunk_ref(id: ChunkId, scheme: EcScheme, placement: &[DServerId]) -> ChunkRef {
    ChunkRef {
        id,
        scheme,
        len: 5,
        placement: placement.to_vec(),
    }
}

/// A committed part record naming `chunks`, in the part decoder's canonical spelling (the shape
/// `staged_protection.rs`'s `part` helper round-trips). A wrong-length placement decodes too, and
/// is then held. Each leg asserts `unresolvable` stays empty, so a record that did not decode
/// fails the leg by name.
fn part(chunks: &[ChunkRef]) -> Bytes {
    let refs: Vec<String> = chunks
        .iter()
        .map(|chunk| String::from_utf8(metadata::encode(chunk).to_vec()).unwrap())
        .collect();
    let len: u64 = chunks.iter().map(|chunk| chunk.len).sum();
    Bytes::from(format!(
        "{{\"chunks\":[{}],\"len\":{len},\"digest\":\"{}\",\"committed_at_millis\":800,\
         \"session_epoch\":{EPOCH}}}",
        refs.join(","),
        "ef".repeat(32)
    ))
}

/// A committed object at `inode` whose one chunk is `chunk`.
fn commit_object(meta: &Meta, inode: InodeId, chunk: ChunkRef) {
    let record = InodeRecord {
        size: chunk.len,
        chunk_map: vec![chunk].into(),
        state: InodeState::Committed,
        version: 1,
        ..Default::default()
    };
    meta.seed(inode_key(inode), metadata::encode(&record));
}

/// A live `pending:` lease on `chunk` — an ordinary one, far from expiry.
fn live_lease(meta: &Meta, chunk: ChunkId) {
    let entry = PendingEntry {
        lease_expiry_millis: NOW + 60_000,
        owner: None,
        staged: None,
    };
    meta.seed(pending_key(chunk), metadata::encode(&entry));
}

/// The value `field` has in `report`'s `Debug` rendering, as text — a count as its digits, a list
/// as `[..]` — or `None` when the rendering has no such field. The fields this slice adds are read
/// this way, so the file builds on a base without them and fails there by assertion.
fn debug_field(report: &RestoreReport, field: &str) -> Option<String> {
    let rendered = format!("{report:?}");
    let start = [format!("{{ {field}: "), format!(", {field}: ")]
        .iter()
        .find_map(|needle| rendered.find(needle.as_str()).map(|at| at + needle.len()))?;
    let rest = &rendered[start..];
    let end = if rest.starts_with('[') {
        rest.find(']')? + 1
    } else {
        rest.find([',', ' ']).unwrap_or(rest.len())
    };
    Some(rest[..end].to_owned())
}

// ---- the audit seam -----------------------------------------------------------------------------

/// One custodian audit event, as the thread that emitted it saw it.
struct AuditEvent {
    thread: ThreadId,
    target: String,
    fields: Vec<String>,
}

fn audit_log() -> &'static Mutex<Vec<AuditEvent>> {
    static LOG: OnceLock<Mutex<Vec<AuditEvent>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::new()))
}

/// Install the capture once for the whole test binary, before any pass on any thread runs, so no
/// audit callsite is ever first met with no subscriber in place.
fn capture_audit() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        tracing_subscriber::registry()
            .with(AuditCapture)
            .try_init()
            .expect("this test binary installs the only global subscriber");
    });
}

struct AuditCapture;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for AuditCapture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target();
        if !target.starts_with("wyrd.custodian.") {
            return;
        }
        let mut fields = FieldText(Vec::new());
        event.record(&mut fields);
        audit_log().lock().unwrap().push(AuditEvent {
            thread: std::thread::current().id(),
            target: target.to_owned(),
            fields: fields.0,
        });
    }
}

struct FieldText(Vec<String>);

impl Visit for FieldText {
    fn record_str(&mut self, _field: &Field, value: &str) {
        self.0.push(value.to_owned());
    }

    fn record_debug(&mut self, _field: &Field, value: &dyn Debug) {
        self.0.push(format!("{value:?}"));
    }
}

const RESTORE_AUDIT: &str = "wyrd.custodian.restore.audit";

/// Whether a pass on this thread emitted an `action` event on the restore audit seam naming
/// `record`.
fn audited(action: &str, record: &str) -> bool {
    let thread = std::thread::current().id();
    audit_log().lock().unwrap().iter().any(|event| {
        event.thread == thread
            && event.target == RESTORE_AUDIT
            && event.fields.iter().any(|field| field == action)
            && event.fields.iter().any(|field| field.contains(record))
    })
}

// ---- (E) staged skips are counted, each kept fragment once --------------------------------------

/// **(E)** One `Open` session whose `part:` records place two fragments nothing else protects, one
/// whose chunk a live lease also holds, one the displaced check would also keep (the committed map
/// places it on server 0, which lacks it; the part record, on server 3, where it is — the shape of
/// `restore_reconcile.rs`'s `the_only_copy_of_a_moved_fragment_is_never_marked`), and one a
/// committed object ALSO places there. Plus one stray. The first four count as staged and nowhere
/// else; the fifth is the committed set's; the stray alone is marked.
///
/// Base: no `staged_skipped` in the report — the four are skipped uncounted.
#[tokio::test]
async fn staged_skips_are_counted_once_by_the_first_protection_that_keeps_them() {
    let meta = Meta::default();
    let d = disks();
    let id = upload("e1");
    meta.seed(mpu_key(&id), open_session());

    // Two fragments only the staged class protects.
    let only_staged = [(1, frag(0xE1, 0)), (2, frag(0xE2, 0))];
    meta.seed(
        part_key(&id, part_no(1)),
        part(&[
            chunk_ref(0xE1, EcScheme::None, &[1]),
            chunk_ref(0xE2, EcScheme::None, &[2]),
        ]),
    );

    // Staged, and its chunk holds a live lease too.
    let staged_and_pending = (0, frag(0xE3, 0));
    meta.seed(
        part_key(&id, part_no(2)),
        part(&[chunk_ref(0xE3, EcScheme::None, &[0])]),
    );
    live_lease(&meta, 0xE3);

    // Staged, and displaced: fragment 0 is on server 3, where the part record places it, not on
    // server 0, where the committed map does. Fragments 1 and 2 are where both place them — the
    // committed set's, and they keep the chunk readable (two of three, k = 2).
    commit_object(&meta, 1, chunk_ref(0xE4, RS_2_1, &[0, 1, 2]));
    meta.seed(
        part_key(&id, part_no(3)),
        part(&[chunk_ref(0xE4, RS_2_1, &[3, 1, 2])]),
    );
    let staged_and_displaced = (3, frag(0xE4, 0));
    let committed_siblings = [(1, frag(0xE4, 1)), (2, frag(0xE4, 2))];

    // Committed AND staged at the same server: the committed set's, first.
    commit_object(&meta, 2, chunk_ref(0xE6, EcScheme::None, &[2]));
    meta.seed(
        part_key(&id, part_no(4)),
        part(&[chunk_ref(0xE6, EcScheme::None, &[2])]),
    );
    let committed_and_staged = (2, frag(0xE6, 0));

    // The one stray: nothing names it.
    let stray = (3, frag(0xE5, 0));

    let kept: Vec<(DServerId, FragmentId)> = only_staged
        .into_iter()
        .chain([
            staged_and_pending,
            staged_and_displaced,
            committed_and_staged,
        ])
        .chain(committed_siblings)
        .collect();
    for &(dserver, fragment) in kept.iter().chain([&stray]) {
        place(&d, dserver, fragment);
    }

    let report = restore_pass(&meta, &d).await;

    assert!(
        report.unresolvable.is_empty(),
        "a seeded record did not read: {report:?}"
    );
    for (field, expected, why) in [
        (
            "staged_skipped",
            "4",
            "each staged-first fragment once, not the committed one",
        ),
        (
            "pending_skipped",
            "0",
            "a staged-first fragment counted again under its lease",
        ),
        (
            "displaced_kept",
            "0",
            "a staged-first fragment counted again as displaced",
        ),
    ] {
        assert_eq!(
            debug_field(&report, field).as_deref(),
            Some(expected),
            "{field}: {why}: {report:?}"
        );
    }
    assert_eq!(report.stranded_marked, 1, "the stray alone: {report:?}");
    assert!(meta.holds(&orphan_key(stray.0, stray.1)), "{report:?}");
    for (dserver, fragment) in kept {
        assert!(
            !meta.holds(&orphan_key(dserver, fragment)),
            "{fragment:?} on server {dserver} was marked although a record protects it"
        );
    }
}

// ---- (H-iii) an untrusted staged record is named, and needs no human ----------------------------

/// **(H-iii)** Session 1 holds one `part:` record whose two chunks each have a wrong-length
/// placement (one and two servers for three fragments); session 2 holds trusted records only.
/// Nothing else is in the store, so the held record is the run's ONLY finding. (a) The report
/// lists session 1's record once, by key, and no key of session 2; (b) no human (#664's plan
/// revision, 2026-09-18), and not clean; (c) nothing of the held chunks marked, and the audit
/// seam still names the record.
///
/// Base: no `staged_untrusted` in the report, and `is_clean()` is true.
#[tokio::test]
async fn an_untrusted_staged_record_is_named_needs_no_human_and_is_not_a_clean_bill() {
    capture_audit();
    let meta = Meta::default();
    let d = disks();

    // Session 1: one part record, neither of whose chunks it can be trusted about.
    let untrusted_id = upload("a1");
    meta.seed(mpu_key(&untrusted_id), open_session());
    let untrusted = part_key(&untrusted_id, part_no(1));
    meta.seed(
        untrusted.clone(),
        part(&[
            chunk_ref(0xA1, RS_2_1, &[0]),
            chunk_ref(0xA2, RS_2_1, &[1, 2]),
        ]),
    );
    let held = [
        (0, frag(0xA1, 0)),
        (1, frag(0xA1, 1)),
        (3, frag(0xA1, 2)),
        (1, frag(0xA2, 0)),
        (2, frag(0xA2, 1)),
        (3, frag(0xA2, 2)),
    ];

    // Session 2: trusted records only.
    let trusted_id = upload("b2");
    meta.seed(mpu_key(&trusted_id), open_session());
    let trusted_parts = [
        part_key(&trusted_id, part_no(1)),
        part_key(&trusted_id, part_no(2)),
    ];
    meta.seed(
        trusted_parts[0].clone(),
        part(&[chunk_ref(0xB1, EcScheme::None, &[1])]),
    );
    meta.seed(
        trusted_parts[1].clone(),
        part(&[chunk_ref(0xB2, EcScheme::None, &[2])]),
    );
    let trusted = [(1, frag(0xB1, 0)), (2, frag(0xB2, 0))];

    for (dserver, fragment) in held.into_iter().chain(trusted) {
        place(&d, dserver, fragment);
    }

    let report = restore_pass(&meta, &d).await;

    // The premise: every record read, and nothing else found — so (b) sees the held record alone.
    assert!(
        report.unresolvable.is_empty()
            && report.pending_unreadable.is_empty()
            && report.dangling.is_empty()
            && report.misplaced.is_empty()
            && report.under_replicated.is_empty()
            && report.stranded_marked == 0,
        "the fixture must hold one finding, the untrusted record: {report:?}"
    );

    // (a) Named by the key the store spells (an ASCII key is its own name), once: it holds two
    // chunks, and a list per chunk would name it twice.
    let name = String::from_utf8(untrusted.clone()).expect("an ASCII key");
    let listed = debug_field(&report, "staged_untrusted")
        .unwrap_or_else(|| panic!("no untrusted staged record named: {report:?}"));
    for key in std::iter::once(mpu_key(&trusted_id)).chain(trusted_parts.iter().cloned()) {
        let key = String::from_utf8(key).expect("an ASCII key");
        assert!(!listed.contains(&key), "trusted {key} listed: {report:?}");
    }
    assert_eq!(listed, format!("[{name:?}]"), "{report:?}");
    // Six held fragments and session 2's two, all kept on staged grounds.
    assert_eq!(
        debug_field(&report, "staged_skipped").as_deref(),
        Some("8"),
        "{report:?}"
    );

    // (b) No human, and not a clean bill.
    assert!(!report.needs_human(), "{report:?}");
    assert!(
        !report.is_clean(),
        "clean over a chunk held on an untrusted record: {report:?}"
    );

    // (c) Nothing of the held chunks marked, and the record still on the audit seam.
    for (dserver, fragment) in held.into_iter().chain(trusted) {
        assert!(
            !meta.holds(&orphan_key(dserver, fragment)),
            "{fragment:?} on server {dserver} was marked although a staged record names its chunk"
        );
    }
    assert!(
        audited("untrusted-staged-record", &name),
        "{name} not named on the audit seam"
    );
}
