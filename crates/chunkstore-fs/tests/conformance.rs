//! Conformance tests for the filesystem `ChunkStore`.
//!
//! Round-trip and integrity assertions are written against the `ChunkStore`
//! trait surface (helpers over `&impl ChunkStore`) so they lift to a shared
//! suite when a second backend (S3) arrives. The corruption and id-guard tests
//! are filesystem-specific (they reach the bytes on disk). Filesystem I/O is
//! sync, so `pollster::block_on` drives the async methods deterministically.

#![forbid(unsafe_code)]

use bytes::Bytes;
use pollster::block_on;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_traits::{ChunkId, ChunkStore, FragmentId, Health};

fn fid(chunk: ChunkId, index: u16) -> FragmentId {
    FragmentId { chunk, index }
}

/// Build a valid v1 fragment carrying `payload`, whose header records `id`'s
/// chunk id and fragment index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

fn store() -> (FsChunkStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");
    (store, dir)
}

// ---- Round-trip (generic over any ChunkStore) ------------------------------

async fn round_trips(store: &impl ChunkStore, id: FragmentId, payload: &[u8]) {
    let frag = fragment(id, payload);
    store.put_fragment(id, frag.clone(), None).await.unwrap();
    let got = store.get_fragment(id).await.unwrap();
    assert_eq!(
        got.as_deref(),
        Some(frag.as_ref()),
        "fragment must round-trip byte-identical"
    );
}

#[test]
fn put_then_get_is_byte_identical() {
    block_on(async {
        let (s, _dir) = store();
        round_trips(&s, fid(1, 0), b"").await;
        round_trips(
            &s,
            fid(0xdead_beef_cafe_babe_0000_0000_1234_5678, 0),
            b"a small payload",
        )
        .await;
        // A non-zero fragment index (an erasure-coding stripe position).
        round_trips(&s, fid(42, 3), b"a parity fragment").await;
    });
}

#[test]
fn fragments_of_one_chunk_are_addressed_independently_by_index() {
    block_on(async {
        let (s, _dir) = store();
        let chunk = 0x5151;
        s.put_fragment(fid(chunk, 0), fragment(fid(chunk, 0), b"index zero"), None)
            .await
            .unwrap();
        s.put_fragment(fid(chunk, 1), fragment(fid(chunk, 1), b"index one"), None)
            .await
            .unwrap();

        let zero = s.get_fragment(fid(chunk, 0)).await.unwrap().unwrap();
        let one = s.get_fragment(fid(chunk, 1)).await.unwrap().unwrap();
        assert_ne!(
            zero, one,
            "different indices of one chunk are distinct fragments"
        );
        // An index the chunk does not have reads as not-found.
        assert!(s.get_fragment(fid(chunk, 2)).await.unwrap().is_none());
    });
}

#[test]
fn get_unknown_id_is_none() {
    block_on(async {
        let (s, _dir) = store();
        assert!(s.get_fragment(fid(99, 0)).await.unwrap().is_none());
    });
}

#[test]
fn health_is_healthy_when_open() {
    block_on(async {
        let (s, _dir) = store();
        assert_eq!(s.health().await.unwrap(), Health::Healthy);
    });
}

// ---- Enumerate + delete (M3, proposal 0005) --------------------------------

#[test]
fn list_and_delete_walk_the_store() {
    block_on(async {
        let (s, _dir) = store();
        // An empty store walks to nothing.
        assert!(s.list_fragments().await.unwrap().is_empty());

        let ids = [fid(0x11, 0), fid(0x11, 4), fid(0x22, 0)];
        for &id in &ids {
            s.put_fragment(id, fragment(id, b"walked"), None)
                .await
                .unwrap();
        }
        let listed: std::collections::HashSet<_> =
            s.list_fragments().await.unwrap().into_iter().collect();
        assert_eq!(
            listed,
            ids.into_iter().collect::<std::collections::HashSet<_>>(),
            "the directory walk recovers exactly the placed fragment ids"
        );

        // Delete one; it disappears from both get and the walk, siblings remain.
        s.delete_fragment(fid(0x11, 4)).await.unwrap();
        assert!(s.get_fragment(fid(0x11, 4)).await.unwrap().is_none());
        let listed: std::collections::HashSet<_> =
            s.list_fragments().await.unwrap().into_iter().collect();
        assert_eq!(
            listed,
            [fid(0x11, 0), fid(0x22, 0)]
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
        );

        // Deleting an absent fragment is an idempotent Ok(()).
        s.delete_fragment(fid(0x11, 4)).await.unwrap();
        s.delete_fragment(fid(0xdead, 9)).await.unwrap();
    });
}

#[test]
fn list_skips_foreign_and_temp_entries() {
    block_on(async {
        let (s, dir) = store();
        let id = fid(0x33, 0);
        s.put_fragment(id, fragment(id, b"real"), None)
            .await
            .unwrap();

        // A leftover `.tmp` (an interrupted put) and a foreign directory/file
        // must not surface as phantom fragments — the walk parses names strictly.
        let chunk_dir = dir.path().join(format!("{:032x}", id.chunk));
        std::fs::write(chunk_dir.join("00001.tmp"), b"interrupted").unwrap();
        std::fs::write(chunk_dir.join("notes.txt"), b"foreign").unwrap();
        std::fs::create_dir_all(dir.path().join("not-a-chunk")).unwrap();

        assert_eq!(
            s.list_fragments().await.unwrap(),
            vec![id],
            "only the valid .frag under a 32-hex chunk dir is listed"
        );
    });
}

// ---- Integrity (filesystem-specific) ---------------------------------------

#[test]
fn corruption_is_detected_on_read() {
    block_on(async {
        let (s, dir) = store();
        let id = fid(7, 0);
        s.put_fragment(id, fragment(id, b"important"), None)
            .await
            .unwrap();

        // Flip a payload byte directly on disk, behind the store's back.
        let path = fragment_path(dir.path(), id);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1; // a payload-checksum byte
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            s.get_fragment(id).await.is_err(),
            "a corrupted fragment must not be returned"
        );
    });
}

#[test]
fn put_rejects_non_fragment_bytes() {
    block_on(async {
        let (s, _dir) = store();
        let err = s
            .put_fragment(fid(1, 0), Bytes::from_static(b"not a fragment"), None)
            .await;
        assert!(err.is_err(), "garbage must be rejected, not stored");
        assert!(s.get_fragment(fid(1, 0)).await.unwrap().is_none());
    });
}

#[test]
fn put_rejects_chunk_or_index_mismatch() {
    block_on(async {
        let (s, _dir) = store();
        // Header chunk id differs from the key's chunk.
        assert!(
            s.put_fragment(fid(0x2222, 0), fragment(fid(0x1111, 0), b"payload"), None)
                .await
                .is_err(),
            "a fragment must be filed under the chunk its header records"
        );
        // Header index differs from the key's index.
        assert!(
            s.put_fragment(fid(0x1111, 1), fragment(fid(0x1111, 0), b"payload"), None)
                .await
                .is_err(),
            "a fragment must be filed under the index its header records"
        );
    });
}

// ---- Issue #638 — the fragment-write authorization deadline (`W_write`) ----
//
// `chunkstore-fs` must honour the identical contract a networked D server does
// (leg E, `docs/design/proposals/draft/0016-multipart-commit-protocol.md:1551-1576`):
// a caller must not get a weaker guarantee by holding a local store. Asserted
// directly against the real `FsChunkStore::put_fragment`, "cheaply, in the same
// file" per the brief — this is the local-store peer of
// `crates/chunkstore-grpc/tests/write_deadline.rs`'s wire-level legs.
//
// Time is injected through the store's `Clock` seam (ADR-0024) rather than slept
// through, so every leg is deterministic and needs no wall clock in the test.

/// A store whose deadline judgments run on `clock`, so the test owns time.
fn store_with_clock<C: wyrd_testkit::Clock>(clock: C) -> (FsChunkStore<C>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");
    (store, dir)
}

/// Scratch (`*.tmp`) files still lying under the store root — a refused write must
/// leave none, exactly as a failed write does.
fn scratch_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    for chunk_dir in std::fs::read_dir(root).expect("read root").flatten() {
        if !chunk_dir.file_type().expect("file type").is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(chunk_dir.path())
            .expect("read chunk dir")
            .flatten()
        {
            if entry.path().extension().and_then(|e| e.to_str()) == Some("tmp") {
                found.push(entry.path());
            }
        }
    }
    found
}

#[test]
fn put_refuses_an_expired_deadline_at_admission_without_touching_the_disk() {
    block_on(async {
        // The store's clock stands at 10_000; the write was authorized until 9_000.
        let (s, dir) = store_with_clock(wyrd_testkit::ManualClock::new(10_000));
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_0001, 0);
        let frag = fragment(id, b"authorized too long ago");

        let err = s
            .put_fragment(id, frag, Some(9_000))
            .await
            .expect_err("a write whose deadline already elapsed must be refused");
        assert!(
            wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "must classify as WriteDeadlineExpired, not a generic/backend fault: {err}"
        );
        assert!(
            s.get_fragment(id).await.unwrap().is_none(),
            "a refused write must not be stored"
        );
        // A write that is doomed on arrival costs the D server *nothing*: no chunk
        // directory and no scratch. That is what the entry check buys over letting the
        // publication-point verdict do all the work.
        assert!(
            !dir.path().join(format!("{:032x}", id.chunk)).exists(),
            "an already-expired write must be refused before any disk work"
        );
        assert!(
            scratch_files(dir.path()).is_empty(),
            "a refused write must leave no scratch behind: {:?}",
            scratch_files(dir.path())
        );
    });
}

#[test]
fn put_within_its_deadline_is_unaffected() {
    block_on(async {
        let (s, _dir) = store_with_clock(wyrd_testkit::ManualClock::new(10_000));
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_0002, 0);
        let frag = fragment(id, b"comfortably inside its window");

        s.put_fragment(id, frag.clone(), Some(70_000))
            .await
            .expect("a live write comfortably inside its deadline must succeed");
        assert_eq!(
            s.get_fragment(id).await.unwrap().as_deref(),
            Some(frag.as_ref())
        );
    });
}

/// A [`wyrd_testkit::Clock`] anchored to the store's **own on-disk progress**, not to a
/// read count (issue #638).
///
/// `FsChunkStore` writes each fragment to a private scratch file (`<index>.<seq>.tmp`)
/// and then publishes it with a single `rename` onto `<index>.frag`. This clock *looks at
/// the store's own chunk directory* on every read and answers:
///
/// * `live` — while the chunk directory is empty, i.e. the store has not yet written the
///   fragment's bytes;
/// * `late` — from the first read taken with the scratch **or** the published `.frag`
///   present, i.e. from the moment the data write completed onwards.
///
/// That makes "time elapsed during the write" a fact about the store's on-disk state
/// rather than an assumption about which read is which — the distinction a scripted-reads
/// clock cannot make, since it can only *call* some read the publication point. A store
/// that judges the deadline only on entry never takes a reading with anything on disk at
/// all: it publishes the write, and the leg below goes red. A store that judges *after*
/// the rename gets its `late` reading too — from the `.frag` — but the record then shows a
/// verdict taken with the fragment published, which the leg below refuses.
///
/// Every reading is recorded with what the directory looked like when it was taken, and
/// that record is the evidence the test asserts on — including the *negative* half, that
/// no verdict was ever rendered with the fragment already published.
#[derive(Clone)]
struct AtPublicationPoint {
    inner: std::sync::Arc<AtPublicationPointInner>,
}

struct AtPublicationPointInner {
    chunk_dir: std::path::PathBuf,
    live: u64,
    late: u64,
    reads: std::sync::Mutex<Vec<ClockRead>>,
}

#[derive(Debug, Clone, Copy)]
struct ClockRead {
    answer: u64,
    scratch_present: bool,
    fragment_present: bool,
}

impl AtPublicationPoint {
    fn new(chunk_dir: std::path::PathBuf, live: u64, late: u64) -> Self {
        Self {
            inner: std::sync::Arc::new(AtPublicationPointInner {
                chunk_dir,
                live,
                late,
                reads: std::sync::Mutex::new(Vec::new()),
            }),
        }
    }

    fn reads(&self) -> Vec<ClockRead> {
        self.inner.reads.lock().expect("clock reads").clone()
    }
}

impl wyrd_testkit::Clock for AtPublicationPoint {
    fn now_millis(&self) -> u64 {
        let (mut scratch_present, mut fragment_present) = (false, false);
        if let Ok(entries) = std::fs::read_dir(&self.inner.chunk_dir) {
            for entry in entries.flatten() {
                match entry.path().extension().and_then(|e| e.to_str()) {
                    Some("tmp") => scratch_present = true,
                    Some("frag") => fragment_present = true,
                    _ => {}
                }
            }
        }
        let answer = if scratch_present || fragment_present {
            self.inner.late
        } else {
            self.inner.live
        };
        self.inner
            .reads
            .lock()
            .expect("clock reads")
            .push(ClockRead {
                answer,
                scratch_present,
                fragment_present,
            });
        answer
    }
}

/// **The bound itself** (issue #638, the enforcement claim of 0016 decision 5), and the
/// two things that have to be true of it at once:
///
/// 1. **The verdict is late enough to be a bound.** It must fall *after* everything that
///    can consume unbounded time — the blocking pool's queue, the chunk-directory
///    creation, the fragment's data write — or it bounds only when the store *accepted*
///    the write. [`AtPublicationPoint`] reports the write live until the store's own bytes
///    appear on disk, and past its deadline from that instant on: a store that judges the
///    deadline only on entry never takes a reading with anything on disk, publishes the
///    write, and this leg goes red.
/// 2. **The verdict is early enough to be honest.** It must fall *before* the publishing
///    rename, because [`wyrd_traits::WriteDeadlineExpired`] asserts the write did **not**
///    take effect. Judging after the rename would mean either keeping the late bytes
///    (0016 outcome (a), the leak) or unlinking them — and the unlink is not atomic with
///    the rename, so a crash in between leaves exactly the bytes the refusal denies. The
///    clock records what the directory looked like at every reading, so the test can
///    assert the negative directly: **no verdict was ever rendered with the fragment
///    published**. There is therefore no instant — and no crash point — at which the
///    store had decided "expired" while the bytes were readable.
///
/// The residue this design does *not* remove is the `rename` syscall's own duration, which
/// no filesystem lets a caller make conditional; that is what `δ_clock` absorbs in
/// `G_orphan > W_write + δ_clock` (`0016:1478`).
#[test]
fn the_deadline_verdict_falls_after_the_bytes_are_written_and_before_publication() {
    block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_0003, 0);
        let clock =
            AtPublicationPoint::new(dir.path().join(format!("{:032x}", id.chunk)), 9_500, 10_500);
        let s = FsChunkStore::open_with_clock(dir.path(), clock.clone()).expect("open store");
        let frag = fragment(id, b"live on arrival, late once its bytes were written");

        let err = s.put_fragment(id, frag, Some(10_000)).await.expect_err(
            "a write whose deadline elapses while the store is writing its bytes must be \
             refused at the publication point — a check taken only on entry bounds when \
             the write was accepted, not when it takes effect",
        );
        assert!(
            wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "the refusal must be the typed deadline class, not an I/O error: {err}"
        );

        let reads = clock.reads();
        // (1) the verdict is downstream of the data write: some reading was taken with the
        // fragment's bytes already on disk (as scratch, or — for a build that judges too
        // late — as the published file).
        assert!(
            reads
                .iter()
                .any(|r| (r.scratch_present || r.fragment_present) && r.answer == 10_500),
            "the store must judge the deadline with the fragment's bytes already on disk: \
             no clock read was taken after the data write — the reads were {reads:?}"
        );
        // (2) …and upstream of the publication. This is the crash-safety property: a
        // refusal that could be rendered with the fragment already published would have a
        // window in which a crash leaves the very bytes the refusal denies.
        assert!(
            reads.iter().all(|r| !r.fragment_present),
            "no clock read may be taken with the fragment already published — a verdict \
             on the far side of the rename can only be honoured by retracting, which is \
             not atomic with the publication: {reads:?}"
        );

        assert!(
            s.get_fragment(id).await.unwrap().is_none(),
            "the refused write must not be observable (0016 outcome (a) is exactly the \
             fragment that is stored but unevidenced)"
        );
        assert!(
            !fragment_path(dir.path(), id).exists(),
            "and nothing was published to disk at all — not published-then-removed"
        );
        assert!(
            scratch_files(dir.path()).is_empty(),
            "no scratch litter either: {:?}",
            scratch_files(dir.path())
        );
        // **And the chunk directory is still standing** — deliberately. It is the one piece
        // of state a refusal shares with other writers, and a rollback that removed it could
        // strip it from under a live writer that has just created it and not yet written its
        // bytes: a live write killed by an expired one. Leaving it costs an empty directory
        // (inert: `get_fragment` reads `None` through it, `list_fragments` cannot see it, and
        // `open` collects it — see the reopen below); removing it costs a race that no number
        // of create retries closes, because N retries lose to N+1 racing refusals. So this
        // assertion is not incidental tidiness — it is the invariant that makes concurrent
        // refusal harmless, and it is asserted here, deterministically, rather than left to
        // the thread-race test in `tests/concurrent_put.rs` to catch.
        let chunk_dir = dir.path().join(format!("{:032x}", id.chunk));
        assert!(
            chunk_dir.is_dir(),
            "a refusal must not remove the shared chunk directory"
        );
        assert_eq!(
            std::fs::read_dir(&chunk_dir)
                .expect("read chunk dir")
                .count(),
            0,
            "and what it leaves there is nothing at all — the directory is empty"
        );

        // The crash-restart observable: re-opening the store — the recovery path a D
        // server runs after an unclean exit — finds nothing to resolve, because a refused
        // write leaves no durable trace whose interpretation could depend on timing. The
        // empty directory goes here, at the one point where this store has no write in
        // flight, so its collection cannot race anybody.
        let reopened = FsChunkStore::open(dir.path()).expect("reopen store");
        assert!(
            reopened.get_fragment(id).await.unwrap().is_none(),
            "a refused write must stay absent across a restart"
        );
        assert!(
            reopened.list_fragments().await.unwrap().is_empty(),
            "and the store holds nothing at all"
        );
        assert!(
            !chunk_dir.exists(),
            "and the empty directory the refusal left is collected at open, so late writes \
             to fresh chunk ids cannot accumulate them across restarts"
        );
    });
}

/// An expiring write **never disturbs a fragment somebody else already published**
/// (issue #638). A fragment an earlier write published — a live write from another writer,
/// or an idempotent retry of this one — is explained by an authorization that was *not*
/// late, so a straggler for the same id must leave it exactly where it is.
///
/// This is a property of the enforcement *ordering*, not of a guard bolted onto a
/// retraction: because the store renders its verdict before it publishes, a refused write
/// never touches the published name at all. The alternative design — publish, then judge,
/// then `remove_file` — has to guess whether the file it is about to delete is its own,
/// and a same-id write that published in between is deleted with it: a deadline mechanism
/// turned into a data-loss mechanism.
///
/// Five readings — three for the live write (entry, publication point, publication
/// *completed*) and two for the straggler, which is refused at its publication point before
/// a third is taken. Only the last is past a deadline; the `remaining()` assertion pins that
/// the store took exactly those reads, so dropping any of the three checks is caught here
/// as well.
#[test]
fn an_expiring_write_never_disturbs_an_already_published_fragment() {
    block_on(async {
        let clock = wyrd_testkit::SteppedClock::new([9_500, 9_500, 9_500, 9_500, 10_500]);
        let (s, dir) = store_with_clock(clock.clone());
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_0005, 0);
        let frag = fragment(id, b"published by a write that was in time");

        // First, a write that lands well inside its window (the acknowledged fragment).
        s.put_fragment(id, frag.clone(), Some(9_900))
            .await
            .expect("the first write is live and must be stored");
        assert_eq!(
            s.get_fragment(id).await.unwrap().as_deref(),
            Some(frag.as_ref()),
            "precondition: the fragment is published"
        );

        // Now a straggler for the same id: live on arrival, expired by the time the store
        // reaches its publication point.
        let err = s
            .put_fragment(id, frag.clone(), Some(10_000))
            .await
            .expect_err("the straggler is past its deadline at the publication point");
        assert!(
            wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "{err}"
        );
        assert_eq!(
            clock.remaining(),
            0,
            "a published put takes three readings — entry, the publication point, and the \
             verification that publication completed in time — and a refused one takes the \
             first two; an unconsumed reading means one of those checks is gone"
        );

        assert_eq!(
            s.get_fragment(id).await.unwrap().as_deref(),
            Some(frag.as_ref()),
            "the fragment the FIRST, live write published must still be there — an \
             expiring write refuses itself, it never removes somebody else's bytes"
        );
        assert!(
            scratch_files(dir.path()).is_empty(),
            "and it leaves no scratch: {:?}",
            scratch_files(dir.path())
        );
    });
}

/// The control for the leg above (issue #638 leg F, the "genuine backend fault" half): a
/// **real** I/O failure from the same production store — the chunk directory's path is
/// occupied by a *file*, so the scratch write fails `ENOTDIR` — must NOT read as a deadline
/// refusal. Without this control, "the refusal is classifiable" would rest on the deadline
/// side alone, and a store that reported every failure as a deadline refusal would pass.
#[test]
fn a_genuine_backend_fault_is_not_a_deadline_refusal() {
    block_on(async {
        let (s, dir) = store_with_clock(wyrd_testkit::ManualClock::new(10_000));
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_0004, 0);
        // Occupy the chunk directory's path with a file: every write beneath it now fails
        // with a real `ENOTDIR` from the filesystem, on any platform and as any user.
        std::fs::write(
            dir.path().join(format!("{:032x}", id.chunk)),
            b"not a directory",
        )
        .expect("plant the obstruction");

        let err = s
            .put_fragment(id, fragment(id, b"doomed by the disk"), Some(70_000))
            .await
            .expect_err("a genuine I/O failure must surface as an error");
        assert!(
            !wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "a broken backend must never be reported as 'refused, too late' — a caller \
             would stop re-authorizing and start ignoring a real fault: {err}"
        );
        assert!(
            err.downcast_ref::<std::io::Error>().is_some(),
            "it stays the backend's own I/O error: {err}"
        );
        // And the deadline refusal is not an I/O error either — the two are disjoint.
        let refusal = s
            .put_fragment(id, fragment(id, b"too late"), Some(9_000))
            .await
            .expect_err("an expired deadline is refused");
        assert!(wyrd_traits::is_write_deadline_expired(refusal.as_ref()));
        assert!(
            refusal.downcast_ref::<std::io::Error>().is_none(),
            "a deadline refusal is an expected outcome, not a backend fault: {refusal}"
        );
    });
}

/// A [`wyrd_testkit::Clock`] that steps past the deadline exactly when the store's
/// **publication completes** — i.e. when `<index>.frag` appears (issue #638).
///
/// This models the one interval the pre-publication verdict cannot cover: `rename(2)` is a
/// syscall the store cannot make conditional, cannot cancel and cannot bound, and on a slow
/// or hung device (the NAS profile this store also serves) it can straddle the deadline.
/// Anchoring "time passed" to the published file's *existence* advances the clock **across
/// the publication syscall** rather than merely before it — a scripted clock can only *call*
/// some read the publication instant, which is precisely how a store that never re-reads
/// after publishing slips through.
///
/// Every read is `live` while the fragment is unpublished (so entry and the pre-publication
/// verdict both pass) and `late` from the first read taken with the fragment present.
#[derive(Clone)]
struct AtPublicationCompletion {
    fragment_path: std::path::PathBuf,
    live: u64,
    late: u64,
}

impl wyrd_testkit::Clock for AtPublicationCompletion {
    fn now_millis(&self) -> u64 {
        if self.fragment_path.exists() {
            self.late
        } else {
            self.live
        }
    }
}

/// **`Ok(())` means the fragment was published before its deadline — checked, not assumed**
/// (issue #638; `0016:1557-1564`, "the bound is only real if the D server enforces it too").
///
/// The pre-publication verdict establishes that publication had not yet *begun* too late.
/// It cannot establish that publication *completed* in time, because the publishing step's
/// own latency is not the store's to control. So the store re-reads its clock immediately
/// after the rename and, unless that reading is still inside the window, declines to
/// acknowledge the write. Two halves, asserted together:
///
/// 1. **A publication the store could not time is never acknowledged.** A store that
///    returns `Ok(())` here would be claiming an in-window landing it never verified — the
///    same "bounds acceptance, not effect" defect one layer down from the caller-side
///    timeout 0016 rejects.
/// 2. **…and it is reported for exactly what it is.** The verdict is
///    `WriteEffect::Unknown`, classifying `Indeterminate` — not the clean refusal's
///    definite "nothing landed", which would be a lie over durable state, and *not* a
///    claim that the publication was late, which the evidence does not support: a clock
///    read timestamps the read, so a `rename` that returned comfortably in time followed by
///    a descheduled thread produces this same reading. The store also does not unlink
///    whatever landed: the retraction is not atomic with the publication and it deletes by
///    path, so it can destroy a concurrent same-id writer's acknowledged fragment. This
///    test pins both the honest verdict and the untouched bytes, so a later "just delete
///    it" would have to change an assertion — and argue for it.
///
/// The control in the same run is the write whose deadline is beyond even the `late`
/// reading: it must still be acknowledged, or the leg would pass on a store that fails every
/// publication.
#[test]
fn a_publication_the_store_could_not_verify_is_never_acknowledged() {
    block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let straggler = fid(0xf00d_0000_0000_0000_0000_0000_0000_0006, 0);
        let clock = AtPublicationCompletion {
            fragment_path: fragment_path(dir.path(), straggler),
            live: 9_500,
            late: 10_500,
        };
        let s = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");
        let frag = fragment(straggler, b"published, but not before the deadline");

        let err = s
            .put_fragment(straggler, frag.clone(), Some(10_000))
            .await
            .expect_err(
                "the store could not verify the publication landed in window, so it must \
                 NOT acknowledge the write — `Ok(())` would assert an in-window landing \
                 that was never checked",
            );
        let outcome = wyrd_traits::write_deadline_outcome(err.as_ref())
            .expect("an unverified publication is a deadline outcome, not a generic fault");
        assert_eq!(
            outcome.effect,
            wyrd_traits::WriteEffect::Unknown,
            "and not the clean refusal: bytes may have landed, so saying `NotApplied` \
             would be a lie over durable state: {err}"
        );
        assert!(
            outcome.effect.may_have_landed(),
            "the caller must be able to read off that durable bytes may exist"
        );
        assert!(
            !err.to_string().contains("landed late"),
            "and it must not over-claim in the other direction either: the store observed \
             a reading, not the syscall's completion time: {err}"
        );
        assert_eq!(
            wyrd_traits::classify(err.as_ref()),
            wyrd_traits::ErrorClass::Indeterminate,
            "neither terminal (something did happen) nor transient (a blind retry is not \
             the remedy): {err}"
        );

        // The truthful half: the bytes really are there. Asserted, not tolerated — a store
        // that "fixed" this by unlinking would be trading a reported late write for the
        // chance of deleting a concurrent writer's acknowledged fragment.
        assert_eq!(
            s.get_fragment(straggler).await.unwrap().as_deref(),
            Some(frag.as_ref()),
            "`Unknown` promises nothing about the bytes either way, so the store must not \
             have unlinked them behind the caller's back"
        );
        assert!(
            scratch_files(dir.path()).is_empty(),
            "and the write still leaves no scratch: {:?}",
            scratch_files(dir.path())
        );

        // Control: same store, same clock, a deadline beyond even the `late` reading.
        let live = fid(0xf00d_0000_0000_0000_0000_0000_0000_0007, 0);
        let live_frag = fragment(live, b"comfortably inside its window at both ends");
        s.put_fragment(live, live_frag.clone(), Some(70_000))
            .await
            .expect("a write whose deadline outlives its publication is acknowledged");
        assert_eq!(
            s.get_fragment(live).await.unwrap().as_deref(),
            Some(live_frag.as_ref())
        );
    });
}

/// A [`wyrd_testkit::Clock`] that expires the write once its bytes are on disk **and, at the
/// same instant, makes the store's rollback impossible** (issue #638).
///
/// The store's refusal path removes its scratch file. This clock takes that scratch away and
/// leaves a **non-empty directory at the same path**, so `remove_file` cannot succeed — for
/// any uid, on any platform, without depending on permissions (the same standard as this
/// file's `ENOTDIR` control, which plants a file where a directory belongs). It is the
/// filesystem's way of saying what a read-only remount, a denied unlink or an exhausted
/// inode table say in production: *the store cannot put itself back*.
#[derive(Clone)]
struct ExpiresAndBlocksRollback {
    chunk_dir: std::path::PathBuf,
    live: u64,
    late: u64,
}

impl wyrd_testkit::Clock for ExpiresAndBlocksRollback {
    fn now_millis(&self) -> u64 {
        let Some(scratch) = std::fs::read_dir(&self.chunk_dir).ok().and_then(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .find(|p| p.extension().and_then(|e| e.to_str()) == Some("tmp"))
        }) else {
            return self.live;
        };
        if scratch.is_file() {
            std::fs::remove_file(&scratch).expect("take the store's scratch away");
            std::fs::create_dir(&scratch).expect("put a directory in its place");
            std::fs::write(scratch.join("occupant"), b"not removable by unlink")
                .expect("and make it non-empty");
        }
        self.late
    }
}

/// **A refusal the store could not roll back is a backend fault, not a clean refusal**
/// (issue #638).
///
/// `WriteEffect::NotApplied` is an unconditional promise — nothing of this write is on the
/// store — and a caller acts on it by re-authorizing and forgetting the attempt. If the
/// rollback failed and the store said `NotApplied` anyway, the residue would be invisible
/// exactly to the party that could clean it up: silent success over an operation that did
/// not happen, the rubric's *Absent or unsupported entries* class (AGENTS.md § Recurring
/// defect classes), and a slow storage leak on any D server whose disk is misbehaving.
///
/// So the store reports the **fault** instead, and this test asserts all three parts of that:
/// the error is not classified as a deadline refusal, it names the failure a caller can act
/// on, and the residue it warned about is genuinely there.
#[test]
fn a_refusal_the_store_could_not_roll_back_is_reported_as_a_backend_fault() {
    block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_0008, 0);
        let clock = ExpiresAndBlocksRollback {
            chunk_dir: dir.path().join(format!("{:032x}", id.chunk)),
            live: 9_500,
            late: 10_500,
        };
        let s = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");

        let err = s
            .put_fragment(
                id,
                fragment(id, b"too late, and un-rollbackable"),
                Some(10_000),
            )
            .await
            .expect_err("the write is past its deadline at the publication point");

        assert!(
            !wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "a refusal the store could not complete must NOT be reported as the definite \
             deadline verdict — that verdict promises the store is untouched: {err}"
        );
        match err.downcast_ref::<wyrd_chunkstore_fs::FsChunkStoreError>() {
            Some(wyrd_chunkstore_fs::FsChunkStoreError::RefusalNotRolledBack {
                id: reported,
                ..
            }) => assert_eq!(*reported, id, "the fault names the write it belongs to"),
            other => panic!("expected a rollback fault naming the cause, got {other:?}: {err}"),
        }
        assert!(
            std::error::Error::source(err.as_ref()).is_some(),
            "the underlying I/O error stays reachable for an operator: {err}"
        );
        assert_eq!(
            wyrd_traits::classify(err.as_ref()),
            wyrd_traits::ErrorClass::Terminal,
            "it is a backend fault, and an unclassified fault is terminal by the seam's \
             fail-safe default: {err}"
        );

        // The report is true: the residue really is on disk. (This is the assertion that
        // fails if the store goes back to discarding the cleanup error — it would then
        // return the clean refusal above, and this leg's first assertion catches it, but
        // the residue is what makes the finding *matter*.)
        assert!(
            !scratch_files(dir.path()).is_empty()
                || dir
                    .path()
                    .join(format!("{:032x}", id.chunk))
                    .read_dir()
                    .map(|mut e| e.next().is_some())
                    .unwrap_or(false),
            "the store said it could not restore itself, so something must still be there"
        );
        // …and, whatever is left, no fragment was published.
        assert!(
            s.get_fragment(id).await.unwrap().is_none(),
            "the refused write was still never published"
        );
    });
}

/// The scratch file `FsChunkStore` is writing under `chunk_dir`, if any.
fn scratch_in(chunk_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    std::fs::read_dir(chunk_dir).ok().and_then(|entries| {
        entries
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some("tmp"))
    })
}

/// A [`wyrd_testkit::Clock`] that expires the write at its publication point and, at that
/// exact instant, performs **one** injection on the store's own chunk directory (issue
/// #638).
///
/// The injection lands in the window between the store rendering its verdict and running its
/// rollback — the window a *concurrent* actor genuinely occupies on a live D server (another
/// writer of the same chunk, another straggler's rollback, an operator's `rmdir`). Anchoring
/// it to the scratch file's existence, rather than to a read count, means it fires at the
/// store's own publication point wherever that is, so these legs keep testing the rollback
/// and not a guess about the order of clock reads.
#[derive(Clone)]
struct AtVerdictInject {
    chunk_dir: std::path::PathBuf,
    live: u64,
    late: u64,
    inject: std::sync::Arc<dyn Fn(&std::path::Path) + Send + Sync>,
    fired: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl AtVerdictInject {
    fn new(
        chunk_dir: std::path::PathBuf,
        inject: impl Fn(&std::path::Path) + Send + Sync + 'static,
    ) -> Self {
        Self {
            chunk_dir,
            live: 9_500,
            late: 10_500,
            inject: std::sync::Arc::new(inject),
            fired: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

impl wyrd_testkit::Clock for AtVerdictInject {
    fn now_millis(&self) -> u64 {
        if scratch_in(&self.chunk_dir).is_none() {
            return self.live;
        }
        if !self.fired.swap(true, std::sync::atomic::Ordering::Relaxed) {
            (self.inject)(&self.chunk_dir);
        }
        self.late
    }
}

/// **A refusal whose residue somebody else already removed is still the clean verdict**
/// (issue #638).
///
/// The rollback's job is to establish a *state* — nothing of this write on the store — not
/// to perform a *syscall*. So a scratch file that has already gone (another process's
/// `reap_write_residue`, an operator, a racing cleanup) is the state the refusal wanted, and
/// treating the `unlink` failure as a backend fault would report a healthy store as broken
/// and push a straggling writer onto the wrong branch: retry-the-backend instead of
/// re-authorize.
///
/// This is the arm the rollback-fault leg cannot reach — that one asserts the *opposite*
/// answer for a genuine failure — so both directions are pinned and neither can be collapsed
/// into the other.
#[test]
fn a_refusal_whose_scratch_already_vanished_is_still_a_clean_refusal() {
    block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_0009, 0);
        let clock = AtVerdictInject::new(dir.path().join(format!("{:032x}", id.chunk)), |chunk| {
            // Somebody else unlinks the store's scratch in the verdict/rollback window.
            if let Some(scratch) = scratch_in(chunk) {
                std::fs::remove_file(scratch).expect("take the scratch away");
            }
        });
        let s = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");

        let err = s
            .put_fragment(
                id,
                fragment(id, b"late, and already tidied up"),
                Some(10_000),
            )
            .await
            .expect_err("the write is past its deadline at the publication point");

        assert!(
            wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "an already-absent scratch is the state the refusal wanted, so this is the \
             clean deadline verdict — not a backend fault: {err}"
        );
        assert_eq!(
            wyrd_traits::write_deadline_outcome(err.as_ref())
                .expect("typed outcome")
                .effect,
            wyrd_traits::WriteEffect::NotApplied,
        );
        assert!(s.get_fragment(id).await.unwrap().is_none());
        assert!(
            scratch_files(dir.path()).is_empty(),
            "and no scratch is left under the store: {:?}",
            scratch_files(dir.path())
        );
        assert!(
            s.list_fragments().await.unwrap().is_empty(),
            "the store holds no fragment — the empty chunk directory the refusal leaves \
             behind is not one, and is collected at the next open"
        );
    });
}

/// **A refusal never takes a chunk directory a concurrent write is using** (issue #638).
///
/// The chunk directory is *shared* state: every fragment of one chunk lives in it, and
/// `create_dir_all` succeeds on a directory that already exists, so no writer can ever
/// establish that it is "the one that created it" — a flag recording that belief over-claims
/// exactly when a racer got there first, and an unconditional `remove_dir` (atomic in the
/// directory's emptiness, so it can never take somebody's *bytes*) still strips the directory
/// from under a live writer between its create and its data write. The rollback therefore
/// removes **only this write's private scratch** and leaves every shared path alone, which is
/// what makes the hazard structurally impossible rather than merely improbable.
///
/// Here a sibling fragment of the same chunk is published in the verdict/rollback window —
/// the window a concurrent writer genuinely occupies. The refusal must come back clean, the
/// sibling must survive, and its directory must still be standing.
#[test]
fn a_refusal_leaves_a_chunk_directory_a_concurrent_write_is_using() {
    block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let straggler = fid(0xf00d_0000_0000_0000_0000_0000_0000_000a, 0);
        let sibling = fid(straggler.chunk, 1);
        let sibling_bytes = fragment(sibling, b"a live write of another fragment");
        let planted = sibling_bytes.clone();
        let chunk_dir = dir.path().join(format!("{:032x}", straggler.chunk));
        let sibling_path = fragment_path(dir.path(), sibling);
        let clock = AtVerdictInject::new(chunk_dir.clone(), move |_| {
            // A concurrent write of a *different* fragment of the same chunk publishes
            // while the straggler is between its verdict and its rollback.
            std::fs::write(&sibling_path, &planted).expect("publish the sibling fragment");
        });
        let s = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");

        let err = s
            .put_fragment(straggler, fragment(straggler, b"too late"), Some(10_000))
            .await
            .expect_err("the straggler is past its deadline at the publication point");

        assert!(
            wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "an occupied chunk directory is not a rollback failure — nothing of *this* \
             write remains: {err}"
        );
        assert!(
            s.get_fragment(straggler).await.unwrap().is_none(),
            "the refused write is still not published"
        );
        assert_eq!(
            s.get_fragment(sibling).await.unwrap().as_deref(),
            Some(sibling_bytes.as_ref()),
            "and the concurrent write's fragment is untouched — a refusal that removed the \
             shared directory would have destroyed it"
        );
        assert!(chunk_dir.is_dir(), "its directory is still standing");
        assert!(
            scratch_files(dir.path()).is_empty(),
            "while the straggler's own scratch is gone: {:?}",
            scratch_files(dir.path())
        );
    });
}

/// **A refusal leaves the shared chunk directory exactly as it found it — even when it is
/// the write that created it** (issue #638).
///
/// This is the deterministic statement of the property the thread race in
/// `tests/concurrent_put.rs` can only amplify: the *only* path a refusal removes is its own
/// private scratch. A rollback that also removed the (now empty) chunk directory would race
/// every live writer that has created that directory and not yet written its bytes, and the
/// retry-the-create mitigation is a margin rather than a fix — N create retries lose to N+1
/// staggered refusals. Removing the shared write removes the race outright, and this leg
/// fails the moment anybody puts it back.
///
/// It also pins the *cost* honestly, in the same test: what is left is an empty directory,
/// and the store's own listing does not see it, so no reader can tell it from the pre-write
/// state through the seam. Where it is collected is the leg below.
#[test]
fn a_refusal_never_removes_the_chunk_directory_even_one_it_created() {
    block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let id = fid(0xf00d_0000_0000_0000_0000_0000_0000_000b, 0);
        let chunk_dir = dir.path().join(format!("{:032x}", id.chunk));
        // The store has never seen this chunk: the directory does not exist, so *this* write
        // is the one that creates it — the case a "remove what I created" rollback would
        // claim, and the case in which a racing live writer is at its most exposed.
        assert!(!chunk_dir.exists(), "the chunk directory starts absent");
        let clock = AtPublicationPoint::new(chunk_dir.clone(), 9_500, 10_500);
        let s = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");

        let err = s
            .put_fragment(
                id,
                fragment(id, b"too late, on a fresh chunk"),
                Some(10_000),
            )
            .await
            .expect_err("the write is past its deadline at the publication point");

        assert!(
            wyrd_traits::is_write_deadline_expired(err.as_ref()),
            "it is the clean deadline verdict: {err}"
        );
        assert!(
            chunk_dir.is_dir(),
            "and the shared chunk directory it created is still standing — a refusal that \
             removed it could strip it from under a concurrent live writer that had just \
             created it, failing a live write on behalf of an expired one"
        );
        assert_eq!(
            std::fs::read_dir(&chunk_dir)
                .expect("read chunk dir")
                .count(),
            0,
            "while everything the refusal itself put there is gone"
        );
        assert!(
            s.get_fragment(id).await.unwrap().is_none()
                && s.list_fragments().await.unwrap().is_empty(),
            "and through the seam the store is indistinguishable from never having seen the \
             write"
        );
    });
}

/// A [`wyrd_testkit::Clock`] that reports every write live until *its* bytes reach the disk
/// anywhere under the store root, then past its deadline — the multi-chunk sibling of
/// [`AtPublicationPoint`], which watches one chunk directory (issue #638).
#[derive(Clone)]
struct AtAnyScratch {
    root: std::path::PathBuf,
    live: u64,
    late: u64,
}

impl wyrd_testkit::Clock for AtAnyScratch {
    fn now_millis(&self) -> u64 {
        if scratch_files(&self.root).is_empty() {
            self.live
        } else {
            self.late
        }
    }
}

/// **The empty directories late writes leave are collected — at the one point where
/// collecting them cannot race a live write** (issue #638).
///
/// A write to a chunk the store has never seen creates its chunk directory before it writes
/// the fragment's bytes, so a refusal that removes only its scratch — which is what a refusal
/// must do, since the directory is shared — leaves one empty directory behind *per late
/// write*. That residue is inert (no fragment: `list_fragments` parses `.frag`, `get_fragment`
/// reads `None` through it) but it must still be bounded, or a straggling writer retrying
/// against a D server whose grace has elapsed litters the root indefinitely.
///
/// It is collected at **`open`** — where one D server owns the root (ADR-0034, Model A) and no
/// write on the store is in flight, so the collection has no concurrent creator to race —
/// and by `remove_dir` alone, so a chunk that still holds a fragment keeps its directory.
/// That second half is asserted here too: a sweep that took occupied directories would be a
/// data-destroying "fix" for a litter problem.
#[test]
fn the_empty_directories_late_writes_leave_are_collected_at_the_next_open() {
    block_on(async {
        let dir = tempfile::tempdir().expect("temp dir");
        let clock = AtAnyScratch {
            root: dir.path().to_path_buf(),
            live: 9_500,
            late: 10_500,
        };
        let s = FsChunkStore::open_with_clock(dir.path(), clock).expect("open store");

        // A fragment that landed *before* any of this: its chunk directory is occupied, and
        // must survive the sweep.
        let kept = fid(0xfeed_0000_0000_0000_0000_0000_0000_0000, 0);
        let kept_bytes = fragment(kept, b"a fragment that landed in time");
        s.put_fragment(kept, kept_bytes.clone(), None)
            .await
            .expect("a deadline-less write is unaffected");

        for n in 0..8u128 {
            let id = fid(0xdead_0000_0000_0000_0000_0000_0000_0000 | n, 0);
            let err = s
                .put_fragment(id, fragment(id, b"another straggler"), Some(10_000))
                .await
                .expect_err("each write expires while its bytes are being written");
            assert!(
                wyrd_traits::is_write_deadline_expired(err.as_ref()),
                "and each is the clean refusal, so the store claims it left nothing: {err}"
            );
        }

        // Through the seam the refusals are invisible already: no fragment, no scratch.
        assert_eq!(
            s.list_fragments().await.unwrap(),
            vec![kept],
            "8 refused writes published nothing"
        );
        assert!(
            scratch_files(dir.path()).is_empty(),
            "and left no scratch: {:?}",
            scratch_files(dir.path())
        );
        assert_eq!(
            std::fs::read_dir(dir.path())
                .expect("read store root")
                .count(),
            9,
            "what they did leave is one empty directory each — the shared container a \
             refusal must not remove while writers may be creating it"
        );

        // The restart a D server performs after an unclean exit collects them.
        let reopened = FsChunkStore::open(dir.path()).expect("reopen store");
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read store root")
            .flatten()
            .map(|e| e.path())
            .collect();
        assert_eq!(
            leftovers,
            vec![dir.path().join(format!("{:032x}", kept.chunk))],
            "open collects every empty chunk directory and nothing else, so the residue is \
             bounded by one server lifetime rather than unbounded: {leftovers:?}"
        );
        assert_eq!(
            reopened.get_fragment(kept).await.unwrap().as_deref(),
            Some(kept_bytes.as_ref()),
            "and the occupied directory's fragment is untouched — the sweep removes empty \
             directories, never a chunk that still holds bytes"
        );
    });
}
