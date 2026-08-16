//! Concurrency regression for `FsChunkStore::put_fragment` (issue #203).
//!
//! Many writers racing on the **same** `FragmentId` must all succeed: each uses
//! a private scratch file and publishes via an atomic rename, so no writer can
//! observe or clobber another's partial bytes and none fails spuriously. Before
//! the fix the scratch path was keyed on the `FragmentId` alone (`<index>.tmp`)
//! and shared across calls; concurrent same-id writes raced on it and the second
//! `fs::rename` could hit `NotFound`, spuriously erroring a legitimate
//! duplicate/repair write.
//!
//! The store's I/O is synchronous (`std::fs`), so real OS threads driving
//! `pollster::block_on` give genuine concurrency without an async runtime; a
//! `Barrier` releases every writer at once to widen the write→rename race. The
//! load is import-light (no GUI / display / async runtime), so it is safe on a
//! headless runner. Per-write scratch *uniqueness* is also asserted structurally
//! in the crate's unit tests
//! (`scratch_names_are_unique_per_seq_and_invisible_to_listing`); this test is
//! the behavioural half — every concurrent put returns `Ok`.
//!
//! The second and third tests are issue #638's destructive-concurrency cases, and both turn
//! on writers whose deadline elapses **after the store has admitted them** — the only writers
//! that reach the rollback at all. A writer already expired when it arrives is refused by the
//! entry check having touched nothing, so racing *those* against live writers exercises
//! neither the rollback nor its interference with a live write: it looks like coverage and is
//! not. [`PerWriterClock`] is what makes the post-admission case reachable, by giving each
//! writer thread its own timeline, and every expired writer asserts it really was admitted
//! (its second clock read) rather than turned away at the door.
//!
//! The second test races them on **one `FragmentId`**: an enforcement point placed after the
//! publishing rename would let a straggler delete a fragment another writer had already
//! published. The third races them on **one chunk directory, distinct fragment indices**: a
//! refusal that removed the shared directory it was left holding could strip it from under a
//! live writer that has just created it.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use bytes::Bytes;
use pollster::block_on;
use wyrd_chunk_format::{encode, FragmentHeader};
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_traits::{ChunkStore, FragmentId};

/// Build a valid v1 fragment whose header records `id`'s chunk and index.
fn fragment(id: FragmentId, payload: &[u8]) -> Bytes {
    let mut header = FragmentHeader::new_v1(id.chunk, payload.len() as u64);
    header.ec_fragment_index = id.index;
    Bytes::from(encode(&header, payload))
}

/// Writers released together per round, and rounds repeated: the pre-fix race is
/// interleaving-dependent, so writers × rounds amplify it to a near-certain red,
/// while the post-fix green is deterministic (every write `Ok` regardless of
/// interleaving — each has private scratch, the rename is the only publish).
const WRITERS: usize = 64;
const ROUNDS: usize = 16;

/// Release `WRITERS` threads at once, each writing `frag` under `id`, and return
/// every call's outcome (the error rendered to a `String` so it crosses the
/// thread boundary cleanly).
fn race_same_id(
    store: &FsChunkStore,
    id: FragmentId,
    frag: &Bytes,
) -> Vec<std::result::Result<(), String>> {
    let barrier = Barrier::new(WRITERS);
    thread::scope(|scope| {
        let handles: Vec<_> = (0..WRITERS)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    block_on(store.put_fragment(id, frag.clone(), None)).map_err(|e| e.to_string())
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

#[test]
fn concurrent_same_id_writes_all_succeed_and_publish_one_verified_fragment() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = FsChunkStore::open(dir.path()).expect("open store");

    let id = FragmentId {
        chunk: 0x00c0_ffee_u128,
        index: 7,
    };
    let frag = fragment(id, b"a duplicate/repair fragment written concurrently");

    for round in 0..ROUNDS {
        let results = race_same_id(&store, id, &frag);
        for (writer, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "round {round} writer {writer}: a concurrent same-id put must not fail \
                 spuriously, got {:?}",
                result.as_ref().err()
            );
        }
    }

    // The atomic rename published exactly one complete, verifying fragment.
    let got = block_on(store.get_fragment(id))
        .expect("get must not error")
        .expect("the fragment was published");
    assert_eq!(
        got, frag,
        "the published fragment is byte-complete and verifies"
    );

    // Scratch files never surface as fragments: the store lists exactly the one id.
    let listed = block_on(store.list_fragments()).expect("list");
    assert_eq!(
        listed,
        vec![id],
        "list_fragments ignores temp scratch, reporting only the one published fragment"
    );
}

/// A [`wyrd_testkit::Clock`] that gives **each writer thread its own timeline** (issue #638).
///
/// The hazard these tests exist for lives strictly *after* the store admits a write: the
/// expired writer must get past the entry check, create the chunk directory, write its
/// scratch, and only then be refused — because that is the only path on which a rollback
/// runs at all. A store-wide clock cannot express that under concurrency: one shared script
/// is consumed in whatever order the threads happen to read it, and a fixed reading makes
/// every "expired" writer expired *on arrival*, i.e. refused at the door with nothing on
/// disk. Racing those against live writers exercises nothing.
///
/// So the timeline is keyed by the writing thread: each writer scripts its own
/// ([`script`](Self::script)) before the barrier releases it, and gets `first` on its first
/// read and `rest` on every read after that — deterministically, whatever the interleaving.
/// A writer scripted `(live, past-its-deadline)` is therefore **admitted and then refused at
/// the publication point**, which is exactly the writer that rolls back.
///
/// [`reads`](Self::reads) is the fixture's own guard: the count a writer's thread took tells
/// the test *where* it was refused (1 = at admission, 2 = at the publication point after its
/// bytes were written, 3 = published and verified), so a fixture that has quietly stopped
/// reaching the rollback fails instead of passing vacuously.
#[derive(Clone, Default)]
struct PerWriterClock {
    timelines: Arc<Mutex<HashMap<thread::ThreadId, WriterTime>>>,
}

/// One writer's scripted timeline and the number of readings the store has taken from it.
#[derive(Clone, Copy)]
struct WriterTime {
    first: u64,
    rest: u64,
    reads: u32,
}

impl PerWriterClock {
    /// Script the calling thread's timeline: `first` for its first reading, `rest` for the
    /// rest. Called by a writer before it enters `put_fragment`.
    fn script(&self, first: u64, rest: u64) {
        self.timelines.lock().expect("per-writer clock").insert(
            thread::current().id(),
            WriterTime {
                first,
                rest,
                reads: 0,
            },
        );
    }

    /// How many readings the store took from the calling thread's timeline.
    fn reads(&self) -> u32 {
        self.timelines
            .lock()
            .expect("per-writer clock")
            .get(&thread::current().id())
            .expect("this thread scripted a timeline")
            .reads
    }
}

impl wyrd_testkit::Clock for PerWriterClock {
    fn now_millis(&self) -> u64 {
        let mut timelines = self.timelines.lock().expect("per-writer clock");
        // A read from an unscripted thread is a fixture bug, not a reading to invent: the
        // store would silently get someone else's time. Fail loudly instead.
        let time = timelines
            .get_mut(&thread::current().id())
            .expect("every clock read must come from a writer that scripted its timeline");
        time.reads += 1;
        if time.reads == 1 {
            time.first
        } else {
            time.rest
        }
    }
}

/// The two timelines the deadline races below use. A writer is *live* for its whole call, or
/// *live on arrival and past its deadline by the time its bytes are written* — the
/// post-admission expiry that is the only way to reach the rollback.
const DEADLINE: u64 = 20_000;
const ADMITTED_AT: u64 = 10_000;
const AFTER_THE_DEADLINE: u64 = 30_000;
const LIVE_DEADLINE: u64 = 70_000;

/// One racer's result in the deadline race below: whether it was a *live* writer, how many
/// readings the store took from its timeline (which pins *where* it was refused), and its
/// outcome — for a failure, whether the seam classified it as a deadline refusal (decided
/// inside the thread, while the typed error is still in hand) plus its rendering.
type DeadlineRaceOutcome = (bool, u32, std::result::Result<(), (bool, String)>);

/// Issue #638, the **destructive-concurrency** half of the write deadline: writers whose
/// deadline elapses after admission, racing live ones on the same `FragmentId`, must refuse
/// *themselves* and leave the live writers' fragment intact.
///
/// This is the hazard an enforcement point placed *after* the publishing rename creates:
/// such a design has to publish and then `remove_file`, and the file it removes may be a
/// concurrent same-id writer's — already renamed, already acknowledged. Judging the
/// deadline *before* the rename makes the hazard structurally impossible: a refused write
/// only ever removes its own private scratch (issue #203's per-call scratch name), so no
/// interleaving can turn the deadline into data loss.
///
/// Every expired writer here is **admitted first** ([`PerWriterClock`]) and refused at its
/// publication point with its scratch already on disk, so each one genuinely runs the
/// rollback next to a live writer's publication — the interleaving that would destroy a
/// fragment under a publish-then-retract design. A writer expired *on arrival* would be
/// turned away by the entry check having written nothing, and would prove nothing; the
/// per-writer read count asserts, per writer and per round, that this is not what happened.
#[test]
fn writers_expiring_after_admission_refuse_themselves_and_never_remove_the_fragment() {
    let dir = tempfile::tempdir().expect("temp dir");
    let clock = PerWriterClock::default();
    let store = FsChunkStore::open_with_clock(dir.path(), clock.clone()).expect("open store");

    let id = FragmentId {
        chunk: 0x0638_0638_u128,
        index: 3,
    };
    let frag = fragment(id, b"a live fragment a straggler must not delete");

    for round in 0..ROUNDS {
        let barrier = Barrier::new(WRITERS);
        let results: Vec<DeadlineRaceOutcome> = thread::scope(|scope| {
            let handles: Vec<_> = (0..WRITERS)
                .map(|writer| {
                    let store = &store;
                    let frag = &frag;
                    let barrier = &barrier;
                    let clock = clock.clone();
                    // Alternate live writers and writers that expire once their bytes are
                    // written, on the same id.
                    let live = writer % 2 == 0;
                    scope.spawn(move || {
                        let deadline = if live {
                            clock.script(ADMITTED_AT, ADMITTED_AT);
                            LIVE_DEADLINE
                        } else {
                            clock.script(ADMITTED_AT, AFTER_THE_DEADLINE);
                            DEADLINE
                        };
                        barrier.wait();
                        let outcome =
                            block_on(store.put_fragment(id, frag.clone(), Some(deadline))).map_err(
                                |e| {
                                    (
                                        wyrd_traits::is_write_deadline_expired(e.as_ref()),
                                        e.to_string(),
                                    )
                                },
                            );
                        (live, clock.reads(), outcome)
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        for (writer, (live, reads, outcome)) in results.iter().enumerate() {
            if *live {
                assert!(
                    outcome.is_ok(),
                    "round {round} writer {writer}: a live write must not be refused by a \
                     racing expired one, got {:?}",
                    outcome.as_ref().err()
                );
            } else {
                let (classified, rendered) = outcome
                    .as_ref()
                    .expect_err("an expired write must be refused");
                assert!(
                    classified,
                    "round {round} writer {writer}: an expired write must be refused as a \
                     typed deadline expiry, got {rendered}"
                );
                assert_eq!(
                    *reads, 2,
                    "round {round} writer {writer}: this writer must be refused at the \
                     PUBLICATION POINT (two readings: admitted, then late with its bytes on \
                     disk) — one reading means it was turned away at admission and never \
                     rolled anything back, so this round proved nothing"
                );
            }
        }

        // The live writers' fragment survives every interleaving of the expired ones.
        let got = block_on(store.get_fragment(id))
            .expect("get must not error")
            .unwrap_or_else(|| {
                panic!(
                    "round {round}: the fragment published by the live writers is gone — an \
                     expiring write removed bytes it did not publish"
                )
            });
        assert_eq!(got, frag, "and it is the complete, verifying fragment");
    }

    assert_eq!(
        block_on(store.list_fragments()).expect("list"),
        vec![id],
        "no scratch from a refused write is left behind for the listing to trip over"
    );
}

/// Issue #638, the **shared-container** half of the concurrency story: a refusal that rolls
/// back next to a live writer creating the same chunk directory must not cost that live
/// writer its write.
///
/// This is the race a rollback creates the moment it removes anything **shared**. A refusal
/// that also removed the chunk directory it was left holding empty would do so exactly while
/// a live writer sits between its `create_dir_all` and its data write, and that writer's
/// `fs::write` then fails `NotFound` — a *live* write killed by an *expired* one. Retrying
/// the create bounds nothing: N retries lose to N+1 staggered refusals, so a margin only
/// makes the failure rarer, never impossible. The store therefore removes **only the write's
/// own private scratch** and leaves every shared path alone, and the empty directories are
/// collected at `open`, where the store has no write in flight by construction
/// (`tests/conformance.rs`, `the_empty_directories_late_writes_leave_are_collected_at_the_next_open`).
///
/// Two things make this a real exercise of that path rather than a decorative one:
///
/// * every expired writer is **admitted first** and refused at its publication point, so it
///   has actually created the directory and written its scratch before it rolls back (a
///   writer expired on arrival never gets that far — the entry check turns it away with the
///   disk untouched, which is what made an earlier version of this test vacuous). The
///   per-writer read count asserts it, per writer and per round;
/// * **each round uses a fresh chunk id**, so every round starts with the directory absent,
///   which is the only state in which the create/remove window exists at all.
///
/// Live and expired writers take **distinct fragment indices** of the one chunk, so what they
/// contend on is the shared directory rather than a fragment (the same-id contention is the
/// test above).
#[test]
fn a_rollback_next_to_a_live_writer_creating_the_same_chunk_never_costs_it_its_write() {
    let dir = tempfile::tempdir().expect("temp dir");
    let clock = PerWriterClock::default();
    let store = FsChunkStore::open_with_clock(dir.path(), clock.clone()).expect("open store");

    for round in 0..ROUNDS {
        // A chunk this store has never seen: its directory does not exist yet, so the live
        // writers must create it while the expiring ones are rolling back inside it.
        let chunk = 0x0638_0000_0000_0000_0000_0000_0000_0000_u128 + round as u128;
        let results: Vec<(bool, u16, u32, std::result::Result<(), String>)> = {
            let barrier = Barrier::new(WRITERS);
            thread::scope(|scope| {
                let handles: Vec<_> = (0..WRITERS)
                    .map(|writer| {
                        let store = &store;
                        let barrier = &barrier;
                        let clock = clock.clone();
                        // Alternate live writers and writers that expire once their bytes
                        // are written, each on its **own** fragment index of the one chunk.
                        let live = writer % 2 == 0;
                        let index = writer as u16;
                        scope.spawn(move || {
                            let id = FragmentId { chunk, index };
                            let frag = fragment(id, b"contending on one chunk directory");
                            let deadline = if live {
                                clock.script(ADMITTED_AT, ADMITTED_AT);
                                LIVE_DEADLINE
                            } else {
                                clock.script(ADMITTED_AT, AFTER_THE_DEADLINE);
                                DEADLINE
                            };
                            barrier.wait();
                            let outcome = block_on(store.put_fragment(id, frag, Some(deadline)))
                                .map_err(|e| e.to_string());
                            (live, index, clock.reads(), outcome)
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            })
        };

        for (live, index, reads, outcome) in &results {
            if *live {
                assert!(
                    outcome.is_ok(),
                    "round {round} index {index}: a live write must not be knocked over by \
                     a racing refusal's rollback, got {:?}",
                    outcome.as_ref().err()
                );
                assert!(
                    block_on(store.get_fragment(FragmentId {
                        chunk,
                        index: *index
                    }))
                    .expect("get must not error")
                    .is_some(),
                    "round {round} index {index}: and its fragment is readable afterwards"
                );
            } else {
                let rendered = outcome
                    .as_ref()
                    .expect_err("an expired write must still be refused");
                assert!(
                    rendered.contains("NOT applied"),
                    "round {round} index {index}: and refused as the clean deadline verdict, \
                     not as a rollback fault: {rendered}"
                );
                assert_eq!(
                    *reads, 2,
                    "round {round} index {index}: this writer must be refused at the \
                     PUBLICATION POINT (two readings: admitted, then late with its scratch \
                     on disk) — one reading means it never created the directory or wrote a \
                     scratch, so it never ran the rollback this test is about"
                );
            }
        }
    }

    // Exactly the live writers' fragments, and nothing else: no scratch, no phantom.
    let listed = block_on(store.list_fragments()).expect("list");
    assert_eq!(
        listed.len(),
        ROUNDS * WRITERS.div_ceil(2),
        "every live write of every round is published, and only those"
    );
    let scratch: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read store root")
        .flatten()
        .flat_map(|chunk_dir| {
            std::fs::read_dir(chunk_dir.path())
                .expect("read chunk dir")
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("tmp"))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        scratch.is_empty(),
        "and every refusal took its own scratch back off the disk: {scratch:?}"
    );
}
