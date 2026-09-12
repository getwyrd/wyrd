//! Issue #772 — the multipart **owned staging entry**: the `sidx:<upload-id>:<part-number>:<chunk-id>`
//! value (`wyrd_core::multipart::{OwnedEntry, StagedPlacement, decode_owned_entry}`) and the two
//! additive ownership fields it adds to the shared `wyrd_core::metadata::PendingEntry` (proposal
//! 0016, `:353`, `:442-491`) — and, because `sidx:` and `pending:` are two key spaces sharing one
//! value shape, the `pending:` half of the same boundary (`metadata::decode_pending_entry`, the
//! `pending:` readers that now go through it, and the two `pending:` writers that apply its rule
//! to what they store).
//!
//! Mirrors `multipart_session_records.rs` (#716) and `multipart_retire_obligation.rs` (#771):
//! hand-authored JSON bytes and the production codec, every witness **decoded rather than
//! constructed** — S3's torn literal and the minted entries aside, whose whole point is what a
//! caller outside the crate can build. Each namespace has two decode surfaces, asserted to agree
//! wherever they can: the namespace's own entry point (`decode_owned_entry`, `decode_pending_entry`)
//! and the store-wide `metadata::decode::<PendingEntry>`, which judges the value's shape alone and
//! so can see neither key relation (the owner, the namespace). Serialization identity is asserted
//! over every accepted witness by `decode_owned_witness` / `decode_pending_witness` — the
//! `decode_both` shape of `multipart_session_records.rs:169`.
//!
//! Unlike its siblings this file is not wholly pure. Its store legs drive production code over an
//! in-process `RedbMetadataStore` — the harness `stream_lease_lapse.rs` and
//! `stream_lease_renewal.rs` already use for these same functions — because what they own is what
//! a **reader** or **writer** does with a misfiled value, which no decode-only witness shows:
//! `renew_pending`, `live_lease_guards` (reached through `create_leased`),
//! `write::sweep_expired_leases` and `put_pending`. The fourth `pending:` reader, the custodian GC's
//! expired-lease scan, has its leg in `crates/custodian/tests/gc.rs`, since it needs the custodian
//! crate.
//!
//! The legs (Success criterion, brief #772), each asserted in **one** test so that negating one
//! production rule fails exactly that test:
//!
//! * **S1** staged geometry — `rs(0,1)`/`rs(3,0)` are `StagedSchemeUnsupported`, at decode and at
//!   the checked constructor alike;
//! * **S2** key/value agreement — an owner other than the key's upload id is refused;
//! * **S3** torn shape — exactly one ownership field is `TornOwnedEntry` under **both** namespaces,
//!   whether the bytes are hand-written or encoded from a literal an outside caller assembled, and
//!   that literal is never stored under `pending:`;
//! * **S4** namespace agreement — each namespace's entry point refuses the other's shape, the two
//!   lease readers in `metadata.rs` refuse a misfiled owned entry rather than renew it or commit
//!   over it, and the two `pending:` writers refuse to store one (asserting only that they refuse —
//!   their boxed error type is not this child's);
//! * **S5** (core half) — the lease sweep skips what it cannot read, reclaims nothing on it,
//!   completes for every other entry on either side of it, and reports each skipped key rather
//!   than returning success — over a named fixture and over seeded populations (Tier 0);
//! * **S6** — a length-mismatched staged placement **decodes** (the contextual check, not this
//!   decode's);
//! * **S7** — an ordinary `pending:` entry re-encodes byte-identically, the omitted-when-absent
//!   fields asserted rather than assumed;
//! * **S8** — an owned entry re-encodes byte-identically across a renewal that moves only its lease,
//!   and a foreign spelling of one is refused rather than accepted and rewritten;
//! * **S9** — an owned entry is minted from outside the crate through the public checked path, both
//!   decoders agree on what it is, and the public validator refuses a hand-built record of either
//!   other shape.
//!
//! No `#![cfg(...)]` here — this file always compiles and always runs.

#![forbid(unsafe_code)]

use pollster::block_on;
use wyrd_core::metadata::{self, EcScheme, InodeRecord, PendingEntry};
use wyrd_core::multipart::{
    decode_owned_entry, sidx_key, OwnedEntry, PartNumber, RecordError, StagedPlacement, UploadId,
};
use wyrd_core::write::{self, WriteError};
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_testkit::Sim;
use wyrd_traits::{ChunkId, CommitOutcome, MetadataStore, WriteBatch};

/// The part attempt and chunk every `sidx:` key below names. Neither is `0` or `1`: a witness that
/// only ever observes a field's smallest value cannot tell a decoder that reads it from one that
/// returns a default.
const PART: u32 = 3;
const CHUNK: ChunkId = 9;
/// The lease every witness carries, and the lease a renewal moves it to.
const LEASE: u64 = 1_500;
const RENEWED: u64 = 4_500;
/// A supported Reed-Solomon geometry — `erasure::supported(2, 1)` — and a placement of exactly
/// its three fragments, so no witness but S6's carries a length mismatch.
const RS_2_1: &str = r#"{"ReedSolomon":{"k":2,"m":1}}"#;
const PLACEMENT: &str = "[5,6,7]";

/// 32 lowercase-hex characters from a 2-character pair — an upload id (`0016:493-497`).
fn hex32(pair: &str) -> String {
    pair.repeat(16)
}

fn upload_id(pair: &str) -> UploadId {
    UploadId::new(hex32(pair)).expect("32 lowercase-hex characters are an upload id")
}

/// The `sidx:` key for [`PART`]/[`CHUNK`] under the session `pair` names, minted by the
/// production `sidx_key`.
fn key(pair: &str) -> Vec<u8> {
    sidx_key(
        &upload_id(pair),
        PartNumber::new(PART).expect("a part number in range"),
        CHUNK,
    )
}

/// The owned entry of the [`owned`] witness, minted through the public checked path — as the
/// first `sidx:` writer (#656–#659), which lives outside this crate, will mint one.
fn minted() -> OwnedEntry {
    let staged = StagedPlacement::new(EcScheme::ReedSolomon { k: 2, m: 1 }, vec![5, 6, 7])
        .expect("rs(2,1) is supported geometry");
    OwnedEntry::new(upload_id("a1"), LEASE, staged)
}

/// The shared record with neither ownership field — what every streaming write stores.
fn ordinary_entry(lease: u64) -> PendingEntry {
    PendingEntry {
        lease_expiry_millis: lease,
        owner: None,
        staged: None,
    }
}

// ===========================================================================
// Hand-authored bytes — field names and order exactly as the production types declare them, so a
// byte-identity assertion after re-encoding is a claim about this codec's own spelling.
// ===========================================================================

/// A staged placement's stored spelling: `scheme`, then `placement`.
fn staged_json(scheme_json: &str, placement: &str) -> String {
    format!("{{\"scheme\":{scheme_json},\"placement\":{placement}}}")
}

/// An **owned** value — both ownership fields present, after the lease.
fn owned_value(lease: u64, owner_pair: &str, staged: &str) -> Vec<u8> {
    format!(
        "{{\"lease_expiry_millis\":{lease},\"owner\":\"{}\",\"staged\":{staged}}}",
        hex32(owner_pair)
    )
    .into_bytes()
}

/// The owned witness every leg uses unless it perturbs one component: session `a1`, `rs(2,1)`
/// over three servers.
fn owned() -> Vec<u8> {
    owned_value(LEASE, "a1", &staged_json(RS_2_1, PLACEMENT))
}

/// An **ordinary** value — the spelling every streaming write has always stored under
/// `pending:`: the lease and nothing else.
fn ordinary(lease: u64) -> Vec<u8> {
    format!("{{\"lease_expiry_millis\":{lease}}}").into_bytes()
}

/// The two **torn** spellings — one ownership field without the other.
fn owner_only(lease: u64) -> Vec<u8> {
    format!(
        "{{\"lease_expiry_millis\":{lease},\"owner\":\"{}\"}}",
        hex32("a1")
    )
    .into_bytes()
}

fn staged_only(lease: u64) -> Vec<u8> {
    format!(
        "{{\"lease_expiry_millis\":{lease},\"staged\":{}}}",
        staged_json(RS_2_1, PLACEMENT)
    )
    .into_bytes()
}

// ===========================================================================
// The decode surfaces + serialization identity. Every witness a leg expects **accepted** goes
// through one of these two helpers, so identity is a property of the whole accepted set rather
// than a test of its own.
// ===========================================================================

/// Decode a `sidx:` witness through the namespace's entry point and, for an **accepted** one,
/// assert that the store-wide shape decode reads the same record and that re-encoding it
/// reproduces the bytes it was read from. A refused witness is returned as-is: the shape decode
/// cannot see the key, so it is not expected to agree on the key-relation rules (S2, S4).
fn decode_owned_witness(
    key: &[u8],
    value: &[u8],
) -> Result<(PartNumber, ChunkId, OwnedEntry), RecordError> {
    let decoded = decode_owned_entry(key, value);
    if let Ok((_, _, entry)) = &decoded {
        let shared: PendingEntry =
            metadata::decode(value).expect("an accepted owned entry is a well-formed record");
        assert_eq!(shared, entry.to_pending(), "the two surfaces disagree");
        assert_eq!(
            String::from_utf8_lossy(metadata::encode(&shared).as_ref()),
            String::from_utf8_lossy(value),
            "decode->encode is not byte-identical for {entry:?}"
        );
    }
    decoded
}

/// The `pending:` peer of [`decode_owned_witness`].
fn decode_pending_witness(value: &[u8]) -> Result<PendingEntry, RecordError> {
    let decoded = metadata::decode_pending_entry(value);
    if let Ok(entry) = &decoded {
        let shared: PendingEntry =
            metadata::decode(value).expect("an accepted pending entry is a well-formed record");
        assert_eq!(&shared, entry, "the two surfaces disagree");
        assert_eq!(
            String::from_utf8_lossy(metadata::encode(entry).as_ref()),
            String::from_utf8_lossy(value),
            "decode->encode is not byte-identical for {entry:?}"
        );
    }
    decoded
}

/// A fresh in-process store for the reader and writer legs — the concrete
/// `stream_lease_lapse.rs` drives the same functions over.
fn store() -> RedbMetadataStore {
    RedbMetadataStore::in_memory().expect("an in-memory redb store opens")
}

/// Put `value` under `pending:<chunk>` **raw** — how a misfiled or damaged value arrives, since
/// neither production `pending:` writer will store one.
async fn put_raw_pending(store: &RedbMetadataStore, chunk: ChunkId, value: &[u8]) {
    let outcome = store
        .commit(WriteBatch::new().put(metadata::pending_key(chunk), value.to_vec()))
        .await
        .expect("the store accepts the put");
    assert_eq!(outcome, CommitOutcome::Committed);
}

async fn stored_pending(store: &RedbMetadataStore, chunk: ChunkId) -> Option<Vec<u8>> {
    store
        .get(&metadata::pending_key(chunk))
        .await
        .expect("the store answers")
        .map(|bytes| bytes.to_vec())
}

// ===========================================================================
// Round trip — the owned entry decodes under its own key, and hands back what the key names.
// ===========================================================================

#[test]
fn an_owned_entry_round_trips_under_its_own_key() {
    let (part, chunk, entry) =
        decode_owned_witness(&key("a1"), &owned()).expect("an owned entry decodes");
    assert_eq!(part.get(), PART);
    assert_eq!(chunk, CHUNK);
    assert_eq!(entry.owner(), &upload_id("a1"));
    assert_eq!(entry.lease_expiry_millis(), LEASE);
    assert_eq!(
        entry.staged().scheme(),
        EcScheme::ReedSolomon { k: 2, m: 1 }
    );
    assert_eq!(entry.staged().placement(), &[5, 6, 7]);

    // `EcScheme::None` has no `(k, m)` pair to judge: one fragment, always valid.
    let replicated = owned_value(LEASE, "a1", &staged_json("\"None\"", "[5]"));
    let (_, _, entry) =
        decode_owned_witness(&key("a1"), &replicated).expect("an rs-free owned entry decodes");
    assert_eq!(entry.staged().scheme(), EcScheme::None);
}

// ===========================================================================
// S1 — staged geometry is judged at decode (ADR-0045's `EcScheme` row, the #285 class).
// ===========================================================================

#[test]
fn s1_staged_geometry_the_erasure_coder_refuses_is_a_typed_error() {
    for (k, m, placement) in [(0u8, 1u8, "[5]"), (3, 0, "[5,6,7]")] {
        let scheme = format!("{{\"ReedSolomon\":{{\"k\":{k},\"m\":{m}}}}}");
        let value = owned_value(LEASE, "a1", &staged_json(&scheme, placement));
        assert_eq!(
            decode_owned_witness(&key("a1"), &value).map(|(_, _, entry)| entry),
            Err(RecordError::StagedSchemeUnsupported { k, m }),
            "rs({k},{m}) is geometry the coder refuses, never a value"
        );
        // A value-only rule, so the shape surface refuses it too — and so does the `pending:`
        // decode, whatever else is wrong with an owned value filed there.
        assert!(metadata::decode::<PendingEntry>(&value).is_err());
        assert!(metadata::decode_pending_entry(&value).is_err());
        // The checked constructor refuses exactly what decode refuses: no path mints it.
        assert_eq!(
            StagedPlacement::new(EcScheme::ReedSolomon { k, m }, vec![5]),
            Err(RecordError::StagedSchemeUnsupported { k, m })
        );
    }
}

/// The scheme is read through the module's **closed** wire (`EcSchemeWire`): an unknown field
/// inside it is a decode error, not a field a renewal's re-encode would drop.
#[test]
fn staged_scheme_is_read_through_a_closed_wire() {
    let scheme = r#"{"ReedSolomon":{"k":2,"m":1,"w":8}}"#;
    let value = owned_value(LEASE, "a1", &staged_json(scheme, PLACEMENT));
    match decode_owned_entry(&key("a1"), &value) {
        Err(RecordError::MalformedRecordValue { namespace, detail }) => {
            assert_eq!(namespace, "sidx:");
            assert!(detail.contains("unknown field"), "{detail}");
        }
        other => panic!("expected MalformedRecordValue(sidx:), got {other:?}"),
    }
}

// ===========================================================================
// S2 — the value's owner is the key's upload id.
// ===========================================================================

#[test]
fn s2_an_owner_other_than_the_keys_upload_id_is_refused() {
    // Session a1's entry, filed under session b2's key.
    assert_eq!(
        decode_owned_witness(&key("b2"), &owned()).map(|(_, _, entry)| entry),
        Err(RecordError::OwnedEntryOwnerMismatch {
            key_owner: upload_id("b2"),
            entry_owner: upload_id("a1"),
        })
    );
    // Why the key is a parameter: the value alone is a perfectly good owned entry.
    assert!(metadata::decode::<PendingEntry>(&owned()).is_ok());
}

// ===========================================================================
// S3 — a torn value is refused under both namespaces (`0016:454-457`).
// ===========================================================================

#[test]
fn s3_a_torn_value_is_refused_under_both_namespaces() {
    let torn = |present, absent| RecordError::TornOwnedEntry { present, absent };
    for (value, present, absent) in [
        (owner_only(LEASE), "owner", "staged"),
        (staged_only(LEASE), "staged", "owner"),
    ] {
        assert_eq!(
            decode_pending_witness(&value),
            Err(torn(present, absent)),
            "the pending: reading"
        );
        assert_eq!(
            decode_owned_witness(&key("a1"), &value).map(|(_, _, entry)| entry),
            Err(torn(present, absent)),
            "the sidx: reading"
        );
        assert!(
            metadata::decode::<PendingEntry>(&value).is_err(),
            "the shared record refuses it too"
        );
    }

    // The literal a caller outside the crate could assemble without the checked path (S9's
    // negation, by construction): the shared record's fields are public, so it compiles — and the
    // public validator refuses it, and so do both decoders the bytes it encodes to.
    let staged =
        StagedPlacement::new(EcScheme::ReedSolomon { k: 2, m: 1 }, vec![5, 6, 7]).expect("rs(2,1)");
    let literals = [
        (
            PendingEntry {
                lease_expiry_millis: LEASE,
                owner: Some(upload_id("a1")),
                staged: None,
            },
            "owner",
            "staged",
        ),
        (
            PendingEntry {
                lease_expiry_millis: LEASE,
                owner: None,
                staged: Some(staged),
            },
            "staged",
            "owner",
        ),
    ];
    for (literal, present, absent) in &literals {
        assert_eq!(
            OwnedEntry::from_pending(literal),
            Err(torn(*present, *absent))
        );
        let bytes = metadata::encode(literal);
        assert_eq!(
            metadata::decode_pending_entry(&bytes),
            Err(torn(*present, *absent))
        );
        assert_eq!(
            decode_owned_entry(&key("a1"), &bytes).map(|(_, _, entry)| entry),
            Err(torn(*present, *absent))
        );
    }

    // Nor can that literal be **stored** under `pending:`, where it would be bytes nothing reads
    // back: both writers refuse it before touching the store, leaving the chunk it names unwritten
    // and the live lease it would renew as it was. Only the refusal is asserted — the writers'
    // boxed error type is not this child's.
    block_on(async {
        let store = store();
        let leased: ChunkId = CHUNK + 1;
        put_raw_pending(&store, leased, &ordinary(LEASE)).await;
        for (literal, _, _) in &literals {
            let put = metadata::put_pending(&store, CHUNK, literal).await;
            assert!(put.is_err(), "a torn entry was stored: {put:?}");
            let renewed = metadata::renew_pending(&store, &[leased], LEASE - 1, literal).await;
            assert!(
                renewed.is_err(),
                "a live lease was renewed to a torn entry: {renewed:?}"
            );
        }
        assert_eq!(stored_pending(&store, CHUNK).await, None);
        assert_eq!(stored_pending(&store, leased).await, Some(ordinary(LEASE)));
    });
}

// ===========================================================================
// S4 — the namespace is a decode-time property, not a convention.
// ===========================================================================

#[test]
fn s4_each_namespace_refuses_the_other_namespaces_shape() {
    // An owned entry filed under `pending:`.
    assert_eq!(
        decode_pending_witness(&owned()),
        Err(RecordError::PendingEntryNamespaceMismatch {
            namespace: "pending:",
            shape: "owned",
        })
    );
    // An ordinary lease under a session's `sidx:` range.
    assert_eq!(
        decode_owned_witness(&key("a1"), &ordinary(LEASE)).map(|(_, _, entry)| entry),
        Err(RecordError::PendingEntryNamespaceMismatch {
            namespace: "sidx:",
            shape: "ordinary",
        })
    );
    // Why one entry point per namespace: the store-wide shape decode reads either shape, because
    // it cannot see which key the bytes came from.
    assert!(metadata::decode::<PendingEntry>(&owned()).is_ok());
    assert!(metadata::decode::<PendingEntry>(&ordinary(LEASE)).is_ok());
}

/// `renew_pending` preconditions on the raw bytes it read and **puts its caller's entry**
/// (`metadata.rs`, `renew_pending`), so a misfiled owned entry it accepted would have its
/// ownership erased by the renewal. It must refuse instead, and leave the record as it was. Only
/// the refusal is asserted — not the boxed error's type.
#[test]
fn s4_renew_pending_refuses_a_misfiled_owned_entry() {
    block_on(async {
        let store = store();
        let ordinary_chunk: ChunkId = CHUNK + 1;
        put_raw_pending(&store, CHUNK, &owned()).await;
        put_raw_pending(&store, ordinary_chunk, &ordinary(LEASE)).await;
        let renewal = ordinary_entry(RENEWED);

        // `now` is inside the lease, so only the value's shape can refuse this renewal.
        let refused = metadata::renew_pending(&store, &[CHUNK], LEASE - 1, &renewal).await;
        assert!(
            refused.is_err(),
            "a misfiled owned entry was renewed as an ordinary lease: {refused:?}"
        );
        assert_eq!(
            stored_pending(&store, CHUNK).await,
            Some(owned()),
            "the refused renewal must leave the owned entry exactly as it was"
        );

        // The same renewal over an ordinary lease still lands: the refusal is the shape's.
        let renewed = metadata::renew_pending(&store, &[ordinary_chunk], LEASE - 1, &renewal)
            .await
            .expect("an ordinary lease renews");
        assert_eq!(renewed, CommitOutcome::Committed);
    });
}

/// The lease guards a phase-3 commit threads into its batch (`live_lease_guards`, reached through
/// `create_leased`) must refuse a misfiled owned entry too, rather than treat it as the live lease
/// protecting the chunk and publish over it.
#[test]
fn s4_a_leased_commit_refuses_over_a_misfiled_owned_entry() {
    block_on(async {
        let store = store();
        put_raw_pending(&store, CHUNK, &owned()).await;
        let record = InodeRecord::new_empty();

        let refused =
            metadata::create_leased(&store, 0, "obj", 7, &record, &[CHUNK], LEASE - 1).await;
        assert!(
            refused.is_err(),
            "a commit was guarded by a misfiled owned entry read as a live lease: {refused:?}"
        );
        assert_eq!(
            store
                .get(&metadata::inode_key(7))
                .await
                .expect("the store answers"),
            None,
            "nothing is published over the refused guard"
        );

        // Guarded by an ordinary live lease, the same commit lands.
        let ordinary_chunk: ChunkId = CHUNK + 1;
        put_raw_pending(&store, ordinary_chunk, &ordinary(LEASE)).await;
        let landed =
            metadata::create_leased(&store, 0, "obj", 7, &record, &[ordinary_chunk], LEASE - 1)
                .await
                .expect("an ordinary lease guards a commit");
        assert_eq!(landed, CommitOutcome::Committed);
    });
}

/// The write half of the same rule. `put_pending` and `renew_pending` store the entry their
/// caller hands them, and the shared record's fields are public, so only the writers themselves
/// stand between an owned entry and a `pending:` value every reader above refuses — a lease no
/// renewal could extend and no sweep could reclaim. Both must refuse it before touching the store.
/// Only the refusal is asserted — not the boxed error's type.
#[test]
fn s4_the_pending_writers_refuse_to_store_an_owned_entry() {
    block_on(async {
        let store = store();
        let owned_entry = minted().to_pending();
        let leased: ChunkId = CHUNK + 1;
        put_raw_pending(&store, leased, &ordinary(LEASE)).await;

        let put = metadata::put_pending(&store, CHUNK, &owned_entry).await;
        assert!(
            put.is_err(),
            "an owned entry was stored under pending: {put:?}"
        );
        assert_eq!(
            stored_pending(&store, CHUNK).await,
            None,
            "the refused put must write nothing"
        );

        let renewed = metadata::renew_pending(&store, &[leased], LEASE - 1, &owned_entry).await;
        assert!(
            renewed.is_err(),
            "a live ordinary lease was renewed into an owned entry: {renewed:?}"
        );
        assert_eq!(
            stored_pending(&store, leased).await,
            Some(ordinary(LEASE)),
            "the refused renewal must leave the lease exactly as it was"
        );

        // The same two writes with an ordinary entry still land: the refusal is the shape's.
        let lease = ordinary_entry(RENEWED);
        assert_eq!(
            metadata::put_pending(&store, CHUNK, &lease)
                .await
                .expect("an ordinary lease is stored"),
            CommitOutcome::Committed
        );
        assert_eq!(
            metadata::renew_pending(&store, &[leased], LEASE - 1, &lease)
                .await
                .expect("an ordinary lease renews"),
            CommitOutcome::Committed
        );
    });
}

// ===========================================================================
// S5 (core half) — the lease sweep classifies, skips and reports; it never `?`-aborts on one
// record, never stops at one, and never returns success over one it skipped.
// ===========================================================================

/// What one entry of a seeded S5 population is, and so what the sweep must do with it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Drawn {
    /// An ordinary lease at or before `now`: reclaimed.
    Expired,
    /// An ordinary lease still live: kept.
    Live,
    /// An owned `sidx:` entry filed under `pending:`, its lease expired: skipped.
    Misfiled,
    /// An `owner`-only torn value, its lease expired: skipped. The `staged`-only spelling stays
    /// out: the pairing rule alone refuses it, which is S3's leg, and this one must fail only when
    /// the sweep's own rule is broken.
    Torn,
    /// Bytes that are not a pending entry at all: skipped.
    Garbage,
}

/// One seeded population, swept over the production redb store. redb scans in key order, so the
/// drawn chunk ids put the unreadable values before, between and after the readable ones.
async fn sweep_a_seeded_population(seed: u64) {
    const KINDS: [Drawn; 5] = [
        Drawn::Expired,
        Drawn::Live,
        Drawn::Misfiled,
        Drawn::Torn,
        Drawn::Garbage,
    ];
    let mut sim = Sim::new(seed);
    let now = 1_000 + sim.gen::<u64>() % 1_000;
    // One entry the sweep reclaims and one it must skip, then up to six more of any kind. Every
    // unreadable value carries a lease the sweep would reclaim if it misread it.
    let mut kinds = vec![Drawn::Expired, KINDS[2 + (sim.gen::<u32>() % 3) as usize]];
    for _ in 0..sim.gen::<u32>() % 7 {
        kinds.push(KINDS[(sim.gen::<u32>() % 5) as usize]);
    }
    let store = store();
    let mut population: Vec<(ChunkId, Drawn, Vec<u8>)> = Vec::new();
    for kind in kinds {
        let chunk = loop {
            let chunk = ChunkId::from(1 + sim.gen::<u32>() % 10_000);
            if population.iter().all(|&(taken, _, _)| taken != chunk) {
                break chunk;
            }
        };
        // At or before `now`, so `now` itself — the boundary — is drawn too.
        let expired = now - sim.gen::<u64>() % 500;
        let value = match kind {
            Drawn::Expired => ordinary(expired),
            Drawn::Live => ordinary(now + 1 + sim.gen::<u64>() % 500),
            Drawn::Misfiled => owned_value(expired, "a1", &staged_json(RS_2_1, PLACEMENT)),
            Drawn::Torn => owner_only(expired),
            Drawn::Garbage => b"not a pending entry".to_vec(),
        };
        put_raw_pending(&store, chunk, &value).await;
        population.push((chunk, kind, value));
    }

    let swept = write::sweep_expired_leases(&store, now).await;

    for (chunk, kind, value) in &population {
        let stored = stored_pending(&store, *chunk).await;
        if *kind == Drawn::Expired {
            assert_eq!(
                stored, None,
                "seed {seed}: the expired lease on chunk {chunk} was left in place: {swept:?}"
            );
        } else {
            assert_eq!(
                stored.as_ref(),
                Some(value),
                "seed {seed}: the {kind:?} entry on chunk {chunk} must be left exactly as it was"
            );
        }
    }
    let keys_of = |pick: fn(Drawn) -> bool| {
        let mut keys: Vec<Vec<u8>> = population
            .iter()
            .filter(|(_, kind, _)| pick(*kind))
            .map(|(chunk, _, _)| metadata::pending_key(*chunk))
            .collect();
        keys.sort();
        keys
    };
    let err = swept.expect_err("a sweep that skipped entries must not report success");
    match err.downcast_ref::<WriteError>() {
        Some(WriteError::UnreadablePendingEntries { reclaimed, skipped }) => {
            let mut reclaimed: Vec<Vec<u8>> = reclaimed
                .iter()
                .map(|chunk| metadata::pending_key(*chunk))
                .collect();
            reclaimed.sort();
            assert_eq!(
                reclaimed,
                keys_of(|kind| kind == Drawn::Expired),
                "seed {seed}"
            );
            let mut skipped: Vec<Vec<u8>> = skipped.iter().map(|(key, _)| key.clone()).collect();
            skipped.sort();
            let unreadable = |kind| matches!(kind, Drawn::Misfiled | Drawn::Torn | Drawn::Garbage);
            assert_eq!(skipped, keys_of(unreadable), "seed {seed}");
        }
        other => panic!("seed {seed}: expected UnreadablePendingEntries, got {other:?}: {err}"),
    }
}

/// A named fixture first — an expired ordinary lease on **each** side of the three unreadable
/// values in key order, so a sweep that stopped at the first of them leaves one behind — then the
/// same rule over seeded populations: seeded Tier 0 (ADR-0009), `wyrd_testkit::Sim` drawing each
/// seed's mix of kinds, chunk ids and leases (the shape of `erasure.rs`'s
/// `seeded_random_data_and_subsets_round_trip`), with the seed in every failure message.
#[test]
fn s5_the_lease_sweep_skips_what_it_cannot_read_and_completes_for_the_rest() {
    block_on(async {
        let store = store();
        let (expired, live, misfiled, torn, garbage, after): (
            ChunkId,
            ChunkId,
            ChunkId,
            ChunkId,
            ChunkId,
            ChunkId,
        ) = (11, 12, 13, 14, 15, 16);
        put_raw_pending(&store, expired, &ordinary(LEASE)).await;
        put_raw_pending(&store, live, &ordinary(RENEWED)).await;
        // All three unreadable values carry a lease the sweep would reclaim if it read them.
        put_raw_pending(&store, misfiled, &owned()).await;
        put_raw_pending(&store, torn, &owner_only(LEASE)).await;
        put_raw_pending(&store, garbage, b"not a pending entry").await;
        // An expired lease the scan reaches only after all three.
        put_raw_pending(&store, after, &ordinary(LEASE)).await;

        let swept = write::sweep_expired_leases(&store, LEASE).await;

        // The sweep completed for every readable entry, before and after the unreadable ones…
        for reclaimed in [expired, after] {
            assert_eq!(
                stored_pending(&store, reclaimed).await,
                None,
                "one unreadable record must not stall the reclaim of an expired lease beside it: \
                 {swept:?}"
            );
        }
        assert_eq!(stored_pending(&store, live).await, Some(ordinary(RENEWED)));
        // …skipped each unreadable one, byte for byte…
        assert_eq!(stored_pending(&store, misfiled).await, Some(owned()));
        assert_eq!(stored_pending(&store, torn).await, Some(owner_only(LEASE)));
        assert_eq!(
            stored_pending(&store, garbage).await,
            Some(b"not a pending entry".to_vec())
        );

        // …and reported what it skipped, beside what it reclaimed, instead of returning success.
        let err = swept.expect_err("a sweep that skipped entries must not report success");
        match err.downcast_ref::<WriteError>() {
            Some(WriteError::UnreadablePendingEntries { reclaimed, skipped }) => {
                assert_eq!(
                    reclaimed,
                    &vec![expired, after],
                    "only the readable expired leases"
                );
                let keys: Vec<Vec<u8>> = skipped.iter().map(|(key, _)| key.clone()).collect();
                assert_eq!(keys, [misfiled, torn, garbage].map(metadata::pending_key));
            }
            other => panic!("expected UnreadablePendingEntries, got {other:?}: {err}"),
        }
        assert!(
            err.to_string().contains("pending:13"),
            "the report must name a skipped entry: {err}"
        );

        for seed in 0..64 {
            sweep_a_seeded_population(seed).await;
        }
    });
}

// ===========================================================================
// S6 — placement length is the contextual check: a mismatched one decodes.
// ===========================================================================

#[test]
fn s6_a_length_mismatched_staged_placement_decodes() {
    // `rs(2,1)` has three fragments; this placement names two servers.
    let value = owned_value(LEASE, "a1", &staged_json(RS_2_1, "[5,6]"));
    let (_, _, entry) = decode_owned_witness(&key("a1"), &value)
        .expect("a length-mismatched placement is liberal on read (ADR-0045 :45-49)");
    assert_eq!(entry.staged().placement(), &[5, 6]);
    assert!(
        StagedPlacement::new(EcScheme::ReedSolomon { k: 2, m: 1 }, vec![5, 6]).is_ok(),
        "the checked constructor judges geometry, not length"
    );
}

// ===========================================================================
// S7 — an ordinary `pending:` entry re-encodes byte-identically.
// ===========================================================================

/// Why this is not cosmetic: the `pending:` renewal puts its caller's freshly encoded entry over a
/// precondition on the raw bytes it read, so an encoder that spelled an absent field `null` would
/// not conflict — it would durably rewrite every ordinary entry it renewed (`PendingEntry`'s
/// "Serialization identity").
#[test]
fn s7_an_ordinary_pending_entry_reencodes_byte_identically() {
    let entry = decode_pending_witness(&ordinary(LEASE)).expect("an ordinary lease decodes");
    assert_eq!(entry.lease_expiry_millis, LEASE);
    assert_eq!(entry.owner, None);
    assert_eq!(entry.staged, None);

    // The entry every streaming write builds (`write.rs`) encodes to exactly what every build
    // before the two fields wrote — the `skip_serializing_if` that makes it so, asserted.
    assert_eq!(
        String::from_utf8_lossy(metadata::encode(&ordinary_entry(LEASE)).as_ref()),
        String::from_utf8_lossy(&ordinary(LEASE)),
        "an absent ownership field must be omitted, never spelled null"
    );
}

// ===========================================================================
// S8 — an owned entry re-encodes byte-identically across a renewal.
// ===========================================================================

/// On an owned value both fields are present, so `skip_serializing_if` never fires; what keeps a
/// renewal from rewriting more than the lease is the `sidx:` decode's canonical-bytes gate — a
/// foreign spelling of an owned entry is refused rather than accepted and then re-spelled by the
/// renewal's put.
#[test]
fn s8_an_owned_entry_reencodes_byte_identically_across_a_renewal() {
    let (_, _, entry) = decode_owned_witness(&key("a1"), &owned()).expect("an owned entry decodes");

    // A renewal moves the lease and nothing else: same owner, same staged placement.
    let renewed = OwnedEntry::new(entry.owner().clone(), RENEWED, entry.staged().clone());
    let renewed_bytes = metadata::encode(&renewed.to_pending());
    assert_eq!(
        String::from_utf8_lossy(renewed_bytes.as_ref()),
        String::from_utf8_lossy(&owned_value(RENEWED, "a1", &staged_json(RS_2_1, PLACEMENT))),
        "a renewal changed more than the lease"
    );
    let (_, _, reread) =
        decode_owned_witness(&key("a1"), &renewed_bytes).expect("the renewed entry decodes");
    assert_eq!(reread, renewed);

    // Two foreign spellings of the same owned entry — fields reordered, whitespace inserted. JSON
    // calls them equal and the shape decode reads them; the `sidx:` entry point refuses them.
    let reordered = format!(
        "{{\"owner\":\"{}\",\"lease_expiry_millis\":{LEASE},\"staged\":{}}}",
        hex32("a1"),
        staged_json(RS_2_1, PLACEMENT)
    )
    .into_bytes();
    let spaced = String::from_utf8(owned())
        .expect("the witness is UTF-8")
        .replace(",\"owner\"", ", \"owner\"")
        .into_bytes();
    for foreign in [reordered, spaced] {
        assert!(metadata::decode::<PendingEntry>(&foreign).is_ok());
        assert_eq!(
            decode_owned_entry(&key("a1"), &foreign).map(|(_, _, entry)| entry),
            Err(RecordError::NoncanonicalRecordValue { namespace: "sidx:" }),
            "a foreign spelling was accepted: {}",
            String::from_utf8_lossy(&foreign)
        );
    }
}

// ===========================================================================
// S9 — an owned entry is mintable from outside `wyrd-core`, through a checked path.
// ===========================================================================

/// This file is its own crate: it sees only `wyrd-core`'s public surface, exactly as the first
/// `sidx:` writer (#656–#659) will. The torn literal that same caller could assemble instead is
/// S3's second half.
#[test]
fn s9_an_owned_entry_is_minted_outside_the_crate_through_the_checked_path() {
    let minted = minted();
    let stored = minted.to_pending();
    assert_eq!(stored.owner, Some(upload_id("a1")));
    assert_eq!(stored.staged.as_ref(), Some(minted.staged()));
    assert_eq!(OwnedEntry::from_pending(&stored), Ok(minted.clone()));

    // The public validator refuses the other valid shape as well: an ordinary lease handed to it
    // is not an owned entry, whoever assembled it.
    assert_eq!(
        OwnedEntry::from_pending(&ordinary_entry(LEASE)),
        Err(RecordError::PendingEntryNamespaceMismatch {
            namespace: "sidx:",
            shape: "ordinary",
        })
    );

    // The minted value encodes to exactly the hand-authored witness — so every leg above is a
    // claim about what this writer path produces.
    let bytes = metadata::encode(&stored);
    assert_eq!(
        String::from_utf8_lossy(bytes.as_ref()),
        String::from_utf8_lossy(&owned())
    );
    // Both decoders agree on what it is: an owned entry under its own key, and never an
    // ordinary lease.
    let (_, _, decoded) =
        decode_owned_witness(&key("a1"), &bytes).expect("the minted entry decodes under sidx:");
    assert_eq!(decoded, minted);
    assert!(decode_pending_witness(&bytes).is_err());
}

/// PR #793 review — an ownership field spelled `null` is **refused**, not read as absent.
/// `Option<T>`'s own decode would map `"owner":null` to `None`, and the encoder never emits
/// that spelling back, so such a lease would decode, pass every rule, and be rewritten
/// field-for-field by the next `renew_pending` — a CAS that WINS while changing the stored
/// shape. Both surfaces refuse it (the wire is shared), so the accepted set stays exactly the
/// encoder's image. Negation: drop `deserialize_with` from either wire field — this test alone
/// fails.
#[test]
fn an_explicit_null_ownership_field_is_refused() {
    for (label, value) in [
        (
            "owner",
            format!("{{\"lease_expiry_millis\":{LEASE},\"owner\":null}}"),
        ),
        (
            "staged",
            format!("{{\"lease_expiry_millis\":{LEASE},\"staged\":null}}"),
        ),
        (
            "both",
            format!("{{\"lease_expiry_millis\":{LEASE},\"owner\":null,\"staged\":null}}"),
        ),
    ] {
        let err = decode_pending_witness(value.as_bytes())
            .expect_err("an explicit null ownership field must not decode");
        assert!(
            matches!(
                err,
                RecordError::MalformedRecordValue {
                    namespace: "pending:",
                    ..
                }
            ),
            "{label}: {err:?}"
        );
        assert!(
            metadata::decode::<PendingEntry>(value.as_bytes()).is_err(),
            "{label}: the store-wide decode read null as absent"
        );
    }
}
