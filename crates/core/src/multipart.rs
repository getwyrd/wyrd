//! The **multipart commit protocol**'s key space and the validated identity types it is
//! spelled in (proposal 0016,
//! `docs/design/proposals/draft/0016-multipart-commit-protocol.md`).
//!
//! This is slices 1–3 of 3 of issue #654's own re-split (itself slice 1 of 7 of #636). It is
//! deliberately **pure**: no [`wyrd_traits::MetadataStore`] call, no `WriteBatch`, no
//! `async fn`. After this module a reader can name every key the protocol will ever write
//! (`0016` §1, `:333-527`), parse it back, **and decode the record values that key space
//! names** — the `mpuctl` singleton ([`AdmissionRecord`], its [`Budget`] profile and the two
//! derivations that profile establishes, `0016:348`, `:1469-1470`), the in-flight lifecycle
//! records ([`SessionRecord`], [`SlotRecord`], [`PartRecord`], [`PartSummary`]), and the two
//! whose identity lives partly in their **key**: the retirement obligation ([`RetirePayload`])
//! and the owned staging entry ([`OwnedEntry`] — `sidx:`'s [`crate::metadata::PendingEntry`]
//! with its two ownership fields, its planned placement a [`StagedPlacement`]). It also
//! **answers** the protocol's verbs, in one typed vocabulary every later slice uses (issue
//! #693): decision 3's verb × state table as total pure functions ([`answer`], `0016:894-1037`),
//! the object's identity ([`multipart_etag`] — the composition ADR-0047 deferred,
//! `0016:3064-3070`), and the request identity a `Completed` tombstone answers a retry on
//! ([`complete_fingerprint`], `0016:898-908`).
//!
//! There is **no** `encode_record`/`decode_record` envelope, and this header's earlier forward
//! reference to one is withdrawn: `0016` §1 gives every value a **key-determined** shape
//! (`:333-356`) and a stored value carries no type tag, so a per-record arm would have nothing
//! to dispatch on. Each record type instead validates inside its own `Deserialize` over the
//! store-wide codec [`crate::metadata::encode`] / [`crate::metadata::decode`] — the shape
//! [`AdmissionRecord`] lands here and every later child repeats.
//!
//! **Two records break that shape, deliberately.** Most of the retirement obligation's rules
//! are relations against its own **key**, so it takes the key as a decoder parameter
//! ([`decode_retire_obligation`]) and carries **no** `Deserialize` at all — a decode that cannot
//! see the key cannot validate against it, and a payload obtained that way would be exactly the
//! value ADR-0045 decision 1 forbids. That is also the second reason a value-only dispatching
//! envelope could not have served this key space.
//!
//! The owned staging entry takes its key too ([`decode_owned_entry`]): its `owner` must be the
//! upload id the key names, and its shape must be the one its namespace holds. Its value *is*
//! the shared `pending:` record, which keeps a shape-level `Deserialize` — live `pending:`
//! readers and a stored corpus depend on it — so the namespace rule is carried by **one decode
//! entry point per namespace** instead: [`decode_owned_entry`] refuses an ordinary lease under
//! `sidx:`, and [`crate::metadata::decode_pending_entry`], which every `pending:` reader goes
//! through, refuses an owned entry under `pending:` — the rule the `pending:` writers also apply
//! to what they store.
//!
//! # The keyed classes (`0016` §1, `:333-527`)
//!
//! | Key | What it addresses |
//! |---|---|
//! | `mpuctl` | the fleet **admission ledger** singleton — no id, no parser |
//! | `mpu:<id>` | one **session** |
//! | `slot:<id>:<k>` | one **in-flight part slot**; the key space *is* the per-session cap |
//! | `part:<id>:<n>` | a **committed part** |
//! | `psum:<id>:<n>` | that part's **summary** |
//! | `sidx:<id>:<n>:<chunk>` | one **owned staging entry** ([`OwnedEntry`], stored as a [`crate::metadata::PendingEntry`] carrying `owner`/`staged`), under a prefix disjoint from `pending:` (`0016:475-491`) |
//! | `retire:bytes:<token>` | a **retirement obligation** ([`RetirePayload`]): orphan-mark bytes, then delete the naming records |
//! | `retire:records:<token>` | records to delete whose bytes something else protects (the same payload, records mode) |
//!
//! Every prefix here is disjoint from every other and from the pre-existing `inode:` /
//! `dirent:` / `pending:` / `bucket:` / `orphan:` (`metadata.rs:30-70`), `seg:` / `seggrp:`
//! (`metadata.rs:293-300`) and `desired:dserver:` (`custodian/src/desired_state.rs:33`),
//! and none is a prefix of another, so no `scan` returns a neighbour's records
//! (`0016:333-356`; ADR-0046 decision 1, disjoint first-class records under a disjoint
//! prefix — never an encoding smuggled into an existing namespace).
//!
//! # Structural validity is a type, not a convention (ADR-0045)
//!
//! Every component a key is built from arrives here as a **validated type** —
//! [`UploadId`], [`AttemptId`], [`PartNumber`], [`SlotIndex`], [`Digest`] — mirroring
//! [`crate::metadata::SegmentNonce`], whose doc records why (`metadata.rs:714-733`):
//! parsing the rule into the type is what makes an unvalidated value *unrepresentable*
//! rather than merely unlikely (ADR-0045, parse-don't-validate). Two consequences this
//! module leans on:
//!
//! * every key constructor is **total and infallible** — it cannot mint a key its own
//!   parser would reject, the property `seg_key` states at `metadata.rs:1219-1233`;
//! * `parse(key(x)) == x` for every keyed class, and a **non-canonical** spelling of the
//!   same record (`slot:<id>:7`, `slot:<id>:0000007`, `slot:<id>:+7`) parses as **no**
//!   record at all. Two spellings of one record would defeat a `require_absent` guard and
//!   hide a record from a bounded range scan — residue nothing enumerates and therefore
//!   nothing reclaims (**C-1**, `docs/principles.md` §5 C-1, the form
//!   `metadata.rs:724` cites it in).
//!
//! Structural validity is checked **at decode**, never by convention at a call site
//! (`0016:390-414`).
//!
//! # Nothing here is written yet — and the living-architecture doc says exactly that
//!
//! This module is the key **grammar** plus the record **shapes**: it has no writer, no store
//! call and no production consumer (the first writers are the store round trips, #656–#659).
//! An earlier revision of this header deferred the living-architecture update to "the slice
//! that first *persists* one"; that clause is **withdrawn**, because it is not what the doc or
//! the convention ended up saying. The living architecture doc describes the system **as it
//! is** (`docs/design/README.md:28`), and a persisted record *definition* is part of that
//! system the moment it is merged — `AGENTS.md:154-158` makes updating it in the same PR a
//! merge requirement, not a follow-up. Its metadata model
//! (`docs/design/architecture/05-building-block-view.md:202-204`) therefore records these
//! record **types** as landed ahead of their writers, along with the answer table and the
//! multipart ETag and Complete fingerprint a `Completed` session stores, and defers the
//! *protocol* — the fenced transitions, staged publication and the retirement drain — to the
//! proposal, whose §1 (`0016:333-356`) stays the normative description of this key space.
//!
//! One type here does reach a live path: the owned entry's value is the shared
//! [`crate::metadata::PendingEntry`], which every streaming write already puts under
//! `pending:`. Its two ownership fields are additive and omitted when absent, so every
//! `pending:` record written today decodes and re-encodes byte-identically. What changes on that
//! path is the namespace rule, applied in both directions: the `pending:` readers refuse an owned
//! or torn value rather than read it as an ordinary lease, and the two `pending:` writers refuse
//! to store one. An expiry sweep that meets such a value skips it, reclaiming nothing on it, and
//! goes on with the rest — `write::sweep_expired_leases` then reports each skipped key in its
//! error, and GC's expired-lease input names each on its audit seam. Neither runs by default in a
//! deployment: the first has no production caller, and GC reads `pending:` only when an operator
//! arms that input (`--gc-expired-pending`), so until then such a value simply stays in place.

use std::cmp::Ordering;
use std::fmt;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
// `sha2::Digest` is the trait `Sha256::new`/`update`/`finalize` come from (the peer usage,
// `crates/gateway-s3/src/crypto.rs:21`); imported anonymously so it never shadows this
// module's own `Digest`, the SHA-256 *value* type (ADR-0047).
use sha2::{Digest as _, Sha256};
use wyrd_traits::{ChunkId, DServerId, SCAN_CAP};

use crate::erasure;
use crate::metadata::{self, ChunkRef, EcScheme, InodeId, SegmentGroup};

// ===========================================================================
// 1. Errors — every structural violation is a typed error, never a value
//    (ADR-0045; `0016:390-414`)
// ===========================================================================

/// A structural violation of a multipart **key**, or of a stored multipart **record value**
/// — never a silently-corrected default (ADR-0045, parse-don't-validate; `0016:390-414`).
///
/// Every value-level variant names **one** rule, so a consumer can tell which rule a stored
/// record broke without parsing a message; that is what lets [`decode_admission_record`]
/// attribute a torn `mpuctl` to the exact relation it violates rather than to "undecodable".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
    /// A key whose prefix, field count or field syntax does not parse — including a key
    /// whose bytes are not UTF-8, a truncated key and one with a trailing component.
    MalformedKey {
        /// The namespace expected.
        namespace: &'static str,
        /// The key as UTF-8 (lossy), for the operator signal.
        key: String,
    },
    /// A token component (upload id, attempt id) that is not exactly [`TOKEN_HEX_LEN`]
    /// lowercase-hex characters.
    TokenNotHex {
        /// The rejected token.
        token: String,
    },
    /// A part number outside `[1, MAX_PART_NUMBER]` — the **format** bound the fixed-width
    /// key grammar can spell, never a live capacity knob.
    PartNumberOutOfRange {
        /// The number found.
        part_number: u64,
    },
    /// A slot index outside `[0, MAX_SLOT_INDEX]` — the **format** bound.
    SlotIndexOutOfRange {
        /// The index found.
        index: u64,
    },
    /// A `retire:` key whose mode component is neither `bytes` nor `records`
    /// (`0016:434-440`).
    UnknownRetireMode {
        /// The mode as read.
        mode: String,
    },
    /// A digest that is not 64 **lowercase**-hex characters (a SHA-256, ADR-0047).
    DigestNotHex {
        /// The rejected text.
        digest: String,
    },
    /// A record **value** whose bytes are not a well-formed record of its class at all: not
    /// JSON, the wrong shape, a field missing or of the wrong type or outside its wire type's
    /// range, or — under `deny_unknown_fields` — a field this build does not know.
    MalformedRecordValue {
        /// The record class expected (`mpuctl`).
        namespace: &'static str,
        /// The decoder's own message, for the operator signal.
        detail: String,
    },
    /// A record **value** that parses and passes every rule of its class, but whose bytes are
    /// not this codec's own spelling of the value they carry — fields reordered, whitespace
    /// inserted, an equivalent `\u` escape. JSON calls the spellings equal; a whole-record CAS
    /// on **exact bytes** (`0016:555-558`) does not, so a value only a foreign spelling can
    /// produce is one no decode→encode caller could ever CAS against: two stored spellings of
    /// one record are the same hazard for `require` that two key spellings are for
    /// `require_absent` (the module's own canonical-key rule above).
    NoncanonicalRecordValue {
        /// The record class expected.
        namespace: &'static str,
    },
    /// **G1** — a profile whose `max_part_chunks` is `0` (`0016:1466`, `> 0`). The
    /// **totality precondition** of the whole record rather than a peer of the rules below:
    /// at zero the `U_ref` of `0016:1469` is `0`, so the `MAX_SESSIONS` quotient of
    /// `0016:1470` has no divisor and the ledger's identity is undefined, not merely wrong.
    MaxPartChunksZero,
    /// **G2** — a profile whose `max_inflight_parts` is `0` (`0016:1471`, range `[1, …]`): no
    /// slot can ever be reserved, so no part can ever be committed and no session can ever
    /// progress.
    MaxInflightPartsZero,
    /// **G3** — a profile promising more parts per session than the `part:`/`psum:`/`sidx:`
    /// key grammar can address ([`MAX_PART_NUMBER`]): the session's later parts would name
    /// records no parser could read. A **format** bound of the encoding, never a live knob —
    /// `0016`'s knob table states no operator range for `MAX_PARTS_PER_SESSION` at all.
    PartsPerSessionUnaddressable {
        /// The cap found.
        max_parts_per_session: u32,
    },
    /// **G4** — a profile with more parts in flight than the session may ever hold
    /// (`0016:1471` clamp 1).
    InflightPartsExceedParts {
        /// The in-flight cap found.
        max_inflight_parts: u32,
        /// The per-session part cap it exceeds.
        max_parts_per_session: u32,
    },
    /// **G5** — a profile whose worst-case owned `sidx:` population per session,
    /// `max_inflight_parts × max_part_chunks`, is past `SCAN_CAP/2` (`0016:1471`, `:2098`):
    /// the per-session `scan("sidx:<id>:")` every teardown depends on would fail
    /// complete-or-fail-loud, stranding that session's residue with no pass that enumerates
    /// it.
    StagingRangeUnscannable {
        /// The owned population that profile can reach, exact.
        owned_sidx: u128,
    },
    /// **G6** — a profile whose staged-chunk ceiling is below one maximal part (`0016:1468`,
    /// the lower end of the settled range): at least one maximal part must remain stageable,
    /// or the ceiling refuses the very first part every session must be able to commit.
    StagedChunksBelowPart {
        /// The staged ceiling found.
        max_staged_chunks: u32,
        /// The per-part chunk cap it is below.
        max_part_chunks: u32,
    },
    /// **G7** — a profile whose reference budget `W_ref` is below one session's own
    /// worst-case footprint `U_ref` (`0016:1473`, the range `[U_ref, deployment RAM]`): the
    /// derivation yields a ledger that can never admit a session.
    BudgetBelowFootprint {
        /// The budget found.
        w_ref: u64,
        /// The per-session footprint it is below (`0016:1469`), exact — this is the one
        /// quantity a torn record can drive past `u64`, so it is reported at the width it was
        /// computed at rather than saturated into the field's.
        u_ref: u128,
    },
    /// **G8** — a `mpuctl` whose stored `max_sessions` is not what its **own** profile
    /// derives (`0016:1470`). The identity this record exists for: `max_sessions` is derived,
    /// never chosen, and a stored limit disagreeing with the profile beside it admits
    /// sessions past the memory bound the reconcile pass is sized for (`0016:2593`, X64).
    MaxSessionsNotDerived {
        /// The limit the record carries.
        stored: u64,
        /// What its own profile derives.
        derived: u64,
    },
    /// A `Completing` session's `publish_target` names a different `(parent, name)` than the
    /// session's own `parent`/`object` (`0016:350`, `:561-563`). `publish_target` is the dirent
    /// identity the fence will flip, never a frozen inode id — a record whose two spellings
    /// disagree would publish one client's upload under another key.
    PublishTargetKeyMismatch {
        /// The session's own parent bucket inode.
        session_parent: InodeId,
        /// The session's own object name.
        session_object: String,
        /// `publish_target.parent`.
        target_parent: InodeId,
        /// `publish_target.name`.
        target_name: String,
    },
    /// A session's `publish_target` carries a fence epoch that disagrees with the session's
    /// own `epoch` — the F18 class (`0016:350`, `:560-563`): `publish_target`'s epoch is what
    /// makes the `Completing` fence's segment-group nonce deterministic **for that attempt**,
    /// so a record whose two epochs disagree addresses another attempt's segment-group.
    PublishTargetEpochMismatch {
        /// The session's own epoch.
        session_epoch: u64,
        /// `publish_target`'s epoch.
        target_epoch: u64,
    },
    /// A `Completing` session whose `segments_written` cursor counts more segments than the
    /// fixed-width `seg:` key grammar can address — [`crate::metadata::MAX_SEGMENT_INDEX`]` +
    /// 1` records, indices `0..=MAX_SEGMENT_INDEX` (`metadata.rs:275-286`, the same
    /// format-bound-vs-capacity-knob line [`RecordError::PartsPerSessionUnaddressable`] draws
    /// for parts). No recovery can have written a segment record past that index, so a cursor
    /// claiming it describes writes that structurally cannot exist — trusted, it would let a
    /// resumed completer skip the whole segment-write phase and flip a root whose segment
    /// records are missing. The mutable capacity knob `MAX_ROOT_SEGMENTS` is deliberately
    /// **not** enforced here, for the same reason `attempts` does not consult
    /// `MAX_COMPLETE_ATTEMPTS`: lowering a knob must never make stored sessions unreadable.
    SegmentCursorUnaddressable {
        /// The cursor found.
        segments_written: u32,
    },
    /// A `PartRecord` chunk's stored `EcScheme::ReedSolomon` is not one
    /// [`crate::erasure::supported`] can encode/decode — `k == 0`, `m == 0`, or any other pair
    /// the coder rejects (ADR-0045's invariant table, the #285 class read applies at
    /// `crate::read::ReadError::InvalidEcScheme`, mirrored here at decode so untrusted stored
    /// geometry never reaches the read path at all).
    ChunkSchemeUnsupported {
        /// The chunk whose stored scheme is invalid.
        chunk_id: ChunkId,
        /// The rejected data-fragment count.
        k: u8,
        /// The parity-fragment count that accompanied it.
        m: u8,
    },
    /// A `SlotRecord` born already lapsed: `lease_expiry_millis <= reserved_at_millis`
    /// (`0016:349`). A slot in this shape is reapable the instant it is written, so a live
    /// part attempt can have its staging reclaimed out from under it.
    SlotLeaseAlreadyLapsed {
        /// When the slot was reserved.
        reserved_at_millis: u64,
        /// The lease expiry the record carries, at or before `reserved_at_millis`.
        lease_expiry_millis: u64,
    },
    /// A `PartRecord` whose own `len` does not equal the checked sum of its `chunks`' logical
    /// lengths (`0016:351`) — mirrors [`crate::metadata::SegmentRecord`]'s
    /// `SegmentLengthMismatch` (`metadata.rs:1170-1185`) for the analogous record here.
    PartLengthMismatch {
        /// The `len` the record declares.
        declared: u64,
        /// The checked sum of `chunks`' logical lengths.
        chunks: u64,
    },
    /// The checked sum of a `PartRecord`'s `chunks`' logical lengths overflows `u64` — a typed
    /// error rather than a silent wrap that a same-width comparison against `len` could then
    /// happily confirm. Mirrors `SegmentLengthOverflow` (`metadata.rs:1208-1218`).
    PartLengthOverflow {
        /// How many chunks the absurd list carries.
        chunks: usize,
    },
    /// A part-number set run whose bounds are reversed, `lo > hi` (`0016:382-388`). A reversed
    /// run names no part at all, and its endpoints are a second spelling of the empty set.
    PartNumberRunReversed {
        /// The run's lower bound.
        lo: u32,
        /// The run's upper bound.
        hi: u32,
    },
    /// A part-number set whose runs are not **strictly ascending and non-adjacent**
    /// (`0016:382-388`): the encoding is coalesced, so an out-of-order, overlapping or abutting
    /// run is a **second spelling** of one set — `[[1,2],[3,4]]` for `[[1,4]]` — and two
    /// spellings of one obligation defeat the `require_absent` installation guard exactly as two
    /// spellings of one key would (`0016:369-373`; the module's canonical-key rule in the
    /// header).
    PartNumberRunsNotCoalesced {
        /// The offending run's lower bound.
        lo: u32,
        /// The upper bound of the run before it.
        previous_hi: u32,
    },
    /// A retirement obligation, or one component of it, that owes **nothing** (`0016:355-356`):
    /// an empty payload, an explicitly empty part set ([`PartNumberSet::from_runs`]), a
    /// present-but-empty chunk list — the payload's own or a flat generation's, both judged by the
    /// module's `checked_chunks` — or a generation naming neither of its two reclamation sources
    /// ([`RetiredMap`]). A drain that met one would
    /// mark nothing, delete the obligation, and record the work as done: residue nothing drains,
    /// and no record left naming it.
    ///
    /// Each spelling is refused where it is read and reports **which** component owes nothing, so
    /// an operator is not left to find the empty list inside an otherwise plausible value.
    RetireObligationOwesNothing {
        /// Which part of the payload owes nothing (`payload` for the whole value).
        component: &'static str,
    },
    /// A `parts: "all"` wildcard outside the one shape that installs it — the `retire:bytes:`
    /// session teardown `{session, all}` the reaper's `Open` arm commits (`0016:2187`). The
    /// wildcard is an instruction to enumerate the session's own `part:<id>:` range **at drain
    /// time**, and only the teardown fence that installs it makes that range immutable
    /// (`0016:664`, `:2187`): without the teardown component the same obligation names whatever a
    /// still-live session happens to hold when the drain arrives, which is a set no writer chose.
    /// The mode half of the rule is [`RetireModeMismatch`](Self::RetireModeMismatch) — under
    /// `retire:records:` the wildcard would delete **every** part record of a session, including
    /// the staged parts whose records are the only protection their bytes have and the only
    /// source of their placements (`0016:919-921`, X104).
    RetireAllPartsWithoutSession,
    /// A retirement payload component stored under the **other** mode's key prefix
    /// (`0016:434-441`). The mode lives in the key precisely so this is a decode error and never
    /// a misread boolean: a record-mode component under `retire:bytes:` would orphan-mark bytes a
    /// live object still names, and a byte-mode component under `retire:records:` would delete
    /// the only records naming durable bytes with no orphan evidence ever written.
    RetireModeMismatch {
        /// The mode the key names.
        key_mode: RetireMode,
        /// The component that may not live under it.
        component: &'static str,
    },
    /// A retirement payload component whose **token scope** disagrees with the key's token
    /// (`0016:358-366`): a generation component under a session (`s:`) token, or a
    /// session-scoped component under a generation (`g:`) token. Either would let a drain
    /// reclaim one identity's data while clearing another identity's obligation
    /// (`0016:369-373`, outcome (a)).
    RetireTokenScopeMismatch {
        /// The token kind the key carries (`s:` or `g:`).
        token: &'static str,
        /// The component found under it.
        component: &'static str,
    },
    /// A session-scoped component whose part scope disagrees with the token's **optional
    /// `:<part-number>:<attempt-id>` suffix** (`0016:358-366`). The suffix exists only for the
    /// per-part obligations (a re-uploaded part's superseded chunks, a losing writer's
    /// compensation, `0016:659`, `:672`); the whole-session obligations a fence, a publication or
    /// a rollback installs carry no suffix (`0016:662-665`, `:2187`, `:2193`). A whole-session
    /// obligation filed under one part's token is cleared by that part's drain and is never
    /// enumerated by the session's own emptiness gate (`0016:374-380`); a per-part obligation
    /// filed session-wide names an attempt nothing can attribute.
    RetireTokenSuffixMismatch {
        /// The component found.
        component: &'static str,
        /// Whether the key's token named a part attempt.
        token_names_part: bool,
    },
    /// A generation payload under a `g:` token naming a **different** `(inode, version)`: the
    /// obligation would evidence one generation's fragments while clearing another's — one
    /// generation's bytes reclaimed and the other's obligation gone, with no record left naming
    /// either (`0016:369-373`, outcome (a)).
    RetireGenerationIdentityMismatch {
        /// The inode the key's token names.
        key_inode: InodeId,
        /// The version the key's token names.
        key_version: u64,
        /// The inode the payload names.
        payload_inode: InodeId,
        /// The version the payload names.
        payload_version: u64,
    },
    /// A `generation` payload naming **both** reclamation sources — an inline `chunks` list and a
    /// `segments` group — when a generation has exactly one ([`RetiredMap`]).
    ///
    /// The obligation mirrors the committed map it retires, and that map is the two-arm
    /// [`crate::metadata::ChunkMap`] (`metadata.rs:1014`): a flat root's chunks are inline and a
    /// segmented root's live in the `seg:` range its group names, with no inline list beside them.
    /// `0016`'s two spellings of the row (`:355` `chunks, segments?`; `:2417` `chunks?, segments`)
    /// are those two cases, not a union. A payload carrying both would leave the drain
    /// (#656–#659) to invent a meaning for a value no writer installs — orphan-mark the inline
    /// list *and* walk a segment range, for a generation only one of them ever described.
    RetireGenerationBothSources {
        /// The inode the generation names.
        inode: InodeId,
        /// The version it names.
        version: u64,
    },
    /// A `retire:records:` payload naming a segment group of a **different epoch** than its
    /// token's (`0016:2350-2380`).
    ///
    /// A segment group's epoch is the `Completing` fence epoch that wrote those segments
    /// (`0016:354`), and the obligation retiring them is installed **by the fence that ends that
    /// very attempt**, whose `require(mpu == Completing@E)` precondition is what the token's
    /// epoch records (`0016:663-665`, `:2357-2362`). So the token's epoch is `E` — the epoch
    /// whose segment keys the payload names — **exactly**. Admitting a window instead would give
    /// one obligation two legal keys, and `require_absent(retire:<mode>:<token>)` cannot refuse a
    /// second installation of an obligation spelled under a key it never looked at
    /// (`0016:369-373`): the same obligation is then installed and drained twice.
    ///
    /// Draining a misfiled one deletes a **different** attempt's `seg:` records — and a later
    /// attempt's may already have been adopted by a winning root flip, so the deletion strips a
    /// *published* object of its segment map while orphan-marking nothing: the F18 class the
    /// epoch-scoped key space exists to make impossible (`0016:2364-2380`).
    RetireSegmentEpochMismatch {
        /// The session epoch the key's token names.
        key_epoch: u64,
        /// The epoch of the segment group the payload names.
        segment_epoch: u64,
    },
    /// A [`crate::metadata::PendingEntry`] carrying **exactly one** of `owner` / `staged`
    /// (`0016:442-457`). Both (an owned `sidx:` entry) or neither (an ordinary `pending:` lease)
    /// are the only shapes any writer produces; a torn value leaves every reader — under either
    /// namespace — to guess which one it is, so it is refused under both.
    TornOwnedEntry {
        /// The ownership field the value carries.
        present: &'static str,
        /// The one it lacks.
        absent: &'static str,
    },
    /// A lease-bearing staging value whose **shape disagrees with the namespace naming it**
    /// (`0016:353`, `:442-457`): an owned entry (`owner` and `staged` present) read under
    /// `pending:` or handed to a `pending:` writer, or an ordinary lease (neither) read under
    /// `sidx:`. The two key spaces share one value shape by design, so which of the two a value
    /// is must be decided against its key: an owned entry accepted as an ordinary lease has its
    /// ownership erased by the next renewal's put or its fragments reclaimed by an expiry sweep,
    /// and an ordinary lease accepted under a session's `sidx:` range is residue attributed to a
    /// session that never staged it.
    PendingEntryNamespaceMismatch {
        /// The namespace the value was read under or offered to (`pending:` or `sidx:`).
        namespace: &'static str,
        /// The shape it carries (`owned` or `ordinary`).
        shape: &'static str,
    },
    /// An owned entry whose stored `owner` is not the upload id its own `sidx:` key names
    /// (`0016:353`). Honouring it would renew — or reclaim — one session's staged data under
    /// another session's identity, through the per-session `sidx:<upload-id>:` range that is the
    /// only way any pass enumerates owned entries (`0016:475-491`).
    OwnedEntryOwnerMismatch {
        /// The upload id the key names.
        key_owner: UploadId,
        /// The upload id the value claims.
        entry_owner: UploadId,
    },
    /// A [`StagedPlacement`] whose stored `EcScheme::ReedSolomon` is not one
    /// [`crate::erasure::supported`] can encode/decode — the rule
    /// [`RecordError::ChunkSchemeUnsupported`] applies to a committed chunk, applied to the
    /// **planned** geometry an owned entry carries (ADR-0045's invariant table, the #285 class).
    /// Reported without a chunk id because the value carries none: the id is in the key.
    StagedSchemeUnsupported {
        /// The rejected data-fragment count.
        k: u8,
        /// The parity-fragment count that accompanied it.
        m: u8,
    },
    /// A Complete's named-part list naming **no** part: there is no assembly to publish and
    /// no request identity a tombstone could match.
    NoPartsNamed,
    /// A named-part list naming one part number twice. Ascending part numbers are a Complete
    /// **validation** (`0016:707`, `:994`), and a repeat is not ascending: it is refused, never
    /// settled by whichever entry a map happened to keep — that would publish an assembly the
    /// client did not name.
    DuplicatePart {
        /// The part number named more than once.
        part_number: u32,
    },
    /// A named-part list whose part numbers are not strictly ascending (`0016:707`, `:994`).
    /// Refused, **never sorted**: sorting would publish, and fingerprint, an order the client
    /// never sent.
    PartsOutOfOrder {
        /// The part number found out of order.
        part_number: u32,
        /// The part number immediately before it in the list.
        previous: u32,
    },
    /// Text that is not the grammar of a composed [`MultipartEtag`], `<64 lowercase-hex>-<N>`
    /// with `N` in canonical decimal: no `-` separator, or a count that is not canonical — a
    /// sign, a leading zero, a non-digit or a second `-`, or no digits at all. A hex half that
    /// is not a digest is [`RecordError::DigestNotHex`].
    MultipartEtagMalformed {
        /// The rejected text.
        etag: String,
    },
    /// A composed [`MultipartEtag`] whose count is canonical decimal but outside
    /// `[1, MAX_PART_NUMBER]`. A Complete names a non-empty, strictly ascending list of part
    /// numbers the key space can address, so no assembly has zero parts or more than
    /// [`MAX_PART_NUMBER`]; a stored ETag claiming either names an assembly that cannot exist.
    EtagPartCountOutOfRange {
        /// The count as read — its canonical digits, deliberately **not** parsed into an
        /// integer: no integer width holds every canonical count, and one past the width would
        /// otherwise be reported as malformed rather than as the out-of-range number it is.
        count: String,
    },
}

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedKey { namespace, key } => {
                write!(f, "malformed {namespace} key {key:?}")
            }
            Self::TokenNotHex { token } => write!(
                f,
                "{token:?} is not {TOKEN_HEX_LEN} lowercase-hex characters (a 128-bit token)"
            ),
            Self::PartNumberOutOfRange { part_number } => write!(
                f,
                "part number {part_number} is outside the format bound [1, {MAX_PART_NUMBER}]"
            ),
            Self::SlotIndexOutOfRange { index } => write!(
                f,
                "slot index {index} is outside the format bound [0, {MAX_SLOT_INDEX}]"
            ),
            Self::UnknownRetireMode { mode } => write!(
                f,
                "retire: key mode {mode:?} is neither `bytes` nor `records`"
            ),
            Self::DigestNotHex { digest } => write!(
                f,
                "{digest:?} is not 64 lowercase-hex characters (a SHA-256 digest)"
            ),
            Self::MalformedRecordValue { namespace, detail } => {
                write!(f, "malformed {namespace} record value: {detail}")
            }
            Self::NoncanonicalRecordValue { namespace } => write!(
                f,
                "{namespace} record value decodes but is not this codec's spelling of it: a \
                 CAS on its re-encoded bytes could never match the store"
            ),
            Self::MaxPartChunksZero => write!(f, "`max_part_chunks` is zero, so U_ref is zero"),
            Self::MaxInflightPartsZero => {
                write!(f, "`max_inflight_parts` is zero: no slot can be reserved")
            }
            Self::PartsPerSessionUnaddressable {
                max_parts_per_session,
            } => write!(
                f,
                "`max_parts_per_session` {max_parts_per_session} is past the format bound \
                 {MAX_PART_NUMBER} the part: key grammar can address"
            ),
            Self::InflightPartsExceedParts {
                max_inflight_parts,
                max_parts_per_session,
            } => write!(
                f,
                "`max_inflight_parts` {max_inflight_parts} exceeds `max_parts_per_session` \
                 {max_parts_per_session}"
            ),
            Self::StagingRangeUnscannable { owned_sidx } => write!(
                f,
                "owned sidx: entries per session {owned_sidx} is past SCAN_CAP/2 ({SCAN_HALF})"
            ),
            Self::StagedChunksBelowPart {
                max_staged_chunks,
                max_part_chunks,
            } => write!(
                f,
                "`max_staged_chunks` {max_staged_chunks} is below one maximal part's \
                 `max_part_chunks` {max_part_chunks}"
            ),
            Self::BudgetBelowFootprint { w_ref, u_ref } => write!(
                f,
                "`w_ref` {w_ref} is below one session's worst-case footprint U_ref {u_ref}"
            ),
            Self::MaxSessionsNotDerived { stored, derived } => write!(
                f,
                "stored `max_sessions` {stored} is not the {derived} its own profile derives"
            ),
            Self::PublishTargetKeyMismatch {
                session_parent,
                session_object,
                target_parent,
                target_name,
            } => write!(
                f,
                "publish_target ({target_parent}, {target_name:?}) disagrees with the \
                 session's own ({session_parent}, {session_object:?})"
            ),
            Self::PublishTargetEpochMismatch {
                session_epoch,
                target_epoch,
            } => write!(
                f,
                "publish_target epoch {target_epoch} disagrees with the session's own epoch \
                 {session_epoch}"
            ),
            Self::SegmentCursorUnaddressable { segments_written } => write!(
                f,
                "`segments_written` {segments_written} is past the {} segment records the \
                 `seg:` key grammar can address",
                metadata::MAX_SEGMENT_INDEX as u64 + 1
            ),
            Self::ChunkSchemeUnsupported { chunk_id, k, m } => write!(
                f,
                "chunk {chunk_id:032x}: invalid stored EC scheme rs({k},{m}); unsupported by \
                 the erasure coder"
            ),
            Self::SlotLeaseAlreadyLapsed {
                reserved_at_millis,
                lease_expiry_millis,
            } => write!(
                f,
                "slot lease_expiry_millis {lease_expiry_millis} is at or before its own \
                 reserved_at_millis {reserved_at_millis}"
            ),
            Self::PartLengthMismatch { declared, chunks } => write!(
                f,
                "declared `len` {declared} does not equal the chunks' summed length {chunks}"
            ),
            Self::PartLengthOverflow { chunks } => {
                write!(f, "summing {chunks} chunks' logical lengths overflows u64")
            }
            Self::PartNumberRunReversed { lo, hi } => {
                write!(f, "part-number run [{lo}, {hi}] is reversed")
            }
            Self::PartNumberRunsNotCoalesced { lo, previous_hi } => write!(
                f,
                "part-number run starting at {lo} overlaps or abuts the run ending at \
                 {previous_hi}: the encoding is coalesced, strictly ascending, non-adjacent runs"
            ),
            Self::RetireObligationOwesNothing { component } => {
                write!(f, "the retirement obligation's `{component}` owes nothing")
            }
            Self::RetireAllPartsWithoutSession => write!(
                f,
                "the `{ALL_PARTS}` part-number wildcard is stored without the `session` \
                 teardown component: only the teardown that installs it freezes the part range \
                 it names"
            ),
            Self::RetireModeMismatch {
                key_mode,
                component,
            } => write!(
                f,
                "a `{component}` retirement component is stored under a `{}` key",
                String::from_utf8_lossy(key_mode.prefix())
            ),
            Self::RetireTokenScopeMismatch { token, component } => write!(
                f,
                "a `{component}` retirement component is stored under a `{token}` token: the \
                 component's scope disagrees with its key"
            ),
            Self::RetireTokenSuffixMismatch {
                component,
                token_names_part,
            } => write!(
                f,
                "a `{component}` retirement component is stored under a token that {} a part \
                 attempt",
                if *token_names_part {
                    "names"
                } else {
                    "does not name"
                }
            ),
            Self::RetireGenerationIdentityMismatch {
                key_inode,
                key_version,
                payload_inode,
                payload_version,
            } => write!(
                f,
                "a generation payload for inode {payload_inode} version {payload_version} is \
                 stored under the token for inode {key_inode} version {key_version}"
            ),
            Self::RetireGenerationBothSources { inode, version } => write!(
                f,
                "the retired generation for inode {inode} version {version} names both an inline \
                 chunk list and a segment group: a published map is one or the other"
            ),
            Self::RetireSegmentEpochMismatch {
                key_epoch,
                segment_epoch,
            } => write!(
                f,
                "a records obligation naming the epoch-{segment_epoch} segment group is stored \
                 under the session token for epoch {key_epoch}: the fence that ends an attempt \
                 installs its obligation, so the token names that attempt's own epoch"
            ),
            Self::TornOwnedEntry { present, absent } => write!(
                f,
                "pending entry carries `{present}` but not `{absent}`: both ownership fields or \
                 neither is the only valid shape"
            ),
            Self::PendingEntryNamespaceMismatch { namespace, shape } => write!(
                f,
                "a `{namespace}` value is never an {shape} pending entry, and this one is"
            ),
            Self::OwnedEntryOwnerMismatch {
                key_owner,
                entry_owner,
            } => write!(
                f,
                "owned entry names owner {entry_owner} but its sidx: key names upload id \
                 {key_owner}"
            ),
            Self::StagedSchemeUnsupported { k, m } => write!(
                f,
                "invalid staged EC scheme rs({k},{m}); unsupported by the erasure coder"
            ),
            Self::NoPartsNamed => write!(f, "the named-part list names no part"),
            Self::DuplicatePart { part_number } => write!(
                f,
                "part number {part_number} is named twice in a list that must be strictly \
                 ascending"
            ),
            Self::PartsOutOfOrder {
                part_number,
                previous,
            } => write!(
                f,
                "part number {part_number} follows {previous} in a list that must be strictly \
                 ascending: refused, never sorted"
            ),
            Self::MultipartEtagMalformed { etag } => write!(
                f,
                "{etag:?} is not `<64 lowercase-hex>-<N>` with a canonical decimal `N`, a \
                 composed multipart ETag"
            ),
            Self::EtagPartCountOutOfRange { count } => write!(
                f,
                "a multipart ETag's part count {count} is outside [1, {MAX_PART_NUMBER}]"
            ),
        }
    }
}

impl std::error::Error for RecordError {}

fn malformed_key(namespace: &'static str, key: &[u8]) -> RecordError {
    RecordError::MalformedKey {
        namespace,
        key: String::from_utf8_lossy(key).into_owned(),
    }
}

// ===========================================================================
// 2. The validated components every key is built from
// ===========================================================================

/// A 128-bit token's canonical textual length: 32 lowercase-hex characters
/// (`0016:493-497`). Taken from [`crate::metadata::SEG_NONCE_HEX_LEN`]
/// (`metadata.rs:289`) rather than restated, since an upload id, an attempt id and a
/// segment-group nonce are the same 128-bit-token shape minted the same way.
pub const TOKEN_HEX_LEN: usize = metadata::SEG_NONCE_HEX_LEN;

/// Whether `token` is a well-formed 128-bit token: exactly [`TOKEN_HEX_LEN`] lowercase-hex
/// characters (`0016:493-497`). Rejects an empty string, a short or long token, uppercase
/// hex, and — because `:` is never a hex digit — a token that embeds the key separator.
pub fn is_token(token: &str) -> bool {
    token.len() == TOKEN_HEX_LEN
        && token
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn require_token(token: String) -> Result<String, RecordError> {
    if is_token(&token) {
        Ok(token)
    } else {
        Err(RecordError::TokenNotHex { token })
    }
}

/// An **upload id**: the 128-bit token a gateway mints for one session, rendered as 32
/// lowercase hex characters (`0016:493-497`).
///
/// A validated type rather than a `String` for the reason
/// [`crate::metadata::SegmentNonce`] is (`metadata.rs:714-733`): it is the component every
/// per-session **range** is derived from (`slot:<id>:`, `part:<id>:`, `sidx:<id>:`), so an
/// id carrying the key separator would name another session's live range, and an empty one
/// would name every session's.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct UploadId(String);

impl UploadId {
    /// The validating constructor — the only way to obtain one.
    pub fn new(id: impl Into<String>) -> Result<Self, RecordError> {
        require_token(id.into()).map(Self)
    }

    /// The id as its 32 hex characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UploadId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// An **attempt id**: the 128-bit token one `UploadPart` attempt stamps into the `slot:`
/// record it reserved, so an ambiguous reserve is settled by re-reading rather than by
/// re-reserving a different index (`0016:349`). Same grammar as [`UploadId`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AttemptId(String);

impl AttemptId {
    /// The validating constructor — the only way to obtain one.
    pub fn new(id: impl Into<String>) -> Result<Self, RecordError> {
        require_token(id.into()).map(Self)
    }

    /// The id as its 32 hex characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AttemptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AttemptId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// The decimal width a part number is zero-padded to in `part:` / `psum:` / `sidx:` keys, so
/// the key's byte-lexicographic order equals part-number order — the property
/// `metadata.rs:270-273` states for `SEG_INDEX_WIDTH`, mirrored for this slice's key space.
///
/// **Six digits, deliberately, pinned at Plan.** The gateway seam is protocol-neutral
/// (ADR-0046 decision 6, `docs/design/adr/0046-bucket-model-real-namespace.md:54-57`:
/// `crates/gateway-core` admits no S3 vocabulary), so the *format* must clear every known
/// front door's ceiling with margin, not just S3's: S3 caps a multipart upload at 10,000
/// parts per its wire protocol, Azure block blobs at 50,000 committed / 100,000 staged
/// blocks. Five digits (S3's own headroom) does not clear Azure's; six does, with room to
/// spare. Each *protocol's* cap is enforced at admission as **capacity**, never here —
/// mirroring `SEG_INDEX_WIDTH` vs `MAX_ROOT_SEGMENTS` (`metadata.rs:270-321`). Widening the
/// key space later is a stored-format change with a migration, exactly as that constant's
/// doc records, so the headroom is bought now rather than borrowed.
pub const PART_NUMBER_WIDTH: usize = 6;

/// The largest part number the `part:`/`psum:`/`sidx:` key grammar can address — the whole
/// key space [`PART_NUMBER_WIDTH`] opens, `999_999`.
///
/// A **format** bound (a constant of the encoding), never the live `MAX_PARTS_PER_SESSION`
/// knob (#655's), which is enforced where a part is *admitted*. Mirrors
/// [`crate::metadata::MAX_SEGMENT_INDEX`] and its reasoning (`metadata.rs:275-286`).
pub const MAX_PART_NUMBER: u32 = 10u32.pow(PART_NUMBER_WIDTH as u32) - 1;

/// A **part number**: `[1, MAX_PART_NUMBER]`, the range the fixed-width key grammar can both
/// render and parse back. Zero is not a part (S3 numbers parts from 1) and a number past the
/// key space would name a `part:` record no parser could read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PartNumber(u32);

impl PartNumber {
    /// The validating constructor — the only way to obtain one.
    pub fn new(part_number: u32) -> Result<Self, RecordError> {
        if part_number == 0 || part_number > MAX_PART_NUMBER {
            return Err(RecordError::PartNumberOutOfRange {
                part_number: u64::from(part_number),
            });
        }
        Ok(Self(part_number))
    }

    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for PartNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for PartNumber {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(u32::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// The decimal width a `slot:` key's index is zero-padded to, so byte-lexicographic key
/// order equals index order (`metadata.rs:270-273`'s rule, mirrored).
///
/// **Six digits, pinned at Plan.** `MAX_INFLIGHT_PARTS`'s own clamp arithmetic
/// (`0016:1471`) reaches **≈524,288** at `MAX_PART_CHUNKS = 1` before the `SCAN_CAP`
/// byte-envelope clamp brings it back down — so the format must address at least that many
/// indices, or a legal (if extreme) `MAX_INFLIGHT_PARTS` would mint a slot index no key
/// could spell. Five digits tops out at 99,999, under that figure; six (`999_999`) clears
/// it with margin, mirroring the `PART_NUMBER_WIDTH` headroom rule above.
pub const SLOT_INDEX_WIDTH: usize = 6;

/// The largest index the `slot:` key grammar can address — the whole key space
/// [`SLOT_INDEX_WIDTH`] opens, `999_999`. A **format** bound: the live `MAX_INFLIGHT_PARTS`
/// (#655's) is enforced at *reservation*, never at decode, because 0016 permits lowering it
/// while live sessions still hold indices above the new cap (`0016:390-402`) — and a record
/// that stopped decoding the moment a knob dropped could no longer be renewed, committed or
/// torn down.
pub const MAX_SLOT_INDEX: u32 = 10u32.pow(SLOT_INDEX_WIDTH as u32) - 1;

/// An **in-flight slot index**: `[0, MAX_SLOT_INDEX]`. Index 0 is a real slot (the key space
/// *is* the per-session cap, `0016:349`), so only the upper bound rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SlotIndex(u32);

impl SlotIndex {
    /// The validating constructor — the only way to obtain one.
    pub fn new(index: u32) -> Result<Self, RecordError> {
        if index > MAX_SLOT_INDEX {
            return Err(RecordError::SlotIndexOutOfRange {
                index: u64::from(index),
            });
        }
        Ok(Self(index))
    }

    /// The index.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for SlotIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for SlotIndex {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(u32::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// A SHA-256 digest, stored as 64 **lowercase**-hex characters.
///
/// The basis ADR-0047 settled for Wyrd's opaque change token — **never MD5**. A validated
/// type because a digest's *shape* is structural: a short or uppercase digest is not a
/// value any composition could use. The two compositions this module computes over part
/// digests are [`multipart_etag`] and [`complete_fingerprint`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    /// A digest from its 32 raw bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The SHA-256 of everything `feed` writes into the hasher — the **one** place this
    /// module names the algorithm (ADR-0047's basis), shared by [`multipart_etag`] and
    /// [`complete_fingerprint`]. The hasher is fed piece by piece, so a composition over a
    /// long part list is never first copied into one buffer.
    fn sha256(feed: impl FnOnce(&mut Sha256)) -> Self {
        let mut hasher = Sha256::new();
        feed(&mut hasher);
        Self(hasher.finalize().into())
    }

    /// The validating parser: exactly 64 lowercase-hex characters. Rejects uppercase hex
    /// (a second spelling of one digest), any other length, and any non-hex byte.
    pub fn from_hex(text: &str) -> Result<Self, RecordError> {
        let not_hex = || RecordError::DigestNotHex {
            digest: text.to_string(),
        };
        let raw = text.as_bytes();
        if raw.len() != 64 {
            return Err(not_hex());
        }
        let mut out = [0u8; 32];
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = lowercase_hex_digit(raw[2 * i]).ok_or_else(not_hex)?;
            let lo = lowercase_hex_digit(raw[2 * i + 1]).ok_or_else(not_hex)?;
            *slot = hi * 16 + lo;
        }
        Ok(Self(out))
    }

    /// The 32 raw digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The canonical lowercase-hex rendering.
    pub fn to_hex(&self) -> String {
        hex_lower(&self.0)
    }
}

/// The 16 lowercase-hex characters, in nibble order — the **one** table [`hex_lower`]
/// renders from, so no rendering path can fall back to a stand-in character.
const HEX_DIGITS: [u8; 16] = *b"0123456789abcdef";

/// One lowercase-hex character back to its nibble; `None` for every other byte —
/// uppercase included, since `A` and `a` would otherwise be two spellings of one digest.
/// The exact inverse of [`HEX_DIGITS`]; the test asserts that over all 256 byte values.
fn lowercase_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::from_hex(&String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// Bytes as lowercase hex — the rendering ADR-0047 settled for the change token, matching
/// the wrapper style `crates/gateway-s3/src/crypto.rs:21-60` carries for this workspace's
/// other SHA-256 usage. Rendering a digest already in hand never needs `sha2` itself.
///
/// Total by construction: a nibble indexes the private `HEX_DIGITS` table directly, so there
/// is **no** fallback character a rendering bug could hide behind. A digest that rendered a
/// wrong character for some byte value would be a second identity for one set of bytes,
/// which is the same two-spellings-of-one-record fault the key grammar refuses (C-1).
pub fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

// ===========================================================================
// 3. Keys and their canonical parsers
// ===========================================================================

/// The singleton admission-ledger key (`0016:348`). Never deleted; an absent record reads
/// as `{count: 0}` (a later slice's concern — this module states only the key). Its
/// **lack** of a trailing `:` is exactly what keeps it disjoint from [`MPU_PREFIX`]: see
/// that constant's doc.
pub const MPUCTL_KEY: &[u8] = b"mpuctl";

/// Key prefix for session records. The trailing `:` is what keeps this **disjoint from**
/// [`MPUCTL_KEY`] — `"mpuctl".starts_with("mpu:")` is `false` because `mpuctl`'s 4th byte is
/// `c`, not `:` — so `scan(MPU_PREFIX)` can never return the admission singleton
/// (`0016:342-344`).
pub const MPU_PREFIX: &[u8] = b"mpu:";
/// Key prefix for in-flight part slots.
pub const SLOT_PREFIX: &[u8] = b"slot:";
/// Key prefix for committed part records.
pub const PART_PREFIX: &[u8] = b"part:";
/// Key prefix for committed part summaries.
pub const PSUM_PREFIX: &[u8] = b"psum:";
/// Key prefix for owned staging entries — **disjoint from `pending:`**, which is the whole
/// point of the class: no global `scan("pending:")` (the restore pass, the expiry sweep)
/// ever enumerates an owned entry (`0016:475-491`).
pub const SIDX_PREFIX: &[u8] = b"sidx:";
/// Key prefix for byte-mode retirement obligations: orphan-mark, then delete the naming
/// records (`0016:355`).
pub const RETIRE_BYTES_PREFIX: &[u8] = b"retire:bytes:";
/// Key prefix for record-mode retirement obligations: delete records only — never
/// orphan-mark (`0016:356`).
pub const RETIRE_RECORDS_PREFIX: &[u8] = b"retire:records:";

/// A **fixed-width** `width`-digit decimal: exactly `width` ASCII digits — no `+`/`-` sign,
/// no short spelling, no over-wide one. This is the rule the zero-padded key fields are
/// checked against, and it is what makes byte-lexicographic key order equal numeric order
/// (`metadata.rs:270-273`). `u32::from_str` alone would accept `+7`, `7` and `0000007` as
/// three spellings of one record.
fn fixed_width_u32(text: &str, width: usize) -> Option<u32> {
    if text.len() != width || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// A decimal in **canonical** form: ASCII digits only (no sign), and no leading zero unless
/// the value *is* `0` — the rule `parse_seg_key` applies to its own un-padded epoch
/// (`metadata.rs:1296-1300`), applied to every variable-width decimal this module parses (a
/// chunk id, a retirement token's epoch, version and per-part-suffix part number).
///
/// Stated here rather than shared with `metadata::parse_canonical_u64`
/// (`metadata.rs:1310-1318`), which is **private** and `u64`-only: making it shared is an
/// edit to `metadata.rs`, and this slice's scope pins that file untouched. The child that
/// first *writes* one of these records is where the two become one generic helper; until
/// then the semantics are pinned identical by the same rejection table
/// (`crates/core/tests/multipart_keys.rs`), digit-for-digit.
fn canonical_decimal<T: std::str::FromStr>(text: &str) -> Option<T> {
    if !is_canonical_decimal(text) {
        return None;
    }
    text.parse().ok()
}

/// The **grammar** half of `canonical_decimal` on its own: whether `text` spells a decimal in
/// canonical form, whatever its magnitude. Split out for the one caller that must tell a
/// non-canonical spelling from a canonical number too wide for its integer type —
/// [`MultipartEtag::parse`]'s count, where the second is an out-of-range count, not a
/// malformed one. One rule, so the two can never disagree about what "canonical" means.
fn is_canonical_decimal(text: &str) -> bool {
    !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text.len() == 1 || !text.starts_with('0'))
}

/// Split `key` into exactly `fields` `:`-separated components after `prefix`, failing closed
/// on a non-UTF-8 key, a missing prefix, or the wrong field count (a truncated key, or one
/// with a trailing component) — the shape every keyed-record parser below shares.
fn split_key<'a>(
    key: &'a [u8],
    prefix: &str,
    namespace: &'static str,
    fields: usize,
) -> Result<Vec<&'a str>, RecordError> {
    let text = std::str::from_utf8(key).map_err(|_| malformed_key(namespace, key))?;
    let rest = text
        .strip_prefix(prefix)
        .ok_or_else(|| malformed_key(namespace, key))?;
    let parts: Vec<&str> = rest.splitn(fields, ':').collect();
    if parts.len() != fields {
        return Err(malformed_key(namespace, key));
    }
    Ok(parts)
}

/// Key for a session record: `mpu:<upload-id>`.
pub fn mpu_key(upload_id: &UploadId) -> Vec<u8> {
    format!("mpu:{upload_id}").into_bytes()
}

/// Parse an [`mpu_key`] back into its upload id.
pub fn parse_mpu_key(key: &[u8]) -> Result<UploadId, RecordError> {
    let fields = split_key(key, "mpu:", "mpu:", 1)?;
    UploadId::new(fields[0])
}

/// Key for one in-flight part slot: `slot:<upload-id>:<index>`, zero-padded to
/// [`SLOT_INDEX_WIDTH`].
pub fn slot_key(upload_id: &UploadId, index: SlotIndex) -> Vec<u8> {
    format!(
        "slot:{upload_id}:{:0width$}",
        index.get(),
        width = SLOT_INDEX_WIDTH
    )
    .into_bytes()
}

/// The bounded per-session slot range: `slot:<upload-id>:` (`0016:349`).
pub fn slot_range(upload_id: &UploadId) -> Vec<u8> {
    format!("slot:{upload_id}:").into_bytes()
}

/// Parse a [`slot_key`] back into `(upload_id, index)`, rejecting every non-canonical
/// spelling of one slot.
pub fn parse_slot_key(key: &[u8]) -> Result<(UploadId, SlotIndex), RecordError> {
    let fields = split_key(key, "slot:", "slot:", 2)?;
    let index =
        fixed_width_u32(fields[1], SLOT_INDEX_WIDTH).ok_or_else(|| malformed_key("slot:", key))?;
    Ok((UploadId::new(fields[0])?, SlotIndex::new(index)?))
}

fn part_scoped_key(prefix: &str, upload_id: &UploadId, part_number: PartNumber) -> Vec<u8> {
    format!(
        "{prefix}{upload_id}:{:0width$}",
        part_number.get(),
        width = PART_NUMBER_WIDTH
    )
    .into_bytes()
}

fn parse_part_scoped_key(
    key: &[u8],
    prefix: &'static str,
    namespace: &'static str,
) -> Result<(UploadId, PartNumber), RecordError> {
    let fields = split_key(key, prefix, namespace, 2)?;
    let part_number = fixed_width_u32(fields[1], PART_NUMBER_WIDTH)
        .ok_or_else(|| malformed_key(namespace, key))?;
    Ok((UploadId::new(fields[0])?, PartNumber::new(part_number)?))
}

/// Key for a committed part: `part:<upload-id>:<part-number>`, zero-padded to
/// [`PART_NUMBER_WIDTH`] so a range read is in part-number order.
pub fn part_key(upload_id: &UploadId, part_number: PartNumber) -> Vec<u8> {
    part_scoped_key("part:", upload_id, part_number)
}

/// The bounded per-session part range: `part:<upload-id>:`.
pub fn part_range(upload_id: &UploadId) -> Vec<u8> {
    format!("part:{upload_id}:").into_bytes()
}

/// Parse a [`part_key`] back into `(upload_id, part_number)`.
pub fn parse_part_key(key: &[u8]) -> Result<(UploadId, PartNumber), RecordError> {
    parse_part_scoped_key(key, "part:", "part:")
}

/// Key for a committed part's summary: `psum:<upload-id>:<part-number>`.
pub fn psum_key(upload_id: &UploadId, part_number: PartNumber) -> Vec<u8> {
    part_scoped_key("psum:", upload_id, part_number)
}

/// The bounded per-session summary range: `psum:<upload-id>:`.
pub fn psum_range(upload_id: &UploadId) -> Vec<u8> {
    format!("psum:{upload_id}:").into_bytes()
}

/// Parse a [`psum_key`] back into `(upload_id, part_number)`.
pub fn parse_psum_key(key: &[u8]) -> Result<(UploadId, PartNumber), RecordError> {
    parse_part_scoped_key(key, "psum:", "psum:")
}

/// Key for one **owned staging entry**: `sidx:<upload-id>:<part-number>:<chunk-id>`. The
/// `<part-number>` component is what lets a reclaimer attribute residue to the part attempt
/// that staged it (`0016:353`).
pub fn sidx_key(upload_id: &UploadId, part_number: PartNumber, chunk: ChunkId) -> Vec<u8> {
    format!(
        "sidx:{upload_id}:{:0width$}:{chunk}",
        part_number.get(),
        width = PART_NUMBER_WIDTH
    )
    .into_bytes()
}

/// The bounded per-session owned range: `sidx:<upload-id>:` — the **only** way any pass
/// reads owned entries; there is no global scan of them anywhere (`0016:475-491`).
pub fn sidx_range(upload_id: &UploadId) -> Vec<u8> {
    format!("sidx:{upload_id}:").into_bytes()
}

/// Parse a [`sidx_key`] back into `(upload_id, part_number, chunk_id)`. The chunk-id
/// component is **canonical** decimal, not fixed-width — `7` and `007` must never both
/// parse, the same canonicality rule the padded fields enforce by width instead.
pub fn parse_sidx_key(key: &[u8]) -> Result<(UploadId, PartNumber, ChunkId), RecordError> {
    let fields = split_key(key, "sidx:", "sidx:", 3)?;
    let part_number =
        fixed_width_u32(fields[1], PART_NUMBER_WIDTH).ok_or_else(|| malformed_key("sidx:", key))?;
    let chunk: ChunkId = canonical_decimal(fields[2]).ok_or_else(|| malformed_key("sidx:", key))?;
    Ok((
        UploadId::new(fields[0])?,
        PartNumber::new(part_number)?,
        chunk,
    ))
}

// ===========================================================================
// 4. The `retire:` token grammar (`0016:358-380`)
// ===========================================================================

/// Which of the two retirement modes an obligation is in — read from the **key**, never a
/// field: a boolean misread once is silent data loss, a malformed key prefix is an error at
/// decode (`0016:434-440`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireMode {
    /// Orphan-mark the bytes, then delete the records that named them.
    Bytes,
    /// Delete records only — **never** orphan-mark.
    Records,
}

impl RetireMode {
    /// Every mode, for a total dispatch.
    pub const ALL: [Self; 2] = [Self::Bytes, Self::Records];

    /// The key prefix this mode lives under.
    pub const fn prefix(self) -> &'static [u8] {
        match self {
            Self::Bytes => RETIRE_BYTES_PREFIX,
            Self::Records => RETIRE_RECORDS_PREFIX,
        }
    }
}

/// Classify the **mode component** alone: which of the two prefixes the key opens with, or
/// which of the two errors it earns. Deliberately **private** — a mode is meaningful only
/// once the key naming it decodes as a whole, which is what [`parse_retire_mode`] answers;
/// nothing outside this module may act on a prefix match.
///
/// The two failures are **different errors**, deliberately, because they are different
/// faults for the drain that meets one: [`RecordError::UnknownRetireMode`] means a key in
/// the `retire:` namespace named a third mode (a record whose disposal rule this build does
/// not know — the drain must not guess between orphan-marking and record-only deletion),
/// while [`RecordError::MalformedKey`] means the key is not a `retire:` key at all (a
/// neighbour's record, or a truncated `retire:bytes` with no token). Reporting the second as
/// "mode `mpu` is neither" would name a mode nothing wrote and send an operator looking for a
/// retirement obligation that does not exist.
fn retire_mode_prefix(key: &[u8]) -> Result<RetireMode, RecordError> {
    if key.starts_with(RETIRE_BYTES_PREFIX) {
        return Ok(RetireMode::Bytes);
    }
    if key.starts_with(RETIRE_RECORDS_PREFIX) {
        return Ok(RetireMode::Records);
    }
    let malformed = || malformed_key("retire:", key);
    let rest = std::str::from_utf8(key)
        .ok()
        .and_then(|text| text.strip_prefix("retire:"))
        .ok_or_else(malformed)?;
    // `retire:<mode>:…` — a mode component exists, and it is neither of the two.
    let (mode, _) = rest.split_once(':').ok_or_else(malformed)?;
    Err(RecordError::UnknownRetireMode {
        mode: mode.to_string(),
    })
}

/// Parse the mode out of a `retire:` key, failing closed on **every** other spelling — the
/// boundary the mode-in-the-key argument rests on: the drain dispatches on the mode and
/// "treats a `retire:` key it cannot parse as an error, never as a default"
/// (`0016:438-440`).
///
/// It therefore answers a mode only for a key that decodes **whole**, token included
/// ([`parse_retire_key`]) — a prefix match is not a mode. `retire:bytes:` with no token, and
/// `retire:bytes:` followed by bytes that are not UTF-8, are `retire:`-namespace keys that
/// name no obligation, so they are [`RecordError::MalformedKey`], not `Ok(Bytes)`: a caller
/// that dispatched on the prefix alone (the retirement drain of #656–#659) would orphan-mark
/// the fragments of a key it cannot read, and orphan-marking on a guess is the permanent,
/// data-losing failure mode C-1 refuses (`docs/principles.md` §5 C-1). Deciding the mode by
/// the same decode that yields the token is what keeps the two answers from ever disagreeing
/// — one spelling, one decision, as everywhere else in this module (ADR-0045).
pub fn parse_retire_mode(key: &[u8]) -> Result<RetireMode, RecordError> {
    parse_retire_key(key).map(|(mode, _)| mode)
}

/// A retirement token, whose grammar makes reuse impossible (`0016:358-380`): every
/// component is minted once — an epoch is bumped by every fence, a `(part, attempt)` pair
/// belongs to one `UploadPart`, an `(inode, version)` pair is produced by exactly one
/// publication. Installation is `require_absent(retire:<mode>:<token>)`, so a collision is a
/// `Conflict` the installer classifies, never a silent overwrite that would replace one
/// obligation's payload with another's and permanently lose the reclamation evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetireToken {
    /// A **session-scoped** obligation: `s:<upload-id>:<epoch>` — the whole-session teardown
    /// an epoch's fence installs — or, with the optional suffix,
    /// `s:<upload-id>:<epoch>:<part-number>:<attempt-id>` for a **per-part** obligation (a
    /// re-uploaded part's superseded chunks, a losing writer's compensation).
    ///
    /// Its part number is **canonical** decimal, not the zero-padded fixed width the
    /// `part:`/`psum:`/`sidx:` keys use, because the two grammars answer different questions:
    /// those keys are read as *ranges in part-number order*, where padding is what makes byte
    /// order equal numeric order, while the only `retire:` range anything reads is the
    /// session's emptiness gate `retire:<mode>:s:<upload-id>:` (`0016:374-380`), which is
    /// order-free. Canonicality — one spelling per token, so `require_absent` cannot be
    /// defeated — is preserved either way: the leading-zero rule here, the width rule there.
    Session {
        /// The owning session's upload id.
        upload_id: UploadId,
        /// The epoch whose fence installed the obligation.
        epoch: u64,
        /// The part attempt, for a per-part obligation.
        part: Option<(PartNumber, AttemptId)>,
    },
    /// A **superseded or deleted generation**: `g:<inode-id>:<version>`.
    Generation {
        /// The inode.
        inode: InodeId,
        /// Its version.
        version: u64,
    },
}

impl fmt::Display for RetireToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session {
                upload_id,
                epoch,
                part: None,
            } => write!(f, "s:{upload_id}:{epoch}"),
            Self::Session {
                upload_id,
                epoch,
                part: Some((part_number, attempt_id)),
            } => write!(f, "s:{upload_id}:{epoch}:{part_number}:{attempt_id}"),
            Self::Generation { inode, version } => write!(f, "g:{inode}:{version}"),
        }
    }
}

/// Key for a retirement obligation: `retire:<mode>:<token>`.
pub fn retire_key(mode: RetireMode, token: &RetireToken) -> Vec<u8> {
    let mut key = mode.prefix().to_vec();
    key.extend_from_slice(token.to_string().as_bytes());
    key
}

/// The bounded session-scoped obligation range for one mode: `retire:<mode>:s:<upload-id>:`.
/// A session's terminal delete gate is two of these emptiness reads, never a walk of the
/// whole `retire:` namespace, which is deliberately not cardinality-bounded
/// (`0016:374-380`).
pub fn retire_session_range(mode: RetireMode, upload_id: &UploadId) -> Vec<u8> {
    let mut key = mode.prefix().to_vec();
    key.extend_from_slice(format!("s:{upload_id}:").as_bytes());
    key
}

/// Parse a [`retire_key`] back into `(mode, token)`, failing closed on any other spelling: an
/// absent or truncated token, a trailing component, a non-canonical epoch/version/part-number
/// (a `+` sign or a leading zero), a malformed upload/attempt id, or a token whose bytes are
/// not UTF-8. This is the **whole** `retire:` decode — [`parse_retire_mode`] is this function
/// keeping only its first half of the answer, never a cheaper prefix test.
pub fn parse_retire_key(key: &[u8]) -> Result<(RetireMode, RetireToken), RecordError> {
    let mode = retire_mode_prefix(key)?;
    let text = std::str::from_utf8(key).map_err(|_| malformed_key("retire:", key))?;
    let rest = &text[mode.prefix().len()..];
    let fields: Vec<&str> = rest.split(':').collect();
    let malformed = || malformed_key("retire:", key);
    let token = match fields.as_slice() {
        ["s", upload_id, epoch] => RetireToken::Session {
            upload_id: UploadId::new(*upload_id)?,
            epoch: canonical_decimal(epoch).ok_or_else(malformed)?,
            part: None,
        },
        ["s", upload_id, epoch, part_number, attempt_id] => RetireToken::Session {
            upload_id: UploadId::new(*upload_id)?,
            epoch: canonical_decimal(epoch).ok_or_else(malformed)?,
            part: Some((
                PartNumber::new(canonical_decimal(part_number).ok_or_else(malformed)?)?,
                AttemptId::new(*attempt_id)?,
            )),
        },
        ["g", inode, version] => RetireToken::Generation {
            inode: canonical_decimal(inode).ok_or_else(malformed)?,
            version: canonical_decimal(version).ok_or_else(malformed)?,
        },
        _ => return Err(malformed()),
    };
    Ok((mode, token))
}

// ===========================================================================
// 5. The admission ledger — the `mpuctl` record VALUE (`0016:348`)
// ===========================================================================

/// Half [`SCAN_CAP`] — the cardinality ceiling `0016` states two of this record's rules
/// against: the per-session owned-`sidx:` population (`0016:1471`, `:2098`) and the
/// `MAX_SESSIONS` clamp that keeps the reaper's `scan("mpu:")` inside one complete-or-fail
/// scan (`0016:1470`).
///
/// [`SCAN_CAP`] earns a place at **decode** where a live capacity knob does not: it is a
/// **seam** constant, documented in the trait crate as "a correctness constraint, not a
/// tuning knob" (`crates/traits/src/lib.rs:272-286`) — one number every backend of the trait
/// must agree on, not a number a deployment chooses. Refusing a stored record against it is
/// therefore refusing it against the format's own arithmetic, and no operator action can make
/// a durable ledger unreadable (`0016:390-402`, the boundary
/// [`crate::metadata::MAX_ROOT_SEGMENTS`] draws for the other direction).
const SCAN_HALF: u64 = (SCAN_CAP as u64) / 2;

/// The budget **profile** tuple `mpuctl` stores and every admitter and custodian compares
/// whole (`0016:348`): `(W_ref, MAX_PART_CHUNKS, MAX_PARTS_PER_SESSION, MAX_INFLIGHT_PARTS,
/// MAX_STAGED_CHUNKS)`. Stored rather than derived per gateway because equal quotients can
/// hide unequal footprints, so a rolling configuration change cannot leave two gateways
/// enforcing different bounds (`0016:2605`, X76; `0016:2593`, X64).
///
/// The fields are private behind the one fallible conversion every surface funnels through
/// (`TryFrom<BudgetWire>`, the rules `Budget::checked_rules` states), so no `Budget` exists
/// whose own derivations are undefined: [`Budget::u_ref`] and [`Budget::max_sessions`] are
/// total for every value of this type, and a tuple that would make them otherwise is an error
/// at the boundary rather than a value inside the program (ADR-0045, parse-don't-validate). A
/// writer-side constructor is deliberately absent — the first writers are the store round
/// trips (#656–#659), and a knob-range check over an operator's *configuration* is a
/// different boundary, #508's and #655's (`0016:1458-1466`).
///
/// The wire shape is **closed** (`deny_unknown_fields`) for the reason [`AdmissionRecord`]
/// records: this tuple is part of the value CAS compares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BudgetWire")]
pub struct Budget {
    w_ref: u64,
    max_part_chunks: u32,
    max_parts_per_session: u32,
    max_inflight_parts: u32,
    max_staged_chunks: u32,
}

impl Budget {
    /// `W_ref` — the reconcile pass's staged-reference memory budget, in chunk-refs.
    pub const fn w_ref(&self) -> u64 {
        self.w_ref
    }

    /// `MAX_PART_CHUNKS` — chunk-refs one `part:` record may hold.
    pub const fn max_part_chunks(&self) -> u32 {
        self.max_part_chunks
    }

    /// `MAX_PARTS_PER_SESSION` — committed parts one session may hold.
    pub const fn max_parts_per_session(&self) -> u32 {
        self.max_parts_per_session
    }

    /// `MAX_INFLIGHT_PARTS` — the `slot:` key space, hence parts in flight per session.
    pub const fn max_inflight_parts(&self) -> u32 {
        self.max_inflight_parts
    }

    /// `MAX_STAGED_CHUNKS` — chunk-refs a session may hold in committed `part:` records.
    pub const fn max_staged_chunks(&self) -> u32 {
        self.max_staged_chunks
    }

    /// The worst-case **owned `sidx:` population of one session** — `MAX_INFLIGHT_PARTS ×
    /// MAX_PART_CHUNKS`, the quantity `0016` bounds by `SCAN_CAP/2` (`0016:1471`, `:2098`) and
    /// charges twice in [`Budget::u_ref_exact`]'s second term (`0016:1469`).
    ///
    /// **One definition, used by both**, so the rule (G5) and the charge can never disagree
    /// about what "in-flight owned refs" means — the reason `checked_chunk_bytes` exists for
    /// the other cross-checked quantity in this repo (`metadata.rs:1208-1218`).
    fn inflight_owned_refs(&self) -> u128 {
        u128::from(self.max_inflight_parts) * u128::from(self.max_part_chunks)
    }

    /// `U_ref` in **exact** integers, verbatim `0016:1469`:
    ///
    /// ```text
    /// U_ref = min( (MAX_PARTS_PER_SESSION + MAX_INFLIGHT_PARTS) x MAX_PART_CHUNKS ,
    ///              MAX_STAGED_CHUNKS + 2 x MAX_INFLIGHT_PARTS x MAX_PART_CHUNKS )
    /// ```
    ///
    /// The first term is the raw part-number space; the second is the enforced staged ceiling
    /// plus the bounded commit overshoot plus the in-flight owned entries — each part charged
    /// its **full** `max_part_chunks`, because a part is not one unit (`0016:1469`). Which
    /// term binds is a property of the profile, not a formality: at maximal parts the raw term
    /// charges far more than Complete would let the session publish, which is why the ceiling
    /// term exists at all.
    ///
    /// **The `u128` width is load-bearing rather than defensive.** A decoder evaluates this
    /// over bytes it has not yet judged (G7 does, on a candidate), so both terms are computed
    /// from values a torn record may set to their field maxima: at `max_staged_chunks =
    /// u32::MAX` the second term leaves `u32`, and at maximal `max_part_chunks` the first
    /// leaves `u64` — while the `min` can make the leaving term irrelevant, so a record that
    /// names it may still be legal and must still decode. Same-width arithmetic answers those
    /// two cases with a panic (debug overflow checks) or a wrapped verdict (release); `u128`
    /// answers both with the mathematical value, and every operand here is at most `2^65`, so
    /// the width itself cannot overflow. ADR-0045 names checked arithmetic for the same reason
    /// on `InodeRecord` version increments and `PendingEntry` lease timestamps
    /// (`docs/design/adr/0045-metadata-validation-boundaries.md:73-74`).
    fn u_ref_exact(&self) -> u128 {
        let raw = (u128::from(self.max_parts_per_session) + u128::from(self.max_inflight_parts))
            * u128::from(self.max_part_chunks);
        let ceiling = u128::from(self.max_staged_chunks) + 2 * self.inflight_owned_refs();
        raw.min(ceiling)
    }

    /// `U_ref` — this profile's worst-case per-session staged-reference footprint
    /// (`Budget::u_ref_exact`, `0016:1469`).
    ///
    /// Total, and `u64` rather than the `u128` it is computed in: G7 refuses any tuple whose
    /// `U_ref` exceeds its own `w_ref`, and `w_ref` is a `u64`, so every `Budget` that exists
    /// has a `U_ref` inside the width its budget is stated in. The narrowing therefore states
    /// a type invariant the way [`crate::metadata::encode`] states serialization's
    /// (`metadata.rs:1562-1566`) — not a fallible step with a hidden failure mode.
    pub fn u_ref(&self) -> u64 {
        u64::try_from(self.u_ref_exact()).expect("G7 bounds every Budget's U_ref by its w_ref")
    }

    /// `MAX_SESSIONS = min( ⌊W_ref / U_ref⌋ , SCAN_CAP/2 )` — **derived, never chosen**
    /// (`0016:1470`).
    ///
    /// Both terms bind. The quotient is the memory bound the reconcile pass is sized for
    /// (`Σ_sessions U_ref ≤ W_ref` by construction); the `SCAN_CAP/2` term is a clamp the
    /// implementation applies rather than an operator range check, because `W_ref` is sized
    /// from host RAM and `U_ref` from the caps — a legal pairing (a large `W_ref` with small
    /// parts) makes the quotient exceed `SCAN_CAP` and break the reaper's `scan("mpu:")`.
    /// The clamp is what makes the two bounds compose.
    ///
    /// Total: G1 ∧ G2 put `U_ref ≥ 1`, so the division always has a divisor.
    pub fn max_sessions(&self) -> u64 {
        (self.w_ref / self.u_ref()).min(SCAN_HALF)
    }

    /// The profile's whole rule set, in one place and applied wherever a [`Budget`] can come
    /// into existence — the shape `InodeRecord::checked_shape` uses for the other cross-field
    /// record invariant in this repo (`metadata.rs:1458-1474`).
    ///
    /// **These are record rules, not a configuration validator.** Every one relates the
    /// tuple's own stored components to each other or to a constant of the **format** that
    /// cannot move under a stored record — [`MAX_PART_NUMBER`] and `SCAN_CAP/2`. The knob
    /// *ranges* `0016` settles for an operator's choice (the `max_chunkref_bytes`
    /// value-ceiling that puts `MAX_PART_CHUNKS` in 165–381, the `B_ops` clamp, the
    /// `MAX_ROOT_SEGMENTS × MAX_SEG_CHUNKS` ceiling on `MAX_STAGED_CHUNKS`) are deliberately
    /// **absent**: those constants have no definition on this base, they are #508's and
    /// #625's to value, and `0016:1466`/`:1468` enforce them where work is admitted
    /// (`UploadPart`, part commit). A decode that consulted one would make a durable ledger
    /// unreadable the day a deployment moved it — what `0016:390-402` and
    /// [`crate::metadata::MAX_ROOT_SEGMENTS`] both forbid, and this ledger is the record every
    /// teardown path must read to decrement `count`.
    ///
    /// `max_parts_per_session ≥ 1` and `max_inflight_parts ≤ MAX_SLOT_INDEX + 1` are
    /// deliberately **not** rules of their own: G2 ∧ G4 implies the first, and G3 ∧ G4 binds
    /// the second tighter than [`MAX_SLOT_INDEX`] would ([`MAX_PART_NUMBER`] is `999_999`).
    fn checked_rules(&self) -> Result<(), RecordError> {
        // G1 (`0016:1466`, `> 0`) — the totality precondition, checked FIRST and before any
        // derivation: at zero, `U_ref` is zero and `MAX_SESSIONS`' quotient has no divisor.
        if self.max_part_chunks == 0 {
            return Err(RecordError::MaxPartChunksZero);
        }
        // G2 (`0016:1471`, the range `[1, …]`) — the second half of that precondition.
        if self.max_inflight_parts == 0 {
            return Err(RecordError::MaxInflightPartsZero);
        }
        // G3 — the `part:` key space, the only bound `0016`'s knob table leaves for this cap.
        if self.max_parts_per_session > MAX_PART_NUMBER {
            return Err(RecordError::PartsPerSessionUnaddressable {
                max_parts_per_session: self.max_parts_per_session,
            });
        }
        // G4 (`0016:1471` clamp 1).
        if self.max_inflight_parts > self.max_parts_per_session {
            return Err(RecordError::InflightPartsExceedParts {
                max_inflight_parts: self.max_inflight_parts,
                max_parts_per_session: self.max_parts_per_session,
            });
        }
        // G5 (`0016:1471`, `:2098`) — the same owned-`sidx:` product `U_ref`'s ceiling term
        // charges, exact in `u128`, so no wrap can defeat the comparison.
        let owned_sidx = self.inflight_owned_refs();
        if owned_sidx > u128::from(SCAN_HALF) {
            return Err(RecordError::StagingRangeUnscannable { owned_sidx });
        }
        // G6 (`0016:1468`, the lower end of the settled range).
        if self.max_staged_chunks < self.max_part_chunks {
            return Err(RecordError::StagedChunksBelowPart {
                max_staged_chunks: self.max_staged_chunks,
                max_part_chunks: self.max_part_chunks,
            });
        }
        // G7 (`0016:1473`, `W_ref`'s range `[U_ref, deployment RAM]`) — against the exact
        // `u128` footprint, so one past `u64` compares as the number it is rather than as a
        // saturated stand-in. It is also what makes [`Budget::u_ref`]'s narrowing total.
        let u_ref = self.u_ref_exact();
        if u128::from(self.w_ref) < u_ref {
            return Err(RecordError::BudgetBelowFootprint {
                w_ref: self.w_ref,
                u_ref,
            });
        }
        Ok(())
    }
}

/// The wire shape of [`Budget`], field order and names exactly as `0016:348` states them.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetWire {
    w_ref: u64,
    max_part_chunks: u32,
    max_parts_per_session: u32,
    max_inflight_parts: u32,
    max_staged_chunks: u32,
}

impl TryFrom<BudgetWire> for Budget {
    type Error = RecordError;

    fn try_from(wire: BudgetWire) -> Result<Self, RecordError> {
        // The candidate is a local value, returned only once every rule holds — so no caller
        // can hold a `Budget` that broke one, and the rules can be stated over the same
        // derivations the type exposes rather than over a parallel copy of them.
        let budget = Self {
            w_ref: wire.w_ref,
            max_part_chunks: wire.max_part_chunks,
            max_parts_per_session: wire.max_parts_per_session,
            max_inflight_parts: wire.max_inflight_parts,
            max_staged_chunks: wire.max_staged_chunks,
        };
        budget.checked_rules()?;
        Ok(budget)
    }
}

/// The **admission ledger** singleton, the value under [`MPUCTL_KEY`]: one record, three
/// fields, CAS'd **whole**, so the count and the limit it was checked against can never be
/// read apart (`0016:348`, decision 6).
///
/// `max_sessions` is checked against `profile` at decode because it is **derived**
/// ([`Budget::max_sessions`], `0016:1470`) and never independently chosen. Admission enforces
/// the **stored** limit — deliberately, so every gateway in the fleet agrees on one number
/// (`0016:2593`, X64) — which is exactly why the stored number may not be free: a torn ledger
/// naming a larger `max_sessions` than its own profile derives would be trusted fleet-wide,
/// and `Σ_sessions U_ref ≤ W_ref`, the bound the whole reconcile pass is sized for, would be
/// exceeded on hosts that never observe the overrun (an OOM landing on the maintenance plane
/// rather than on the gateway that caused it).
///
/// `count` is deliberately **not** checked against `max_sessions`. Occupancy above a lowered
/// cap is legitimate live state, not a torn identity: a profile lowered while sessions are
/// live leaves the ledger over its new cap until the population drains, and admission simply
/// refuses to grow it. Refusing it at decode would make the ledger unreadable exactly when
/// every teardown path needs to read it to decrement `count` — wedging multipart fleet-wide
/// with no path that clears it (`0016:390-402`; the same liberal-on-read boundary
/// [`crate::metadata::MAX_ROOT_SEGMENTS`] draws). The line this record class settles: two
/// stored spellings of **one** quantity (`max_sessions` versus what `profile` derives) are a
/// decode error; one quantity merely being large relative to another (`count` versus
/// `max_sessions`) is not.
///
/// # Serialization identity, stated with its domain
///
/// For bytes **this codec wrote**, decode→encode is byte-identical: every field is required,
/// none is optional, defaulted or skipped, and the wire shape is **closed**
/// (`deny_unknown_fields`), so [`crate::metadata::encode`] re-emits exactly the names, order
/// and numbers it read. That is the property a whole-record CAS needs, and the reason the
/// shape is closed rather than tolerant: a dropped-on-read field would be silent, and the two
/// CAS shapes this repo already contains punish it differently but both durably — the
/// `inode:` commits precondition on the **re-encoded** prior (`metadata.rs:1794`, `:1919`;
/// ADR-0047), where the re-encode would no longer equal the stored bytes and every later CAS
/// would `Conflict` forever, while the `pending:` commits precondition on the **raw bytes they
/// read** (`metadata.rs:2012`), where the CAS succeeds and the put silently writes the record
/// back without the field. `0016:348` does not say which shape `mpuctl` takes (that is
/// #656–#659's), so the closed shape forecloses both — a loud typed decode error at the one
/// place a human can read it (ADR-0045). Its cost is that a future additive field to this
/// record is a versioned format change, exactly as `0016:390-402` says a format maximum is.
///
/// The other half of that identity is byte-level and lives at the decode entry point:
/// [`decode_admission_record`] re-encodes what it decoded and requires the input bytes back
/// (`require_canonical`), so a foreign spelling of the same value — fields reordered,
/// whitespace inserted — is refused as [`RecordError::NoncanonicalRecordValue`] rather than
/// decoded to a value whose re-encoding no longer matches the store. JSON calls those
/// spellings equal; the whole-record CAS does not, and the gate is what frees #656–#659's
/// writer from ever depending on which precondition shape (`metadata.rs:1794` vs `:2012`) it
/// takes against bytes some other writer spelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "AdmissionRecordWire")]
pub struct AdmissionRecord {
    count: u64,
    max_sessions: u64,
    profile: Budget,
}

impl AdmissionRecord {
    /// How many `mpu:` records exist, in any state (`0016:348`).
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// The governing limit those increments were admitted against.
    pub const fn max_sessions(&self) -> u64 {
        self.max_sessions
    }

    /// The budget tuple that establishes [`Self::max_sessions`].
    pub const fn profile(&self) -> &Budget {
        &self.profile
    }
}

/// The wire shape of [`AdmissionRecord`], field order and names exactly as `0016:348` states
/// them. Its `profile` is the **unvalidated** [`BudgetWire`], deliberately: the profile's own
/// rules are applied by [`Budget`]'s conversion inside this record's conversion, where the
/// typed [`RecordError`] survives — a nested validating `Deserialize` would have been
/// stringified by serde's `custom` funnel before [`decode_admission_record`] could see it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRecordWire {
    count: u64,
    max_sessions: u64,
    profile: BudgetWire,
}

/// The ledger's own rule, over a profile [`Budget`]'s conversion has already judged — the one
/// place an [`AdmissionRecord`] can come into existence:
///
/// * **G8** `max_sessions == profile.max_sessions()` (`0016:1470`).
impl TryFrom<AdmissionRecordWire> for AdmissionRecord {
    type Error = RecordError;

    fn try_from(wire: AdmissionRecordWire) -> Result<Self, RecordError> {
        let profile = Budget::try_from(wire.profile)?;
        let derived = profile.max_sessions();
        let stored = wire.max_sessions;
        if stored != derived {
            return Err(RecordError::MaxSessionsNotDerived { stored, derived });
        }
        Ok(Self {
            count: wire.count,
            max_sessions: stored,
            profile,
        })
    }
}

/// Decode the `mpuctl` value ([`MPUCTL_KEY`]) with its rejection **attributed**: the rule a
/// stored ledger broke comes back as its own [`RecordError`] variant, not as prose.
///
/// The peer of [`crate::metadata`]'s per-record decoders (`decode_segment_record`,
/// `metadata.rs:2536-2547`), and public for the same reason: the store round trips that read
/// `mpuctl` (#656–#659) need the fault typed, because "this ledger is torn" and "the store is
/// failing" are different operator actions and a stringified error is indistinguishable from a
/// backend outage.
///
/// It reaches the wire struct through the store-wide [`crate::metadata::decode`] and then
/// applies the record's rules directly, rather than decoding into [`AdmissionRecord`] and
/// recovering the type afterwards: serde's `Error::custom` funnel turns a domain error into a
/// `serde_json::Error` on the way out, so a `downcast` after the fact cannot see it. Decoding
/// through [`AdmissionRecord`]'s own `Deserialize` — what [`crate::metadata::decode`] does for
/// any consumer holding the type — applies the **same** rules and differs only in that the
/// failure arrives untyped.
pub fn decode_admission_record(value: &[u8]) -> Result<AdmissionRecord, RecordError> {
    let wire: AdmissionRecordWire =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "mpuctl",
            detail: err.to_string(),
        })?;
    require_canonical(AdmissionRecord::try_from(wire)?, value, "mpuctl")
}

/// The canonical-bytes gate every `decode_*` in this module closes with: re-encode what
/// decoded and require the input bytes back, or the value is refused as
/// [`RecordError::NoncanonicalRecordValue`].
///
/// This is what turns each decoder's accepted set into **exactly** the encoder's image, byte
/// for byte — the property a whole-record CAS turns on (`0016:555-558`): serde reads a foreign
/// spelling of the same value (fields reordered, whitespace inserted, an equivalent `\u`
/// escape) without complaint, and every such spelling is a stored record that a decode→encode
/// caller could never CAS against (`require(key, encode(prior))`, `metadata.rs:1794`, `:1919`,
/// would `Conflict` forever) or would silently rewrite (`require(key, current)` on the raw
/// bytes, `metadata.rs:2012`). Two spellings of one record are the `require` hazard that two
/// spellings of one key are for `require_absent`, and this module already refuses the latter
/// (the canonical-key rule in the header) — so stored bytes only this codec's own writer could
/// have produced are the only bytes it will vouch for.
///
/// It lives at the `decode_*` seam and **only** there, because only that seam holds the bytes:
/// the store-wide [`crate::metadata::decode`] surface (S1) hands serde the parse and never
/// sees the input again, so canonicality is the one rule of these records S1 cannot check —
/// the reason the identity tests reach these entry points for it.
fn require_canonical<T: Serialize>(
    record: T,
    value: &[u8],
    namespace: &'static str,
) -> Result<T, RecordError> {
    if metadata::encode(&record).as_ref() == value {
        Ok(record)
    } else {
        Err(RecordError::NoncanonicalRecordValue { namespace })
    }
}

// ===========================================================================
// 6. The session record — the `mpu:<upload-id>` value (`0016:350`)
// ===========================================================================

/// The target dirent identity a `Completing` session's fence will flip: parent bucket inode +
/// object name — **never** a frozen inode id (`0016:350`, `:561-563`), plus the `Completing`
/// fence epoch `E` that makes the attempt's segment-group nonce deterministic. Stamped onto
/// the session record the moment it fences into `Completing`.
///
/// A plain value with no invariant of its own: its component types ([`InodeId`], `String`,
/// `u64`) each validate their own shape, and the **identity** it must hold against the session
/// that carries it — parent/name/epoch must agree with the session's own — is a cross-record
/// relation, checked in [`SessionRecord`]'s own decode (legs 1c, 1c-epoch), not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishTarget {
    /// The bucket inode the publish will bind the name under.
    pub parent: InodeId,
    /// The object name the publish will bind.
    pub name: String,
    /// The `Completing` fence epoch this attempt is writing under.
    pub epoch: u64,
}

/// What a `Completed` session's fence actually published (`0016:350`): the generation it
/// created or superseded, and a fingerprint of the *ordered* `(part_number, digest)` list the
/// winning Complete named, so a retried `CompleteMultipartUpload` against the same upload id
/// can be told apart from a genuinely different assembly (iteration-10 finding 9).
///
/// This record is the **only** source of a retry's answer: by the time a tombstone answers,
/// its `retire:records:` obligation may have deleted every `part:` record the ETag was
/// composed from (`0016:964-968`), so nothing is left to recompute it from. The ETag is
/// therefore stored **whole** — the composed [`MultipartEtag`], `-N` suffix included — and
/// handed back verbatim ([`Publication::of`]); a bare digest would make a retry answer an
/// ETag the original Complete never returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    /// The published inode.
    pub inode: InodeId,
    /// The published generation's version.
    pub version: u64,
    /// The published object's ETag, exactly as the winning Complete answered it: the
    /// [`multipart_etag`] composition (ADR-0047's SHA-256 basis — never MD5), validated
    /// against its grammar at decode (`0016:3064-3070`: "the value is in any case recorded
    /// in the `Completed` session record").
    pub etag: MultipartEtag,
    /// When the flip landed (logical milliseconds).
    pub completed_at_millis: u64,
    /// The [`complete_fingerprint`] of the named-part list the winning Complete sent.
    pub complete_fingerprint: Digest,
}

/// A session's lifecycle state (`0016:350`, the state machine at `:528-602`) — decoded on its
/// own, independent of the rest of [`SessionRecord`], so a malformed state shape is attributed
/// before any cross-field identity check runs (the Defect field's "each validating inside its
/// own `Deserialize`").
///
/// Only `Completing` carries the fence stamps — `0016:350` states them as landing "on
/// Completing also": `fenced_at_millis`, `segments_written` and `publish_target`. A value
/// carrying one under any other state, or missing one while claiming `Completing`, is a
/// decode error, never a silently-defaulted value (leg 1j; `0016:403-415` names this exact
/// example: a `Completing`-only `fenced_at_millis` on an `Open` session). Derived
/// `Deserialize` gives both directions for free: a struct variant with required fields
/// rejects a value missing one, and `deny_unknown_fields` on the enum rejects a field that
/// does not belong to the resolved variant — including one that belongs to a *different*
/// variant, which is exactly the forbidden-field case leg 1j demonstrates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum SessionState {
    /// Accepting `UploadPart`s; no write to this record.
    ///
    /// **Deliberately a zero-field struct variant, not a unit variant.** Serde's generated
    /// `Deserialize` for an internally-tagged **unit** variant does not consult the rest of
    /// the map at all once the tag matches, so a stray sibling field is silently ignored even
    /// under the enum's own `deny_unknown_fields` — verified against this exact serde version
    /// before relying on it (a unit `Open` let `{"kind":"Open","fenced_at_millis":9}` decode).
    /// An empty struct variant does not have that gap: it goes through the same
    /// per-field-name check every non-empty variant does, so an unexpected sibling is
    /// `unknown field`, which is what leg 1j needs.
    Open {},
    /// Fenced for `Complete`: segments are being written (or awaiting the flip) at `epoch`.
    Completing {
        /// When the fence landed (logical milliseconds); `W_completing` is measured from it.
        fenced_at_millis: u64,
        /// The segment-write cursor (`0016` §3). Bounded at decode by the `seg:` key
        /// grammar's [`crate::metadata::MAX_SEGMENT_INDEX`]` + 1` — see
        /// [`RecordError::SegmentCursorUnaddressable`].
        segments_written: u32,
        /// The dirent identity and fence epoch this attempt is publishing under.
        publish_target: PublishTarget,
    },
    /// Fenced for teardown without publishing; draining. Same zero-field-variant reasoning as
    /// `Open`.
    Aborting {},
    /// Published; draining.
    Completed {
        /// What the fence actually published.
        completion: Completion,
    },
}

/// The **one** spelling of an absent `content_type` this record accepts: the field omitted
/// (`#[serde(default)]` on the wire below), never a present-but-`null` field.
///
/// `Option<String>`'s own `Deserialize` accepts both and maps them to the same `None`, which
/// would make two stored spellings of one quantity — the line [`AdmissionRecord`]'s doc
/// already settles as a decode error for `max_sessions`. Here it is worse than redundant: the
/// record is CAS'd whole (`0016:555-558`) and [`crate::metadata::encode`] **omits** an absent
/// `content_type` (the `skip_serializing_if` on [`SessionRecord`]'s field), so `null` is a
/// spelling this codec can decode but can never re-emit — decode→encode would silently rewrite
/// the stored bytes, which is exactly the identity a whole-record CAS turns on. Requiring a
/// string when the key is present makes the accepted set **exactly** the encoder's own output,
/// so `encode(decode(bytes)) == bytes` holds for every value that decodes at all, not merely
/// for the shapes this codec happens to write.
///
/// [`crate::metadata::InodeRecord`]'s own optional trio stops at `skip_serializing_if`
/// (`metadata.rs:1406-1419`) because it must keep decoding records written before those fields
/// existed; this record class has **no** stored corpus to stay compatible with — its first
/// writer is #656–#659 — so the stricter shape costs nothing and closes the spelling.
///
/// It never returns `None`: serde calls it only for a **present** key, and absence is the
/// `#[serde(default)]` beside it. The `Option` in the signature is the field's type, not a
/// second absence channel.
fn de_content_type<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some).map_err(|err| {
        DeError::custom(format!(
            "content_type: {err} (an absent content type is spelled by omitting the field)"
        ))
    })
}

/// The wire shape of [`SessionRecord`] — every field the session carries regardless of state,
/// plus `state` itself, whose own [`SessionState`] `Deserialize` enforces which of the
/// state-dependent fields may accompany it (leg 1j). `deny_unknown_fields` closes the shape
/// against a field this build does not know (leg 1m), and `de_content_type` closes the one
/// field that is genuinely optional against its second spelling.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionRecordWire {
    parent: InodeId,
    object: String,
    #[serde(default, deserialize_with = "de_content_type")]
    content_type: Option<String>,
    created_at_millis: u64,
    clock_source: String,
    epoch: u64,
    attempts: u32,
    state: SessionState,
}

/// A multipart upload **session**, the `mpu:<upload-id>` value (`0016:350`): target bucket
/// (`parent`) + object name, `content_type`, `created_at_millis`, `clock_source`, `epoch`,
/// `attempts`, and the lifecycle [`SessionState`].
///
/// # Serialization identity
///
/// **`encode(decode(bytes)) == bytes` for every value this record accepts** — the property a
/// whole-record CAS on the session turns on: every transition is a compare-and-set on the
/// session record's **exact current bytes** (`0016:555-558`). It is stated over the accepted
/// set rather than over the shapes this codec happens to write, so no accepted value can be
/// one whose re-encode differs from what the store holds.
///
/// Seven of the eight fields are required, so their spelling is forced. `content_type` is
/// genuinely optional (`0016:350`) and is the whole of the argument:
///
/// * absent is spelled by **omitting** the field — `skip_serializing_if` below, the
///   convention [`crate::metadata::InodeRecord`] states at length for its own optional trio
///   (`metadata.rs:1394-1419`) and `AGENTS.md:170-172` makes a repo rule. Emitting
///   `"content_type":null` for an absent value instead would put bytes in the store that a
///   later decode→encode could not reproduce;
/// * and `null` — `Option`'s other spelling of the same `None` — is refused at decode
///   (`de_content_type`), because it is the mirror hole: a spelling accepted on read that
///   the encoder can never write back.
///
/// Both halves are needed. Under this repo's two CAS shapes a rewrite-on-decode is durable
/// either way: `require(key, encode(prior))` (`metadata.rs:1794`, `:1919`) turns it into a
/// permanent `Conflict`, and `require(key, current)` on the raw bytes read
/// (`metadata.rs:2012`) lets the CAS win and silently write the record back in the other
/// spelling. Which shape a session transition uses is #656–#659's to choose, so this record
/// forecloses both.
///
/// The byte-level half, as for [`AdmissionRecord`]: decode **is** a canonicalisation check.
/// [`decode_session_record`] re-encodes what it decoded and requires the input bytes back
/// (`require_canonical`), so a foreign JSON spelling of the same value — fields reordered,
/// whitespace inserted — is [`RecordError::NoncanonicalRecordValue`], never a value whose
/// re-encoding a CAS could not match against the store.
///
/// No writer-side constructor: the first writer is the store round trip (#656–#659), and a
/// `SessionRecord` this module minted directly could not be relied on to hold the identity
/// [`decode_session_record`] enforces — precisely the reason [`AdmissionRecord`] and
/// [`Budget`] omit one too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SessionRecordWire")]
pub struct SessionRecord {
    parent: InodeId,
    object: String,
    /// Omitted when absent, never emitted as `null`: see this type's "Serialization
    /// identity". The decode half is `de_content_type`.
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    created_at_millis: u64,
    clock_source: String,
    epoch: u64,
    attempts: u32,
    state: SessionState,
}

impl SessionRecord {
    /// The bucket inode the session targets.
    pub const fn parent(&self) -> InodeId {
        self.parent
    }

    /// The object name the session targets.
    pub fn object(&self) -> &str {
        &self.object
    }

    /// The client's declared content type, if any.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// When the session was created (logical milliseconds).
    pub const fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }

    /// The clock source that stamped this session's timestamps (`0016:1957-1990`).
    pub fn clock_source(&self) -> &str {
        &self.clock_source
    }

    /// The session's current epoch; every transition is a fenced CAS that bumps it.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Complete fences attempted so far. `MAX_COMPLETE_ATTEMPTS` (decision 3) is a live
    /// **knob**, so it is enforced where a fence is *admitted*, never here: a decode that
    /// consulted it would make every session written under a higher cap unreadable the day it
    /// was lowered — including to the teardown path (`0016:390-402`).
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// The session's lifecycle state.
    pub const fn state(&self) -> &SessionState {
        &self.state
    }
}

/// The session's own cross-field rule, over a [`SessionState`] whose own shape
/// [`SessionState`]'s `Deserialize` has already judged — the one place a [`SessionRecord`] can
/// come into existence:
///
/// * **1c** `publish_target.parent`/`name` must equal the session's own `parent`/`object`.
/// * **1c-epoch** `publish_target.epoch` must equal the session's own `epoch`.
/// * `segments_written` must not exceed the `seg:` key grammar's
///   [`crate::metadata::MAX_SEGMENT_INDEX`]` + 1` records — the format bound, never the
///   `MAX_ROOT_SEGMENTS` capacity knob (see [`RecordError::SegmentCursorUnaddressable`]).
impl TryFrom<SessionRecordWire> for SessionRecord {
    type Error = RecordError;

    fn try_from(wire: SessionRecordWire) -> Result<Self, RecordError> {
        if let SessionState::Completing {
            segments_written,
            publish_target,
            ..
        } = &wire.state
        {
            if *segments_written > metadata::MAX_SEGMENT_INDEX + 1 {
                return Err(RecordError::SegmentCursorUnaddressable {
                    segments_written: *segments_written,
                });
            }
            if publish_target.parent != wire.parent || publish_target.name != wire.object {
                return Err(RecordError::PublishTargetKeyMismatch {
                    session_parent: wire.parent,
                    session_object: wire.object,
                    target_parent: publish_target.parent,
                    target_name: publish_target.name.clone(),
                });
            }
            if publish_target.epoch != wire.epoch {
                return Err(RecordError::PublishTargetEpochMismatch {
                    session_epoch: wire.epoch,
                    target_epoch: publish_target.epoch,
                });
            }
        }
        Ok(Self {
            parent: wire.parent,
            object: wire.object,
            content_type: wire.content_type,
            created_at_millis: wire.created_at_millis,
            clock_source: wire.clock_source,
            epoch: wire.epoch,
            attempts: wire.attempts,
            state: wire.state,
        })
    }
}

/// Decode the `mpu:` value ([`mpu_key`]) with its rejection attributed, the peer of
/// [`decode_admission_record`] for this record — necessary rather than cosmetic, because
/// [`SessionRecord`]'s `#[serde(try_from = ...)]` funnels its [`RecordError`] through serde's
/// `Error::custom` on the way out of a plain [`crate::metadata::decode`] call, stringifying it
/// before a `downcast` could recover the variant (the reason `decode_admission_record` reaches
/// its own wire struct directly instead of decoding into the validated type).
pub fn decode_session_record(value: &[u8]) -> Result<SessionRecord, RecordError> {
    let wire: SessionRecordWire =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "mpu:",
            detail: err.to_string(),
        })?;
    require_canonical(SessionRecord::try_from(wire)?, value, "mpu:")
}

// ===========================================================================
// 7. The slot record — the `slot:<upload-id>:<index>` value (`0016:349`)
// ===========================================================================

/// The wire shape of [`SlotRecord`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SlotRecordWire {
    part_number: PartNumber,
    attempt_id: AttemptId,
    reserved_at_millis: u64,
    lease_expiry_millis: u64,
}

/// One **in-flight part slot**, the `slot:<upload-id>:<index>` value (`0016:349`): which part
/// number claimed this index, the attempt id that stamped it (so an ambiguous reserve is
/// settled by re-reading rather than re-reserving a different index), and the reservation's
/// lease window.
///
/// Every field is required and the wire shape is closed, so decode→encode is byte-identical —
/// see [`SessionRecord`]'s "Serialization identity" for why that matters to a whole-record CAS.
///
/// No writer-side constructor — see [`SessionRecord`]'s doc for why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SlotRecordWire")]
pub struct SlotRecord {
    part_number: PartNumber,
    attempt_id: AttemptId,
    reserved_at_millis: u64,
    lease_expiry_millis: u64,
}

impl SlotRecord {
    /// The part number this slot was reserved for.
    pub const fn part_number(&self) -> PartNumber {
        self.part_number
    }

    /// The attempt id that claimed it.
    pub fn attempt_id(&self) -> &AttemptId {
        &self.attempt_id
    }

    /// When the slot was reserved (logical milliseconds).
    pub const fn reserved_at_millis(&self) -> u64 {
        self.reserved_at_millis
    }

    /// When the reservation's lease expires.
    pub const fn lease_expiry_millis(&self) -> u64 {
        self.lease_expiry_millis
    }
}

/// The slot's own rule — the one place a [`SlotRecord`] can come into existence:
///
/// * **1i (slot half)** `lease_expiry_millis > reserved_at_millis`: a slot born already
///   lapsed is reapable the instant it is written (`0016:349`).
impl TryFrom<SlotRecordWire> for SlotRecord {
    type Error = RecordError;

    fn try_from(wire: SlotRecordWire) -> Result<Self, RecordError> {
        if wire.lease_expiry_millis <= wire.reserved_at_millis {
            return Err(RecordError::SlotLeaseAlreadyLapsed {
                reserved_at_millis: wire.reserved_at_millis,
                lease_expiry_millis: wire.lease_expiry_millis,
            });
        }
        Ok(Self {
            part_number: wire.part_number,
            attempt_id: wire.attempt_id,
            reserved_at_millis: wire.reserved_at_millis,
            lease_expiry_millis: wire.lease_expiry_millis,
        })
    }
}

/// Decode the `slot:` value ([`slot_key`]) with its rejection attributed — the peer of
/// [`decode_admission_record`]/[`decode_session_record`] for this record, and necessary for
/// the same reason.
pub fn decode_slot_record(value: &[u8]) -> Result<SlotRecord, RecordError> {
    let wire: SlotRecordWire =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "slot:",
            detail: err.to_string(),
        })?;
    require_canonical(SlotRecord::try_from(wire)?, value, "slot:")
}

// ===========================================================================
// 8. The part record and its summary — `part:`/`psum:<upload-id>:<part-number>`
//    (`0016:351-352`)
// ===========================================================================

/// Refuse a chunk whose stored `EcScheme` is not one [`crate::erasure`] can encode/decode —
/// the nested structural check leg 1i's `PartRecord` half demands (ADR-0045's invariant table:
/// `EcScheme::ReedSolomon` → `erasure::supported(k, m)`), applied to every chunk a
/// `PartRecord` carries so untrusted stored geometry never reaches the read path (the #285
/// class this mirrors at `crate::read::ReadError::InvalidEcScheme`). `EcScheme::None` has no
/// `(k, m)` pair to check and is always valid.
fn checked_chunk_scheme(chunk: &ChunkRef) -> Result<(), RecordError> {
    if let EcScheme::ReedSolomon { k, m } = chunk.scheme {
        if !erasure::supported(k as usize, m as usize) {
            return Err(RecordError::ChunkSchemeUnsupported {
                chunk_id: chunk.id,
                k,
                m,
            });
        }
    }
    Ok(())
}

/// The total logical length of `chunks`, **checked**: overflow is an error, never a wrap — the
/// definition this decode check applies (mirrors [`crate::metadata`]'s `checked_chunk_bytes`,
/// `metadata.rs:1208-1218`, the same cross-check for the analogous `SegmentRecord`).
fn checked_chunk_len(chunks: &[ChunkRef]) -> Result<u64, RecordError> {
    chunks
        .iter()
        .try_fold(0u64, |total, chunk| total.checked_add(chunk.len))
        .ok_or(RecordError::PartLengthOverflow {
            chunks: chunks.len(),
        })
}

/// The wire shape of one chunk of a [`PartRecord`] — a **closed** mirror of
/// [`crate::metadata::ChunkRef`], read in its place so the nested value is judged by exactly
/// the rules the record around it is (ADR-0045; the invariant this child restores).
///
/// [`crate::metadata::ChunkRef`]'s own `Deserialize` is deliberately **tolerant** in two ways
/// that are right for the record class it was written for and wrong for this one
/// (`metadata.rs:114-140`):
///
/// * `placement` is `#[serde(default)]` — "additive metadata on a never-yet-deployed schema"
///   (`metadata.rs:120-124`), so an `inode:` written before the field decodes with an empty
///   vector. Nothing skips it on the way out, so a chunk that arrived **without** `placement`
///   re-encodes **with** `"placement":[]`;
/// * the shape is **open** — an unknown field inside a chunk object is silently dropped, and
///   re-encodes gone.
///
/// Both are the same fault this module refuses one level up (leg 1m, and [`SessionRecord`]'s
/// "Serialization identity"): a value accepted on read whose re-encode is not the bytes read.
/// A `part:` record is CAS'd whole exactly as the session is, so under
/// `require(key, encode(prior))` (`metadata.rs:1794`, `:1919`) that chunk wedges every later
/// commit on a permanent `Conflict`, and under `require(key, current)`
/// (`metadata.rs:2012`) the CAS wins and the put silently rewrites the record — dropping a
/// field a later build wrote, or inserting one the stored bytes never had.
///
/// This class has **no** stored corpus to stay compatible with — its first writer is the part
/// commit of #656–#659, and [`crate::metadata::encode`] always emits `placement` for a
/// `ChunkRef` (there is no `skip_serializing_if` on it) — so requiring the field and closing
/// the shape makes the accepted set **exactly** the encoder's own output. It is deliberately
/// not a change to `ChunkRef` itself: `inode:`/`seg:` records have a stored corpus that turns
/// on that tolerance, and this child's scope pins `metadata.rs` untouched.
///
/// **Geometry is judged, length is not.** `placement`'s *length* is still unchecked here — the
/// standing contextual check, liberal on read (ADR-0045, `AGENTS.md:146-149`,
/// `0016:416-429`): a present-but-wrong-length vector decodes, because it re-encodes exactly
/// as it arrived. Absence and presence are different questions from length.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkRefWire {
    id: ChunkId,
    scheme: EcSchemeWire,
    len: u64,
    placement: Vec<DServerId>,
}

impl From<ChunkRefWire> for ChunkRef {
    fn from(wire: ChunkRefWire) -> Self {
        Self {
            id: wire.id,
            scheme: wire.scheme.into(),
            len: wire.len,
            placement: wire.placement,
        }
    }
}

/// The wire shape of [`crate::metadata::EcScheme`], closed for the reason [`ChunkRefWire`]
/// is: the variant names and field names are `EcScheme`'s own, so this is the same value
/// space, but an unknown field inside `ReedSolomon` is a decode error here rather than a
/// silently dropped one. Same reasoning one level down; without it the closure above would
/// stop at the chunk object and leave the scheme object open.
///
/// **How this mirror fails if `EcScheme` moves.** A new *field* on [`ChunkRefWire`]'s source
/// type breaks the build at that struct's conversion (a missing field), the way
/// `InodeRecordWire`'s does (`metadata.rs:1439-1455`). A new **variant** here does not: a
/// `part:` value naming it would be an `unknown variant` decode rejection until this mirror
/// learns it. That is loud, typed and attributed — the same failure a versioned format change
/// has (`0016:390-402`) — never a silently dropped or re-spelled scheme, which is the outcome
/// this type exists to prevent.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum EcSchemeWire {
    None,
    ReedSolomon { k: u8, m: u8 },
}

impl From<EcSchemeWire> for EcScheme {
    fn from(wire: EcSchemeWire) -> Self {
        match wire {
            EcSchemeWire::None => Self::None,
            EcSchemeWire::ReedSolomon { k, m } => Self::ReedSolomon { k, m },
        }
    }
}

/// The wire shape of [`PartRecord`]. Its chunks are read through the **closed** [`ChunkRefWire`]
/// rather than [`crate::metadata::ChunkRef`] directly — see that type for why.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PartRecordWire {
    chunks: Vec<ChunkRefWire>,
    len: u64,
    digest: Digest,
    committed_at_millis: u64,
    session_epoch: u64,
}

/// A **committed part**, the `part:<upload-id>:<part-number>` value (`0016:351`): its ordered
/// chunk list, logical length, digest, commit time, and the session epoch the fenced commit
/// wrote it under.
///
/// Every field is required — none optional, defaulted or skipped — and the wire shape is
/// closed, so decode→encode is byte-identical here without the argument [`SessionRecord`]'s
/// "Serialization identity" has to make for its one optional field. **That claim covers the
/// `chunks` list too, and only because it is read through the module's own closed
/// `ChunkRefWire` rather than through [`crate::metadata::ChunkRef`] directly** (see that wire
/// type's doc): `ChunkRef`'s own `Deserialize` defaults an omitted `placement` and ignores an
/// unknown field, both of which re-encode differently from the bytes they were read from — the
/// identity a whole-record CAS turns on, one level in.
///
/// No writer-side constructor — see [`SessionRecord`]'s doc for why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PartRecordWire")]
pub struct PartRecord {
    chunks: Vec<ChunkRef>,
    len: u64,
    digest: Digest,
    committed_at_millis: u64,
    session_epoch: u64,
}

impl PartRecord {
    /// This part's ordered chunks, each already validated structurally (leg 1i).
    pub fn chunks(&self) -> &[ChunkRef] {
        &self.chunks
    }

    /// The part's logical length in bytes — the checked sum of `chunks`' lengths (leg 1k).
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether the part carries no bytes — read off the `len` leg 1k has already reconciled
    /// with `chunks`. A zero-length part is a **value**, not a decode error: ADR-0045's
    /// `ChunkRef` row asks for a length consistent with the scheme, not a non-zero one, and
    /// the erasure encoder handles a zero-length chunk (`erasure.rs:79-83`, `shard_size`'s
    /// `.max(1)`). Present because [`Self::len`] is (clippy's `len_without_is_empty`).
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The part's content digest.
    pub const fn digest(&self) -> &Digest {
        &self.digest
    }

    /// When the part was committed.
    pub const fn committed_at_millis(&self) -> u64 {
        self.committed_at_millis
    }

    /// The session epoch this commit was fenced under.
    pub const fn session_epoch(&self) -> u64 {
        self.session_epoch
    }
}

/// The part's own rules, applied to chunks whose own [`Deserialize`] has already judged their
/// shape — the one place a [`PartRecord`] can come into existence:
///
/// * **1i (`PartRecord` half)** every chunk's `EcScheme` passes `checked_chunk_scheme` —
///   structural, at decode. A chunk's `placement` length is deliberately **not** checked
///   here: it is the standing *contextual* check, liberal on read (ADR-0045,
///   `AGENTS.md:146-149`, `0016:416-429`), so a `ChunkRef` whose `placement` length does not
///   match its scheme's fragment count still decodes.
/// * **1k** `len` equals the `checked_chunk_len` sum of `chunks`, overflow-checked —
///   mirrors [`crate::metadata::SegmentRecord`]'s own `from_wire` `checked_chunk_bytes`
///   comparison (`metadata.rs:1170-1185`, `:1208-1218`).
impl TryFrom<PartRecordWire> for PartRecord {
    type Error = RecordError;

    fn try_from(wire: PartRecordWire) -> Result<Self, RecordError> {
        let chunks: Vec<ChunkRef> = wire.chunks.into_iter().map(ChunkRef::from).collect();
        for chunk in &chunks {
            checked_chunk_scheme(chunk)?;
        }
        let total = checked_chunk_len(&chunks)?;
        if total != wire.len {
            return Err(RecordError::PartLengthMismatch {
                declared: wire.len,
                chunks: total,
            });
        }
        Ok(Self {
            chunks,
            len: wire.len,
            digest: wire.digest,
            committed_at_millis: wire.committed_at_millis,
            session_epoch: wire.session_epoch,
        })
    }
}

/// Decode the `part:` value ([`part_key`]) with its rejection attributed — the peer of
/// [`decode_admission_record`]/[`decode_session_record`]/[`decode_slot_record`] for this
/// record, and necessary for the same reason.
pub fn decode_part_record(value: &[u8]) -> Result<PartRecord, RecordError> {
    let wire: PartRecordWire =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "part:",
            detail: err.to_string(),
        })?;
    require_canonical(PartRecord::try_from(wire)?, value, "part:")
}

/// A committed part's **summary**, the `psum:<upload-id>:<part-number>` value (`0016:352`):
/// everything about the part except its chunk list, a few tens of bytes, written in the same
/// batch as the `part:` record it summarizes so the pair is always consistent. No invariant of
/// its own — the `chunks`/`len` agreement it restates is [`PartRecord`]'s own commit-time
/// obligation, not a relation this record can check in isolation from the part it pairs with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartSummary {
    /// How many chunks the summarized part holds.
    pub chunks: u32,
    /// The summarized part's logical length.
    pub len: u64,
    /// The summarized part's digest.
    pub digest: Digest,
    /// When the summarized part was committed.
    pub committed_at_millis: u64,
}

/// Decode the `psum:` value ([`psum_key`]) with its rejection attributed, for the same reason
/// the other per-record decoders in this section exist — [`PartSummary`] has no `TryFrom` of
/// its own, but a malformed value should still surface [`RecordError::MalformedRecordValue`]
/// rather than a bare [`crate::metadata::decode`] error escaping untyped.
pub fn decode_part_summary(value: &[u8]) -> Result<PartSummary, RecordError> {
    let summary: PartSummary =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "psum:",
            detail: err.to_string(),
        })?;
    require_canonical(summary, value, "psum:")
}

// ===========================================================================
// 9. The retirement obligation — the `retire:<mode>:<token>` VALUE (`0016:355-388`)
// ===========================================================================

/// A **range-encoded** part-number set (`0016:382-388`): `[[1, 400]]` for a contiguous run, so
/// the common "every staged part was published" case is a few bytes and the worst case — 10,000
/// alternating part numbers — still fits inside one value.
///
/// The encoding is **canonical**, and that is the whole reason this is a type rather than a
/// `Vec<(u32, u32)>`: the runs are ordered, non-overlapping and **non-adjacent**, so `[[1,4]]`
/// is the *only* spelling of `{1,2,3,4}` and `[[1,2],[3,4]]` is not a second one
/// ([`RecordError::PartNumberRunsNotCoalesced`]). A retirement obligation is installed under
/// `require_absent(retire:<mode>:<token>)` and drained under `require(retire:… == prior)`
/// (`0016:369-373`, `:667`), so two spellings of one set are two records one drain would answer
/// twice and one `require` could never match — the value-side form of the two-spellings-of-one-key
/// hazard the module's key grammar refuses (**C-1**, `docs/principles.md` §5).
///
/// Every endpoint is a [`PartNumber`]: `[1, MAX_PART_NUMBER]`, the **format** bound the
/// `part:`/`psum:`/`sidx:` key grammar can spell, never the live `MAX_PARTS_PER_SESSION` knob
/// (`0016:390-402`). A number past it would name a `part:` record no parser could read back.
/// Those two rules together are also the set's **cardinality** bound, so no separate one is
/// spelled: runs must ascend and never abut, so the most a canonical set can hold is
/// `⌈MAX_PART_NUMBER / 2⌉` of them — an alternating set over the whole key space, the worst case
/// `0016:382-388` sizes the encoding for.
///
/// It is also **non-empty by construction**. A part set naming no part is an obligation that
/// owes nothing — residue a drain would clear having marked nothing — so the emptiness rule lives
/// in the type rather than in each writer's discipline: neither constructor can mint one, which
/// is what keeps a writer (#656–#659) from computing an empty set for the root flip's unnamed
/// staged parts (`0016:662`, `:919-921`) and storing an obligation this decoder then refuses,
/// under a key the session's terminal-delete gate can never see emptied (`0016:673`).
///
/// There is deliberately **no** `Deserialize`: the only decode path is [`RetirePayload`]'s wire
/// shape, which routes through [`Self::from_runs`] so every rejection arrives as its own typed
/// [`RecordError`] rather than as a serde message — the reason [`crate::metadata::SegmentNonce`]
/// carries none either (`metadata.rs:757-760`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct PartNumberSet(Vec<(u32, u32)>);

impl PartNumberSet {
    /// The validating constructor every decode routes through — the one home of the
    /// canonical-spelling and non-emptiness rules (ADR-0045, parse-don't-validate: a
    /// non-canonical or empty set is an error at the boundary, never a value inside the program).
    pub fn from_runs(runs: Vec<(u32, u32)>) -> Result<Self, RecordError> {
        if runs.is_empty() {
            return Err(RecordError::RetireObligationOwesNothing { component: "parts" });
        }
        let mut previous_hi: Option<u32> = None;
        for &(lo, hi) in &runs {
            PartNumber::new(lo)?;
            PartNumber::new(hi)?;
            if lo > hi {
                return Err(RecordError::PartNumberRunReversed { lo, hi });
            }
            if let Some(previous_hi) = previous_hi {
                // `previous_hi + 1` cannot overflow: `PartNumber::new` bounded it by
                // `MAX_PART_NUMBER`, three orders of magnitude below `u32::MAX`.
                if lo <= previous_hi + 1 {
                    return Err(RecordError::PartNumberRunsNotCoalesced { lo, previous_hi });
                }
            }
            previous_hi = Some(hi);
        }
        Ok(Self(runs))
    }

    /// Mint the set from any iterator of part numbers, coalescing contiguous runs — the
    /// writer-side counterpart of [`Self::from_runs`]: it sorts, deduplicates and coalesces, so it
    /// can only produce the canonical encoding its own decode accepts. It exists so that no writer
    /// (#656–#659) has to spell the rule a second time; a hand-built run vector is exactly how a
    /// second spelling of one obligation gets stored.
    ///
    /// `None` for an iterator naming **no** part, because the empty set is not a value this type
    /// has: the writer rows that compute one compute a *possibly*-empty set — the parts a Complete
    /// left unnamed (`0016:662`, `:919-921`) — and the empty case is not an obligation to install
    /// under a smaller payload but an obligation not to install at all, since a stored empty set
    /// is residue no drain can clear (`0016:673`). Answering `None` is what puts that decision in
    /// front of the writer instead of inside a record its own decoder would refuse.
    pub fn from_numbers(numbers: impl IntoIterator<Item = PartNumber>) -> Option<Self> {
        let sorted: std::collections::BTreeSet<u32> =
            numbers.into_iter().map(PartNumber::get).collect();
        let mut runs: Vec<(u32, u32)> = Vec::new();
        for number in sorted {
            match runs.last_mut() {
                // `last.1 + 1` cannot overflow: every input is a [`PartNumber`], so every run
                // endpoint is bounded by `MAX_PART_NUMBER` — the same argument `from_runs` makes.
                Some(last) if last.1 + 1 == number => last.1 = number,
                _ => runs.push((number, number)),
            }
        }
        (!runs.is_empty()).then_some(Self(runs))
    }

    /// The set's runs, `[lo, hi]` inclusive, ascending, non-adjacent and never empty.
    ///
    /// There is deliberately no `len`, `is_empty` or member iterator beside it: the runs are the
    /// stored shape, emptiness is not a state this type has, and the drain that walks them
    /// (#656–#659) is what decides how to page a run of up to [`MAX_PART_NUMBER`] members. A
    /// convenience iterator minted here would be an API this child has no consumer for and no
    /// test that could pin its paging behaviour.
    pub fn runs(&self) -> &[(u32, u32)] {
        &self.0
    }
}

/// The literal a [`PartScope::All`] is spelled as.
const ALL_PARTS: &str = "all";

/// Which of a session's parts an obligation covers (`0016:355-356`, `:2187` vs `:2193`).
///
/// The two arms are the two things a writer can know at install time, and they are **not** two
/// spellings of one thing: the fence that tears down an `Open` session names no list, because a
/// list frozen by the caller is read *before* its fence lands and a part commit that won the
/// read-then-fence window would be missing from it — the fence makes the session's own bounded
/// `part:<id>:` range immutable, so [`Self::All`] is the instruction to enumerate that range at
/// drain time (`0016:2187`, `:673`). A publication or a `Completing` rollback, by contrast,
/// names the exact part numbers it published or left staged, because *which* parts they are is
/// the whole content of the obligation (`0016:662`, `:919-921`, `:2193`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartScope {
    /// Every part the token's session holds, enumerated from its own bounded ranges at drain
    /// time. Spelled `"all"`, and legal **only** in the `retire:bytes:` session teardown
    /// `{session, all}` that freezes that range (`0016:2187`) — see `ALL_PARTS_COMPONENT` and
    /// [`RecordError::RetireAllPartsWithoutSession`] for the two halves of that rule.
    All,
    /// Exactly these part numbers, frozen under the fence that installed the obligation.
    /// Spelled as the range encoding itself, `[[1,4],[7,9]]` (`0016:382-388`).
    Set(PartNumberSet),
}

impl Serialize for PartScope {
    /// Serialized as the value `0016:382` writes — the range encoding **itself**, not wrapped in
    /// a tag object, so `parts` reads as `[[1, 400]]` there and here alike; the wildcard is the
    /// one string no range encoding can be confused with.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::All => serializer.serialize_str(ALL_PARTS),
            Self::Set(set) => set.serialize(serializer),
        }
    }
}

/// The wire shape of [`PartScope`]: the wildcard string, or the raw range encoding for
/// [`PartNumberSet::from_runs`] to judge — so a non-canonical set is attributed to its own
/// [`RecordError`] variant rather than arriving as a serde message.
enum PartScopeWire {
    All,
    Set(Vec<(u32, u32)>),
}

impl<'de> Deserialize<'de> for PartScopeWire {
    /// Hand-written rather than `#[serde(untagged)]`, for the rejection: an untagged enum
    /// reports only "data did not match any variant of untagged enum PartScopeWire", naming an
    /// internal type and no rule, where the visitor below names the two spellings a `parts`
    /// component may take. A third spelling is a decode error either way — this one an operator
    /// can act on.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ScopeVisitor;

        impl<'de> serde::de::Visitor<'de> for ScopeVisitor {
            type Value = PartScopeWire;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    "the wildcard {ALL_PARTS:?} or a range-encoded part-number set"
                )
            }

            fn visit_str<E: DeError>(self, value: &str) -> Result<PartScopeWire, E> {
                if value == ALL_PARTS {
                    Ok(PartScopeWire::All)
                } else {
                    Err(E::invalid_value(serde::de::Unexpected::Str(value), &self))
                }
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                seq: A,
            ) -> Result<PartScopeWire, A::Error> {
                Vec::<(u32, u32)>::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
                    .map(PartScopeWire::Set)
            }
        }

        deserializer.deserialize_any(ScopeVisitor)
    }
}

impl TryFrom<PartScopeWire> for PartScope {
    type Error = RecordError;

    fn try_from(wire: PartScopeWire) -> Result<Self, RecordError> {
        Ok(match wire {
            PartScopeWire::All => Self::All,
            PartScopeWire::Set(runs) => Self::Set(PartNumberSet::from_runs(runs)?),
        })
    }
}

/// One **present** retirement chunk list, judged: never spelled `[]`, and every chunk's stored
/// `EcScheme` one [`crate::erasure`] can encode/decode. The single home of both rules, shared by
/// the payload's own `chunks` component and a flat generation's inline list, so neither can be
/// given the weaker check (an *absent* list is its caller's question — the component is simply
/// not there):
///
/// * a **present but empty** list is [`RecordError::RetireObligationOwesNothing`] — a component
///   that owes nothing is residue a drain would clear having marked nothing. It is refused
///   *here*, where the list is read, rather than left to the canonical-bytes gate to reject as a
///   re-encode mismatch (the field is skipped when empty, so the two bytes differ): the operator
///   signal then names the empty list rather than "non-canonical bytes", and the two rules stay
///   separately falsifiable — each has its own negation leg in the named test;
/// * every chunk passes [`checked_chunk_scheme`] — an obligation's chunk list is exactly the
///   untrusted stored geometry a drain fans its orphan marks out over, the #285 class made
///   durable (ADR-0045's invariant table, `0045:71-72`). A chunk's `placement` **length** is
///   deliberately not checked: the standing contextual check, liberal on read (ADR-0045
///   `:45-49`, `:72`; `AGENTS.md:146-149`; `0016:416-432`).
fn checked_chunks(
    wire: Vec<ChunkRefWire>,
    component: &'static str,
) -> Result<Vec<ChunkRef>, RecordError> {
    if wire.is_empty() {
        return Err(RecordError::RetireObligationOwesNothing { component });
    }
    let chunks: Vec<ChunkRef> = wire.into_iter().map(ChunkRef::from).collect();
    for chunk in &chunks {
        checked_chunk_scheme(chunk)?;
    }
    Ok(chunks)
}

/// The wire shape of [`RetireGeneration`], closed and reading its chunks through the module's
/// own [`ChunkRefWire`] for the reason that type records. Both reclamation sources are optional
/// here — **which combinations are legal is [`RetireGeneration`]'s rule, not serde's** (an
/// untagged enum would report only "data did not match any variant"), so each rejection arrives
/// as its own [`RecordError`] naming the generation it was found on.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetireGenerationWire {
    inode: InodeId,
    version: u64,
    #[serde(default)]
    chunks: Option<Vec<ChunkRefWire>>,
    #[serde(default)]
    segments: Option<SegmentGroup>,
}

/// Which of the two reclamation sources a retired generation's bytes are named by — the
/// **committed map this obligation mirrors**, and therefore a two-arm choice rather than a pair:
/// [`crate::metadata::ChunkMap`] is `Flat(Vec<ChunkRef>) | Segmented(SegmentedMap)`
/// (`metadata.rs:1002-1021`), and a [`crate::metadata::SegmentedMap`] carries **no** inline
/// chunks — its chunks live in the `seg:` records its group names. A generation was published
/// under one of those two shapes, so exactly one of them names its bytes.
///
/// Making that a type rather than a rule over two optional fields is ADR-0045's
/// parse-don't-validate over this child's category: a generation owing both, or neither, is
/// **unrepresentable** once decoded rather than merely refused somewhere (`0045:42-49`).
///
/// Its arms are open, as [`crate::metadata::ChunkMap`]'s are, so the **non-emptiness** of a
/// `Flat` list is not a property of this type but of the decode that reads one (`checked_chunks`
/// — an obligation owing nothing is residue no drain can clear). That is sound here for the same
/// reason no record type in this module has a writer-side constructor: a [`RetireGeneration`] can
/// only come into existence by decoding, so a hand-built `RetiredMap` has nowhere to go. The
/// first writers (#656–#659) inherit that obligation with the constructor they add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetiredMap {
    /// A **flat** generation's own chunk list, copied into the obligation because the root that
    /// named it is being overwritten or unlinked in the same batch — after which nothing else
    /// names those fragments (`0016:355`, `:668`).
    Flat(Vec<ChunkRef>),
    /// A **segmented** generation's group, naming its `seg:<group-nonce>:<epoch>:` range. The
    /// segments are named by their **keys**, never by frozen placements: the drain resolves that
    /// bounded range and orphan-marks each record's *current* `ChunkRef.placement`, so a fragment
    /// a reconstruction or rebalance repoint moved before the supersede won its inode CAS is
    /// marked at the position it actually occupies (`0016:2417-2425`).
    Segmented(SegmentGroup),
}

/// A **superseded or deleted object generation** whose bytes a `retire:bytes:` obligation owes
/// (`0016:355`, `:668`, `:2416-2425`): the `(inode, version)` pair exactly one publication
/// produced, and the one [`RetiredMap`] that named its bytes.
///
/// # Exactly one source, and why `0016` looks like it says otherwise
///
/// `0016` spells the row two ways — `{inode, version, chunks, segments?}` at `:355` and
/// `{inode, version, chunks?, segments}` at `:2417`. Those are the **two cases**, not a union:
/// a flat generation retires by its copied `chunks` (`:355`, the supersede of a flat root) and a
/// segmented one by its `segments` group, re-read at drain time (`:2417`, the supersede of a
/// segmented root). The obligation mirrors the committed map it retires, and that map is the
/// two-arm [`crate::metadata::ChunkMap`] (`metadata.rs:1014`) — there is no published shape with
/// both — so **both present is a decode error**
/// ([`RecordError::RetireGenerationBothSources`]) and neither present is an obligation owing
/// nothing ([`RecordError::RetireObligationOwesNothing`]). Settled for this record format by the
/// human on 2026-09-11 after the two spellings above had been read as a union; the erratum
/// against `0016`'s `:355` row rides the PR description, and this module does not edit the
/// proposal.
///
/// A shape no writer can install is not a shape the record should be able to hold: accepting it
/// would leave the first drain (#656–#659) to invent a meaning for a value the protocol never
/// produces — orphan-mark the inline list *and* walk a segment range, on a generation only one
/// of them ever described.
///
/// Whichever source it carries is **omitted when absent**, never spelled `[]` or `null`, so the
/// accepted set stays exactly the encoder's image (`AGENTS.md:170-172`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireGeneration {
    inode: InodeId,
    version: u64,
    map: RetiredMap,
}

impl RetireGeneration {
    /// The inode the retired generation belonged to.
    pub const fn inode(&self) -> InodeId {
        self.inode
    }

    /// The retired generation's version.
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// The one source naming its bytes — the arm the drain dispatches on.
    pub const fn map(&self) -> &RetiredMap {
        &self.map
    }
}

impl Serialize for RetireGeneration {
    /// Written through a closed wire struct, the shape [`crate::metadata::SegmentedMap`]
    /// serializes through (`metadata.rs:983-992`): the arm decides **which** of the two fields
    /// exists, and the absent one is omitted rather than spelled `null`, so decode→encode is the
    /// identity for both cases.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Wire<'a> {
            inode: InodeId,
            version: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            chunks: Option<&'a [ChunkRef]>,
            #[serde(skip_serializing_if = "Option::is_none")]
            segments: Option<&'a SegmentGroup>,
        }
        let (chunks, segments) = match &self.map {
            RetiredMap::Flat(chunks) => (Some(chunks.as_slice()), None),
            RetiredMap::Segmented(group) => (None, Some(group)),
        };
        Wire {
            inode: self.inode,
            version: self.version,
            chunks,
            segments,
        }
        .serialize(serializer)
    }
}

/// The generation's own rules, the ones knowable from the value alone: it names **exactly one**
/// reclamation source, and the source it names is not empty. Its remaining rules are relations —
/// its chunks' geometry, judged with the payload's own by `checked_chunks`, and its
/// `(inode, version)` against the `g:` token that names it (`RetirePayload::checked_against_key`).
impl TryFrom<RetireGenerationWire> for RetireGeneration {
    type Error = RecordError;

    fn try_from(wire: RetireGenerationWire) -> Result<Self, RecordError> {
        let map = match (wire.chunks, wire.segments) {
            (Some(_), Some(_)) => {
                return Err(RecordError::RetireGenerationBothSources {
                    inode: wire.inode,
                    version: wire.version,
                })
            }
            (Some(chunks), None) => RetiredMap::Flat(checked_chunks(chunks, "generation.chunks")?),
            (None, Some(group)) => RetiredMap::Segmented(group),
            (None, None) => {
                return Err(RecordError::RetireObligationOwesNothing {
                    component: "generation",
                })
            }
        };
        Ok(Self {
            inode: wire.inode,
            version: wire.version,
            map,
        })
    }
}

/// Which token kind a payload component's writer row files it under (`0016:358-366`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenScope {
    /// The whole session at one epoch — the **suffix-free** `s:<upload-id>:<epoch>` token.
    SessionWide,
    /// One part attempt — `s:<upload-id>:<epoch>:<part-number>:<attempt-id>`.
    PerPart,
    /// One object generation — `g:<inode-id>:<version>`.
    Generation,
}

/// One component of a [`RetirePayload`], with the mode and the token scope **its own writer row
/// fixes**. The six constants below are that writer table, transcribed once: `0016`'s value
/// column (`:355-356`) and the batches that install each obligation (`:659-673`, `:2187`,
/// `:2193`, `:2417`). Every cross-check a decoder can make against a key is a lookup in it, so
/// no rule is spelled twice and no component can be given a weaker check than its siblings.
#[derive(Debug, Clone, Copy)]
struct Component {
    /// The field name, for the rejection that reports which component was found.
    name: &'static str,
    /// The mode this component may live under, or `None` for a component both modes install.
    mode: Option<RetireMode>,
    /// The token kind that names it.
    scope: TokenScope,
}

/// `{session, …}` — the session's **own staged residue**: its owned `sidx:` fragments, orphan-marked
/// and then deleted by this obligation (`0016:355`, "bytes to orphan-mark, then the naming records
/// to delete"; `:2587` states the marking explicitly).
///
/// **Bytes only.** Every writer row that names it installs it under `retire:bytes:` — the abort /
/// reap fence (`0016:664`, `:1001`), the `Completing`→`Aborting` and restore fences (`:665`,
/// `:823`), the reaper's two arms (`:2187`, `:2193`). Under `retire:records:` it would mean
/// *delete those staging records without marking their bytes*, which strands durable fragments
/// with no record naming them and no orphan evidence — outcome (a) (`0016:369-373`). The
/// `retire:records:` row's value column reuses the `{session, parts}` shorthand (`0016:356`), but
/// its own prose and every batch row give that namespace exactly two contents: the **published**
/// parts' records and one rolled-back attempt's segments (`:356`, `:662`, `:663`, `:665`, `:823`,
/// `:2194`). The session's *own* records are the terminal delete's, gated on this obligation
/// already having drained (`:673`) — never an obligation's.
const SESSION_COMPONENT: Component = Component {
    name: "session",
    mode: Some(RetireMode::Bytes),
    scope: TokenScope::SessionWide,
};

/// `{parts: <set>}` — an **explicit** part-number set, under either mode: their **bytes** for the
/// staged parts a Complete did not name (`0016:662`, `:919-921`) and their **records** for the
/// published ones (`0016:662`, `:356`).
const PARTS_COMPONENT: Component = Component {
    name: "parts",
    mode: None,
    scope: TokenScope::SessionWide,
};

/// `{parts: "all"}` — the wildcard, a **different writer row** from an explicit set and therefore
/// its own entry in this table: exactly one batch installs it, the reaper's `Open` teardown
/// `retire:bytes:{session, all}` (`0016:2187`), and it is legal only in that shape. Under
/// `retire:records:` it would tell a drain to delete **every** part record of a live session —
/// including the staged parts whose records are the only thing protecting their bytes and the only
/// source of their placements, the precise deletion `0016:919-921` (iteration-14 finding 3, X104)
/// forbids. The companion rule that it may not appear without `session` is
/// [`RecordError::RetireAllPartsWithoutSession`]: `all` is an instruction to enumerate a range the
/// installing fence has just frozen, and only the session teardown freezes it.
const ALL_PARTS_COMPONENT: Component = Component {
    name: "parts:all",
    mode: Some(RetireMode::Bytes),
    scope: TokenScope::SessionWide,
};

/// `{chunks: […]}` — the **per-part** obligation: a re-uploaded part's superseded chunks, a
/// losing writer's compensation, a post-staging local refusal (`0016:659`, `:672`, `:1620`).
/// Bytes only — its chunks have no naming record left, so they must be orphan-marked.
const CHUNKS_COMPONENT: Component = Component {
    name: "chunks",
    mode: Some(RetireMode::Bytes),
    scope: TokenScope::PerPart,
};

/// `{generation: {…}}` — a superseded or deleted object generation (`0016:355`, `:668`,
/// `:2417`). Bytes only, and the one component a `g:` token names.
const GENERATION_COMPONENT: Component = Component {
    name: "generation",
    mode: Some(RetireMode::Bytes),
    scope: TokenScope::Generation,
};

/// `{seg: {nonce, epoch}}` — one rolled-back `Completing` attempt's dangling segment records
/// (`0016:663`, `:665`, `:823`). **Records only**: those segments' fragments are still protected
/// by the `part:` records, so orphan-marking them would mark live bytes (`0016:2347-2350`).
const SEG_COMPONENT: Component = Component {
    name: "seg",
    mode: Some(RetireMode::Records),
    scope: TokenScope::SessionWide,
};

/// The wire shape of [`RetirePayload`] — **closed** (`deny_unknown_fields`) and reading every
/// nested chunk through this module's own [`ChunkRefWire`], for the reasons that type records: a
/// retirement obligation is installed and drained under exact-bytes preconditions
/// (`0016:369-373`, `:667`), so a field silently dropped or defaulted on the way in is a record
/// nothing can precondition on.
///
/// Each component is optional **on the wire** and load-bearing in combination: `0016`'s writer
/// rows install `{session, parts}` (`:665`, `:2193`) and `{parts}` **and/or** `{seg}` (`:356`)
/// as single values under single keys, so the shape is a set of components rather than a closed
/// list of alternatives.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirePayloadWire {
    #[serde(default)]
    session: bool,
    #[serde(default)]
    parts: Option<PartScopeWire>,
    #[serde(default)]
    chunks: Option<Vec<ChunkRefWire>>,
    #[serde(default)]
    generation: Option<RetireGenerationWire>,
    #[serde(default)]
    seg: Option<SegmentGroup>,
}

/// What one retirement obligation owes — the `retire:bytes:<token>` / `retire:records:<token>`
/// value (`0016:355-356`).
///
/// # Why it is a set of components, not a choice between them
///
/// `0016`'s writer rows install **combined** obligations, and a payload type whose arms made
/// them inexpressible would silently force a writer to install two records where the protocol
/// installs one — two keys where `require_absent` and the session's own emptiness gate expect
/// one (`0016:369-380`). The rows, each a single value under a single key:
///
/// | Key | Payload | Row |
/// |---|---|---|
/// | `retire:bytes:s:<id>:<E>` | `{session}` | the abort/reap fence's own spelling (`0016:664`) — the session's staged residue, no part |
/// | `retire:bytes:s:<id>:<E>` | `{session, all}` | the reaper's `Open` teardown, that residue **and** every part (`0016:2187`) — the **only** row the `all` wildcard has |
/// | `retire:bytes:s:<id>:<E>` | `{session, parts: <set>}` | the `Completing`→`Aborting` fence, incl. the restore fence (`0016:665`, `:823`, `:2193`) |
/// | `retire:bytes:s:<id>:<E>` | `{parts: <set>}` | the root flip's **unnamed** staged parts (`0016:662`, `:919-921`) |
/// | `retire:bytes:s:<id>:<E>:<n>:<a>` | `{chunks: […]}` | a losing writer / re-upload compensation (`0016:659`, `:672`, `:1620`) |
/// | `retire:bytes:g:<inode>:<version>` | `{generation: {…, chunks}}` | supersede or unlink of a **flat** generation, by the chunk list its root carried (`0016:355`, `:668`) |
/// | `retire:bytes:g:<inode>:<version>` | `{generation: {…, segments}}` | supersede or unlink of a **segmented** one, by the `seg:` range its group names (`0016:2417`) — **exactly one of the two**, never both ([`RetiredMap`]) |
/// | `retire:records:s:<id>:<E>` | `{parts: <set>}` | the root flip's **published** parts (`0016:662`, `:919-921`) |
/// | `retire:records:s:<id>:<E>` | `{seg: {…}}` | a `Completing` rollback's dangling segments (`0016:663`, `:665`) |
/// | `retire:records:s:<id>:<E>` | `{parts: <set>, seg: {…}}` | both, in one payload (`0016:356`, "and/or") |
///
/// The one place the record *is* a choice is one level in, and for the opposite reason: a
/// generation's [`RetiredMap`] mirrors the two-arm committed map it retires, so its two sources
/// are exclusive where the payload's components are combinable.
///
/// # Its identity lives partly in its key
///
/// The mode is the key's prefix (`0016:434-441`) and the obligation's identity is its token
/// (`0016:358-380`), so this value is decoded **against** the key that names it —
/// [`decode_retire_obligation`], the only entry point, takes both halves. Every component
/// carries the mode and token scope of its own writer row (the `Component` table above), and a
/// payload that disagrees with its key is a typed error rather than a value: one accepted under the
/// wrong token — or the wrong *scope* of token — reclaims one attempt's data while clearing
/// another's obligation, and the loss is invisible because no record names the bytes any more
/// (`0016:369-373`, outcome (a); ADR-0045 decision 1).
///
/// # No `Deserialize`, and no writer-side constructor
///
/// Every other record class in this module derives `Deserialize`, so the store-wide
/// [`crate::metadata::decode`] can read it from a value alone. **This one deliberately does
/// not.** Most of its rules are relations against its key, so a value-only decode would hand a
/// caller a `RetirePayload` that has never met the token naming it — precisely the value
/// ADR-0045 decision 1 says must not exist, and precisely the value the drain (#656–#659) must
/// never act on. Absence of the impl is what makes that unreachable: a `metadata::decode`
/// turbofished with this type does not compile, so the single decode surface is
/// [`decode_retire_obligation`] and the key-relation checks cannot be skipped by reaching for
/// the store-wide seam the sibling records use. [`Serialize`] stays: encoding an
/// already-validated payload is what the canonical-bytes gate and every future writer need.
///
/// There is no writer-side constructor either, as for every record type in this module: the
/// first writers are the store round trips (#656–#659), and a value that could be built without
/// passing decode's rules is a value those writers could make durable without them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RetirePayload {
    /// Omitted when absent rather than spelled `false` (`AGENTS.md:170-172`), so the accepted
    /// set stays exactly the encoder's image.
    #[serde(skip_serializing_if = "is_absent")]
    session: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    parts: Option<PartScope>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    chunks: Vec<ChunkRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation: Option<RetireGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seg: Option<SegmentGroup>,
}

/// Whether a presence flag is absent — the `skip_serializing_if` that keeps an absent `session`
/// component **omitted** instead of spelled `false`.
const fn is_absent(present: &bool) -> bool {
    !*present
}

impl RetirePayload {
    /// Whether the obligation owes the session's **own staged residue** — its owned `sidx:`
    /// fragments, orphan-marked and then deleted (`0016:355`, `:2587`) — distinct from the parts
    /// it may also name. Never the `mpu:`/`slot:` records themselves: those are the terminal
    /// delete's, and it is gated on this obligation having already drained (`0016:673`).
    pub const fn session(&self) -> bool {
        self.session
    }

    /// Which of the session's parts it owes, if any.
    pub const fn parts(&self) -> Option<&PartScope> {
        self.parts.as_ref()
    }

    /// The chunk list of a per-part obligation — each chunk's stored geometry already judged by
    /// `checked_chunk_scheme`.
    pub fn chunks(&self) -> &[ChunkRef] {
        &self.chunks
    }

    /// The superseded or deleted generation it owes, if any.
    pub const fn generation(&self) -> Option<&RetireGeneration> {
        self.generation.as_ref()
    }

    /// The rolled-back attempt's segment group it owes, if any.
    pub const fn segments(&self) -> Option<&SegmentGroup> {
        self.seg.as_ref()
    }

    /// The components this payload actually carries, each with the mode and token scope its
    /// writer row fixes. An absent component is silent: it is checked against nothing.
    ///
    /// The two `parts` spellings are **two rows** of that table, not one: an explicit set is
    /// installed under either mode, while the `all` wildcard has exactly one writer row
    /// (`0016:2187`) — so they are looked up as different components and a wildcard can never
    /// inherit an explicit set's permissions.
    fn present_components(&self) -> impl Iterator<Item = Component> + '_ {
        [
            (self.session, SESSION_COMPONENT),
            (
                matches!(self.parts, Some(PartScope::Set(_))),
                PARTS_COMPONENT,
            ),
            (
                matches!(self.parts, Some(PartScope::All)),
                ALL_PARTS_COMPONENT,
            ),
            (!self.chunks.is_empty(), CHUNKS_COMPONENT),
            (self.generation.is_some(), GENERATION_COMPONENT),
            (self.seg.is_some(), SEG_COMPONENT),
        ]
        .into_iter()
        .filter_map(|(present, component)| present.then_some(component))
    }

    /// The one rule that holds wherever the value is decoded, key or no key: the obligation owes
    /// **something**. A payload carrying no component at all is residue nothing drains — a drain
    /// would mark nothing, delete the obligation, and record the work as done.
    ///
    /// Its per-component forms are their own types' rules, already applied by the time a payload
    /// exists, and each names the component it found rather than the whole value: an empty part
    /// set is unrepresentable ([`PartNumberSet`]), a present-but-empty chunk list is refused
    /// where it is read ([`checked_chunks`], which also judges every chunk's geometry there), and
    /// a generation naming neither of its two reclamation sources is refused by
    /// [`RetireGeneration`]'s own decode.
    fn checked_shape(&self) -> Result<(), RecordError> {
        if self.present_components().next().is_none() {
            return Err(RecordError::RetireObligationOwesNothing {
                component: "payload",
            });
        }
        Ok(())
    }

    /// The rules **only a key-taking decode can make** — the payload against the key that names
    /// it:
    ///
    /// * **mode** — each component's mode is its writer row's (`0016:434-441`): `chunks`,
    ///   `generation` and `session` orphan-mark, so they are `retire:bytes:` only; `seg` must
    ///   never orphan anything, so it is `retire:records:` only (`0016:2347-2350`). Two
    ///   components whose modes differ therefore cannot share a payload at all, which is what
    ///   keeps every cross-component combination `0016` does not install unrepresentable rather
    ///   than separately refused;
    /// * **token scope** — a `generation` lives only under a `g:` token and every other
    ///   component only under an `s:` one, and the `s:` token's optional
    ///   `:<part-number>:<attempt-id>` suffix is present for exactly the per-part component
    ///   (`0016:358-366`);
    /// * **generation identity** — a `generation` names the same `(inode, version)` its `g:`
    ///   token does;
    /// * **segment epoch** — a `seg` group's epoch is its token's epoch, exactly;
    /// * **the wildcard's one row** — `parts: "all"` is legal only in the session teardown
    ///   `{session, all}` the reaper's `Open` arm installs (`0016:2187`). The mode half is the
    ///   `ALL_PARTS_COMPONENT` row above; the `session` half is here, because it is a relation
    ///   between two components rather than between one component and the key. `all` is the
    ///   instruction to enumerate the session's own `part:<id>:` range **at drain time**, and
    ///   only a teardown fence makes that range immutable (`0016:664`, `:2187`); without the
    ///   teardown it would name whatever a still-live session happens to hold when the drain
    ///   arrives.
    ///
    /// ## What the segment check does **not** prove
    ///
    /// It binds the **epoch component only**. A segment group's nonce is deliberately
    /// independent of the upload id, because segment records outlive the `mpu:` tombstone that
    /// would otherwise be their only reuse guard (`0016:499-509`), so a *foreign* session's
    /// group carrying the right epoch is **not** detectable at decode — the group's identity is
    /// the installing writer's to establish and the drain's to act on (#656–#659). What this
    /// check does establish is the property `require_absent` needs: **one canonical key per
    /// obligation**. The fence that ends an attempt is the batch that installs the obligation
    /// naming that attempt's segments (`0016:663-665`, `:2357-2362`), and it preconditions on
    /// `require(mpu == Completing@E)`, so `E` is both the token's epoch and the epoch whose
    /// `seg:` keys the payload names. Admitting `E ± 1` as well would give one obligation
    /// several legal keys, and an installer's `require_absent` on one of them cannot see the
    /// others: the same obligation is installed and drained twice (`0016:369-373`).
    fn checked_against_key(
        &self,
        mode: RetireMode,
        token: &RetireToken,
    ) -> Result<(), RecordError> {
        for component in self.present_components() {
            component.checked_mode(mode)?;
            component.checked_scope(token)?;
        }
        if matches!(self.parts, Some(PartScope::All)) && !self.session {
            return Err(RecordError::RetireAllPartsWithoutSession);
        }
        // Both relations below are reached only for a token kind the scope check has already
        // proved, so neither arm can silently skip its rule: a `generation` under anything but a
        // `g:` token, or a `seg` under anything but an `s:` one, has returned above.
        if let (Some(generation), RetireToken::Generation { inode, version }) =
            (&self.generation, token)
        {
            if generation.inode != *inode || generation.version != *version {
                return Err(RecordError::RetireGenerationIdentityMismatch {
                    key_inode: *inode,
                    key_version: *version,
                    payload_inode: generation.inode,
                    payload_version: generation.version,
                });
            }
        }
        if let (Some(group), RetireToken::Session { epoch, .. }) = (&self.seg, token) {
            if group.epoch() != *epoch {
                return Err(RecordError::RetireSegmentEpochMismatch {
                    key_epoch: *epoch,
                    segment_epoch: group.epoch(),
                });
            }
        }
        Ok(())
    }
}

impl Component {
    /// The mode half of this component's writer row (`0016:434-441`).
    fn checked_mode(self, mode: RetireMode) -> Result<(), RecordError> {
        match self.mode {
            Some(required) if required != mode => Err(RecordError::RetireModeMismatch {
                key_mode: mode,
                component: self.name,
            }),
            _ => Ok(()),
        }
    }

    /// The token half of this component's writer row (`0016:358-366`) — the kind of token, and
    /// for a session token whether it names a part attempt.
    fn checked_scope(self, token: &RetireToken) -> Result<(), RecordError> {
        let scope_mismatch = |named| {
            Err(RecordError::RetireTokenScopeMismatch {
                token: named,
                component: self.name,
            })
        };
        let suffix_mismatch = |token_names_part| {
            Err(RecordError::RetireTokenSuffixMismatch {
                component: self.name,
                token_names_part,
            })
        };
        match (self.scope, token) {
            (TokenScope::Generation, RetireToken::Session { .. }) => scope_mismatch("s:"),
            (TokenScope::SessionWide | TokenScope::PerPart, RetireToken::Generation { .. }) => {
                scope_mismatch("g:")
            }
            (TokenScope::SessionWide, RetireToken::Session { part: Some(_), .. }) => {
                suffix_mismatch(true)
            }
            (TokenScope::PerPart, RetireToken::Session { part: None, .. }) => {
                suffix_mismatch(false)
            }
            _ => Ok(()),
        }
    }
}

impl TryFrom<RetirePayloadWire> for RetirePayload {
    type Error = RecordError;

    fn try_from(wire: RetirePayloadWire) -> Result<Self, RecordError> {
        let payload = Self {
            session: wire.session,
            parts: wire.parts.map(PartScope::try_from).transpose()?,
            chunks: wire
                .chunks
                .map(|chunks| checked_chunks(chunks, "chunks"))
                .transpose()?
                .unwrap_or_default(),
            generation: wire
                .generation
                .map(RetireGeneration::try_from)
                .transpose()?,
            seg: wire.seg,
        };
        payload.checked_shape()?;
        Ok(payload)
    }
}

/// Decode a retirement obligation from **both halves of the record** — its key ([`retire_key`])
/// and its value — returning everything the pair means: the **mode** and the **token** the key
/// names, beside the payload the value carries.
///
/// The mode is part of the answer, not merely a check made and dropped, because it is what the
/// drain *dispatches* on and the two modes are opposite instructions over the same payload shape
/// (`0016:434-441`): `retire:bytes:{parts}` orphan-marks those parts' fragments and then deletes
/// their records, while `retire:records:{parts}` deletes records whose bytes a published object
/// still protects and must **never** orphan-mark anything. A caller holding only
/// `(token, payload)` would have to re-parse the key to tell them apart — a second spelling of a
/// decision this decode has already made, and the one place a drain could get it backwards.
///
/// The key is a parameter because most of this record class's rules are relations between the
/// two halves, and a decode that cannot see the key can check none of them: the **mode**
/// (`0016:434-441`), the token **scope** and its per-part **suffix** (`0016:358-366`), the
/// generation **identity**, the segment group's **epoch** (`0016:2357-2362`) and the one writer
/// row the `all` wildcard has (`0016:2187`). An obligation's identity lives partly in its token —
/// that is the whole point of `require_absent` on the key (`0016:369-373`) — so a value-only
/// decoder would vouch for a payload it has no way to attribute, which is how a drain comes to
/// reclaim one attempt's data while clearing another's. That is also why [`RetirePayload`] has no
/// `Deserialize`: this is not merely the *recommended* decode surface, it is the only one that
/// exists.
///
/// The value's own rules (an obligation or one component owing nothing, an unsupported nested
/// chunk geometry, a non-canonical part-number set) are each type's own and hold wherever the
/// value is decoded; they are applied **first**, so a torn value is attributed to the rule it
/// broke rather than to the key it happens to sit under — the order
/// [`decode_session_record`]/[`decode_part_record`] apply. It closes with the canonical-bytes
/// gate every decoder in this module closes with (`require_canonical`): every retirement
/// obligation is installed and drained under exact-bytes preconditions (`0016:369-373`, `:667`),
/// so a re-encode that is not the identity is a record nothing can precondition on.
pub fn decode_retire_obligation(
    key: &[u8],
    value: &[u8],
) -> Result<(RetireMode, RetireToken, RetirePayload), RecordError> {
    let (mode, token) = parse_retire_key(key)?;
    let wire: RetirePayloadWire =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "retire:",
            detail: err.to_string(),
        })?;
    let payload = RetirePayload::try_from(wire)?;
    payload.checked_against_key(mode, &token)?;
    Ok((mode, token, require_canonical(payload, value, "retire:")?))
}

// ===========================================================================
// 10. The owned staging entry — the `sidx:<upload-id>:<part-number>:<chunk-id>` VALUE
//     (`0016:353`, `:442-491`)
// ===========================================================================

/// The shape [`RecordError::PendingEntryNamespaceMismatch`] names for a value carrying both
/// ownership fields — an owned `sidx:` entry.
pub(crate) const OWNED_SHAPE: &str = "owned";
/// The shape it names for a value carrying neither — an ordinary `pending:` lease.
pub(crate) const ORDINARY_SHAPE: &str = "ordinary";

/// The **ownership pairing rule**, in one place (`0016:442-457`): a
/// [`crate::metadata::PendingEntry`]'s `owner` and `staged` are present together — an owned
/// `sidx:` entry — or absent together — an ordinary `pending:` lease. Exactly one is a torn value
/// no writer of this protocol produces.
///
/// One definition, applied by every seam that can meet the value: the shared record's own
/// `Deserialize` (so the `pending:` decode refuses a torn value as well), the `pending:`
/// namespace rule (`PendingEntry::checked_ordinary_lease`, so neither `pending:` writer stores
/// one), [`decode_owned_entry`]'s wire path (which needs the rejection typed rather than
/// stringified by serde's `Error::custom`), and [`OwnedEntry::from_pending`], the validator a
/// writer outside this crate can apply to a record it assembled by hand. So no two of them can
/// disagree about what "torn" means — the reason `Budget::inflight_owned_refs` is shared between
/// its rule and its charge.
///
/// It judges the value's shape alone. Which of the two valid shapes a value may have is a relation
/// against its **key**, checked by each namespace's own decode entry point — [`decode_owned_entry`]
/// for `sidx:`, [`crate::metadata::decode_pending_entry`] for `pending:` — and, for `pending:`,
/// by its writers too.
pub(crate) fn checked_ownership_pairing(
    owner_present: bool,
    staged_present: bool,
) -> Result<(), RecordError> {
    if owner_present == staged_present {
        return Ok(());
    }
    Err(RecordError::TornOwnedEntry {
        present: if owner_present { "owner" } else { "staged" },
        absent: if owner_present { "staged" } else { "owner" },
    })
}

/// The wire shape of [`StagedPlacement`], closed and reading its scheme through the module's own
/// [`EcSchemeWire`] for the reason [`ChunkRefWire`] records: an unknown field at either level is a
/// decode error, never a field dropped on the way in and missing from the re-encode a lease
/// renewal puts.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedPlacementWire {
    scheme: EcSchemeWire,
    placement: Vec<DServerId>,
}

/// A chunk's **planned** EC placement, carried only by an owned `sidx:` entry (`0016:442-473`):
/// the same `(scheme, per-fragment D-server vector)` a committed [`crate::metadata::ChunkRef`]
/// carries, written at intent time — before any fragment reaches a D server — so a record-only
/// reaper can compute the entry's `orphan:<dserver>:<chunk>:<index>` keys and a drain can count
/// its fragments as held on a specific server without a live write plan in hand
/// (`0016:459-473`).
///
/// **Geometry is judged at decode; length is not.** The scheme must be one
/// [`crate::erasure::supported`] can encode/decode (ADR-0045's invariant table, `0045:71`; the
/// #285 class): untrusted stored geometry such as `rs(0, 1)` is
/// [`RecordError::StagedSchemeUnsupported`], never a value the reaper's fragment arithmetic then
/// indexes with. The placement's **length** is deliberately not checked against the scheme's
/// fragment count: that is the standing *contextual* check, liberal on read (ADR-0045 `:45-49` and
/// its `ChunkRef` row `:72`; `AGENTS.md:146-149`; `0016:416-432`, which names this very record),
/// so a length-mismatched placement decodes. What a maintenance pass does with one is that pass's
/// to decide, and no pass in this tree reads a `sidx:` key yet.
///
/// The fields are private behind [`Self::new`], the one checked constructor, so no
/// `StagedPlacement` exists — decoded or minted — whose geometry the coder refuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "StagedPlacementWire")]
pub struct StagedPlacement {
    scheme: EcScheme,
    placement: Vec<DServerId>,
}

impl StagedPlacement {
    /// The checked constructor — for the writer that stages an owned entry (`write::intent`
    /// recording its `WritePlan` placement, `0016:459-473`; #656–#659), which sits outside this
    /// module's decode path. It refuses exactly the geometry decode refuses.
    pub fn new(scheme: EcScheme, placement: Vec<DServerId>) -> Result<Self, RecordError> {
        checked_staged_scheme(scheme)?;
        Ok(Self { scheme, placement })
    }

    /// How the chunk is fragmented — already `erasure::supported` (see this type's doc).
    pub const fn scheme(&self) -> EcScheme {
        self.scheme
    }

    /// The D server planned to hold each fragment, by fragment index. Its **length** is not a
    /// decode-time invariant (see this type's doc).
    pub fn placement(&self) -> &[DServerId] {
        &self.placement
    }
}

/// Refuse a staged scheme [`crate::erasure`] cannot encode/decode — the peer of
/// [`checked_chunk_scheme`] for *planned* geometry, on the same predicate
/// ([`crate::erasure::supported`]), attributed to its own variant because the value it judges
/// carries no chunk id. `EcScheme::None` has no `(k, m)` pair to check and is always valid.
fn checked_staged_scheme(scheme: EcScheme) -> Result<(), RecordError> {
    if let EcScheme::ReedSolomon { k, m } = scheme {
        if !erasure::supported(k as usize, m as usize) {
            return Err(RecordError::StagedSchemeUnsupported { k, m });
        }
    }
    Ok(())
}

impl TryFrom<StagedPlacementWire> for StagedPlacement {
    type Error = RecordError;

    fn try_from(wire: StagedPlacementWire) -> Result<Self, RecordError> {
        Self::new(wire.scheme.into(), wire.placement)
    }
}

/// The wire shape of a `sidx:` value — [`crate::metadata::PendingEntry`]'s own three fields, read
/// here so [`decode_owned_entry`] can attribute each rule to its own [`RecordError`] variant
/// rather than to the message a `try_from` conversion funnels through
/// [`crate::metadata::decode`] (the reason [`decode_admission_record`] reaches its own wire struct
/// too). Only the *shape* is mirrored: the pairing rule is `checked_ownership_pairing` and the
/// geometry rule is [`StagedPlacement`]'s conversion, both shared with the record's own
/// `Deserialize`.
///
/// **Closed**, as every wire shape in this module is, where the shared record's own wire stays
/// open: an owned entry has no stored corpus to stay compatible with (its first writer is
/// #656–#659), so a field this build does not know is a decode error here rather than a field a
/// renewal's re-encode would silently drop.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedEntryWire {
    lease_expiry_millis: u64,
    #[serde(default)]
    owner: Option<UploadId>,
    #[serde(default)]
    staged: Option<StagedPlacementWire>,
}

/// The value under a `sidx:<upload-id>:<part-number>:<chunk-id>` key (`0016:353`, `:442-457`),
/// with its two ownership fields present **by type** rather than by convention.
///
/// 0016 makes that value *be* a [`crate::metadata::PendingEntry`] rather than a parallel type, so
/// one renewal loop and one set of lease guards serve an owned entry and an ordinary lease alike.
/// On the shared record the two fields are therefore `Option`s; this is the view in which they are
/// not. [`decode_owned_entry`] returns it, and a writer mints the stored value from it:
/// [`Self::new`] takes both components, so the record [`Self::to_pending`] hands back can never be
/// torn.
///
/// # Minting one outside this crate
///
/// The first `sidx:` writer (#656–#659) lives in another crate, and the shared record's fields are
/// public — every in-tree `pending:` writer builds it as a literal. A writer left to hand-assemble
/// an owned literal could encode a torn value: bytes both decoders refuse, an entry its own reaper
/// could never read back. So the checked path for the **value** is public — [`StagedPlacement::new`]
/// (geometry), [`Self::new`] (both fields, by type), [`Self::to_pending`] (the stored form) — and
/// [`Self::from_pending`] validates a record that did not come from it. The **key** stays the
/// writer's: the one relation no value-side check can see, the owner against the key's upload id,
/// is [`decode_owned_entry`]'s, so a writer files the entry under the [`sidx_key`] it builds from
/// [`Self::owner`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedEntry {
    owner: UploadId,
    lease_expiry_millis: u64,
    staged: StagedPlacement,
}

impl OwnedEntry {
    /// The owned entry a staging write puts under `sidx:` for one chunk. Total: both components
    /// are already validated types, and the entry's one relation — its owner against the key
    /// naming it — is [`decode_owned_entry`]'s, the only party holding that key.
    pub const fn new(owner: UploadId, lease_expiry_millis: u64, staged: StagedPlacement) -> Self {
        Self {
            owner,
            lease_expiry_millis,
            staged,
        }
    }

    /// The owning session.
    pub const fn owner(&self) -> &UploadId {
        &self.owner
    }

    /// When the lease expires (logical milliseconds).
    pub const fn lease_expiry_millis(&self) -> u64 {
        self.lease_expiry_millis
    }

    /// The chunk's planned EC placement.
    pub const fn staged(&self) -> &StagedPlacement {
        &self.staged
    }

    /// The shared-record form this entry is **stored** as — the value a `sidx:` put writes, with
    /// both ownership fields present.
    pub fn to_pending(&self) -> metadata::PendingEntry {
        metadata::PendingEntry {
            lease_expiry_millis: self.lease_expiry_millis,
            owner: Some(self.owner.clone()),
            staged: Some(self.staged.clone()),
        }
    }

    /// Validate a shared record that did **not** come from [`Self::to_pending`] — one a caller
    /// assembled by hand — as an owned entry: the pairing rule first (a torn record is
    /// [`RecordError::TornOwnedEntry`]), then the shape an owned entry must have (an ordinary
    /// lease is [`RecordError::PendingEntryNamespaceMismatch`], never a silently accepted value).
    ///
    /// It holds no key, so it cannot check the one relation [`decode_owned_entry`] can — the
    /// owner against the key's upload id.
    pub fn from_pending(entry: &metadata::PendingEntry) -> Result<Self, RecordError> {
        checked_ownership_pairing(entry.owner.is_some(), entry.staged.is_some())?;
        match (&entry.owner, &entry.staged) {
            (Some(owner), Some(staged)) => Ok(Self::new(
                owner.clone(),
                entry.lease_expiry_millis,
                staged.clone(),
            )),
            _ => Err(RecordError::PendingEntryNamespaceMismatch {
                namespace: "sidx:",
                shape: ORDINARY_SHAPE,
            }),
        }
    }
}

/// Decode an owned staging entry from **both halves of the record** — its key ([`sidx_key`]) and
/// its value — the `sidx:` namespace's one decode entry point. Returns the part attempt and chunk
/// the key names beside the entry the value carries.
///
/// The key is taken because two of this record's rules are relations against it, which a decode
/// that cannot see the key cannot check (ADR-0045 decision 1):
///
/// 1. **namespace** — a `sidx:` value is an owned entry. An ordinary lease found here (neither
///    ownership field) is [`RecordError::PendingEntryNamespaceMismatch`]: the mirror of the
///    `pending:` entry point refusing an owned one ([`crate::metadata::decode_pending_entry`]);
/// 2. **owner** — the value's `owner` is the upload id the key names
///    ([`RecordError::OwnedEntryOwnerMismatch`]).
///
/// It hands back the key's `(part_number, chunk)` for the reason [`decode_retire_obligation`]
/// hands back its token: the pass that reads this record needs them — the part number attributes
/// residue to the part attempt that staged it, the chunk id is half of the
/// `orphan:<dserver>:<chunk>:<index>` keys the `staged` placement completes (`0016:353`) — and
/// re-parsing the key would be a second decision site that could disagree with this one. The
/// upload id is not among them: it is [`OwnedEntry::owner`], which this decode has just proved
/// equal to the key's.
///
/// The value's own rules run first — the pairing (`checked_ownership_pairing`) and the staged
/// geometry ([`StagedPlacement`]) — so a torn value is attributed to the rule it broke rather than
/// to the key it happens to sit under, the order [`decode_session_record`] and
/// [`decode_retire_obligation`] apply. It closes with the canonical-bytes gate every decoder in
/// this module closes with (`require_canonical`): a lease renewal preconditions on the **raw bytes
/// it read** and puts a freshly encoded entry (`crate::metadata::renew_pending`'s shape), so a
/// foreign spelling of an equal value would be bytes such a renewal silently rewrites — more than
/// the lease it came to extend — rather than a value it could renew in place.
pub fn decode_owned_entry(
    key: &[u8],
    value: &[u8],
) -> Result<(PartNumber, ChunkId, OwnedEntry), RecordError> {
    let (key_owner, part_number, chunk) = parse_sidx_key(key)?;
    let wire: OwnedEntryWire =
        metadata::decode(value).map_err(|err| RecordError::MalformedRecordValue {
            namespace: "sidx:",
            detail: err.to_string(),
        })?;
    checked_ownership_pairing(wire.owner.is_some(), wire.staged.is_some())?;
    let (Some(owner), Some(staged)) = (wire.owner, wire.staged) else {
        return Err(RecordError::PendingEntryNamespaceMismatch {
            namespace: "sidx:",
            shape: ORDINARY_SHAPE,
        });
    };
    let staged = StagedPlacement::try_from(staged)?;
    if owner != key_owner {
        return Err(RecordError::OwnedEntryOwnerMismatch {
            key_owner,
            entry_owner: owner,
        });
    }
    let entry = OwnedEntry::new(owner, wire.lease_expiry_millis, staged);
    require_canonical(entry.to_pending(), value, "sidx:")?;
    Ok((part_number, chunk, entry))
}

// ===========================================================================
// 11. The multipart ETag and the Complete request identity (`0016:894-1037`; the
//     composition ADR-0047 deferred, `0016:3064-3070`, ADR-0047:73-89, `:112`)
// ===========================================================================

/// The named-part list a Complete sent, validated as the **one** order both digests below are
/// defined over: at least one part, strictly ascending part numbers, none named twice.
///
/// It **refuses** any other list ([`RecordError::NoPartsNamed`],
/// [`RecordError::PartsOutOfOrder`], [`RecordError::DuplicatePart`]) and never sorts it:
/// ascending part numbers are a Complete *validation* (`0016:707`, `:994`), so the order a
/// valid request names is already the canonical one, and a sort would compose — and
/// fingerprint — an assembly in an order the client never sent. It hands the same slice back,
/// so "the parts the client named" means one thing to both digests.
fn canonical_named_parts(
    named: &[(PartNumber, Digest)],
) -> Result<&[(PartNumber, Digest)], RecordError> {
    if named.is_empty() {
        return Err(RecordError::NoPartsNamed);
    }
    for ((previous, _), (current, _)) in named.iter().zip(named.iter().skip(1)) {
        let (part_number, previous) = (current.get(), previous.get());
        match part_number.cmp(&previous) {
            Ordering::Greater => {}
            Ordering::Equal => return Err(RecordError::DuplicatePart { part_number }),
            Ordering::Less => {
                return Err(RecordError::PartsOutOfOrder {
                    part_number,
                    previous,
                })
            }
        }
    }
    Ok(named)
}

/// A composed **multipart ETag**, `<64 lowercase hex>-<N>`: the value [`multipart_etag`]
/// computes, a [`Completion`] records, and an identical retry is answered with.
///
/// A validated type rather than a `String`: the only ways to obtain one are to compose it
/// ([`multipart_etag`]) or to parse its one canonical spelling ([`MultipartEtag::parse`]), so
/// `N` is always in `[1, MAX_PART_NUMBER]` and the text form round-trips byte for byte. A
/// second spelling of one ETag would be a second identity for one object, and a record
/// carrying it would be one that a whole-record CAS on its exact bytes (`0016:555-558`) could
/// never match.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MultipartEtag {
    composed: Digest,
    parts: u32,
}

impl MultipartEtag {
    /// The validating parser, the exact inverse of [`fmt::Display`]: 64 lowercase-hex
    /// characters, one `-`, then the count `N` in **canonical** decimal (no sign, no leading
    /// zero) within `[1, MAX_PART_NUMBER]`.
    ///
    /// Each rule has its own error: a hex half that is not a digest is
    /// [`RecordError::DigestNotHex`]; a missing separator or a non-canonical count is
    /// [`RecordError::MultipartEtagMalformed`]; and a canonical count outside the range — zero,
    /// one past [`MAX_PART_NUMBER`], or one wider than any integer type — is
    /// [`RecordError::EtagPartCountOutOfRange`], never "malformed".
    pub fn parse(text: &str) -> Result<Self, RecordError> {
        let malformed = || RecordError::MultipartEtagMalformed {
            etag: text.to_string(),
        };
        let (hex, count) = text.split_once('-').ok_or_else(malformed)?;
        let composed = Digest::from_hex(hex)?;
        if !is_canonical_decimal(count) {
            return Err(malformed());
        }
        // The count is canonical, so a failed `u32` parse can only be an overflow — a count
        // past `u32::MAX`, hence past `MAX_PART_NUMBER` — and it gets the same range error.
        let parts = count
            .parse::<u32>()
            .ok()
            .filter(|parts| (1..=MAX_PART_NUMBER).contains(parts))
            .ok_or_else(|| RecordError::EtagPartCountOutOfRange {
                count: count.to_string(),
            })?;
        Ok(Self { composed, parts })
    }

    /// The SHA-256 over the parts' raw digests in part order — the hex half.
    pub const fn composed(&self) -> Digest {
        self.composed
    }

    /// `N`: how many parts the object was assembled from — the `-N` suffix.
    pub const fn parts(&self) -> u32 {
        self.parts
    }
}

impl fmt::Display for MultipartEtag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.composed, self.parts)
    }
}

impl Serialize for MultipartEtag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MultipartEtag {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::parse(&String::deserialize(deserializer)?).map_err(DeError::custom)
    }
}

/// The **multipart ETag** — the composition this module settles on ADR-0047's basis, which
/// closed the digest algorithm and deferred only the composition to the multipart slice
/// (`docs/design/adr/0047-object-metadata-model.md:73-89`, `:112`; `0016:3064-3070`):
///
/// ```text
/// etag = lowercase_hex( SHA-256( d_1 || d_2 || ... || d_N ) ) + "-" + N
/// ```
///
/// `d_i` is the **raw 32-byte** digest of the *i*-th part the Complete named — never its hex
/// text, with no separator and no part number mixed in — in the list's own order, which
/// `canonical_named_parts` requires to be strictly ascending; `N` is how many parts the list
/// names, not the highest part number. The value is therefore a pure function of the recorded
/// part digests and their order, so a retry naming the same parts re-derives the same ETag —
/// the one property the commit protocol needs from it (`0016:3066-3069`).
///
/// **Never MD5.** S3's own multipart ETag is an MD5 of MD5s; ADR-0047 rejected re-opening the
/// dependency wall for a legacy equality S3 itself does not guarantee, and clients compare the
/// value only for equality.
///
/// A list that is empty, not strictly ascending, or names a part twice is a typed error —
/// never an ETag over a sorted or de-duplicated copy of it.
pub fn multipart_etag(named: &[(PartNumber, Digest)]) -> Result<MultipartEtag, RecordError> {
    let named = canonical_named_parts(named)?;
    let composed = Digest::sha256(|hasher| {
        for (_, digest) in named {
            hasher.update(digest.as_bytes());
        }
    });
    let parts = u32::try_from(named.len())
        .expect("a strictly ascending list of part numbers is at most MAX_PART_NUMBER long");
    Ok(MultipartEtag { composed, parts })
}

/// The **request identity** a `Completed` tombstone answers a retry on (`0016:898-908`): a
/// SHA-256 over the `(part_number, digest)` pairs the Complete named, in its order —
///
/// ```text
/// fingerprint = SHA-256( be32(n_1) || d_1 || be32(n_2) || d_2 || ... || be32(n_N) || d_N )
/// ```
///
/// with `be32(n_i)` the part number as 4 big-endian bytes and `d_i` its raw 32-byte digest.
/// Every pair is the same 36 bytes wide, so two different lists never share a preimage.
///
/// Separate from [`multipart_etag`] on purpose: the ETag hashes the digests alone, so two
/// assemblies of the same bodies under **different part numbers** share an ETag. The
/// fingerprint mixes the numbers in, so a client reusing a consumed upload id with a
/// different list is answered `NoSuchUpload` — never told *its* assembly succeeded while the
/// store holds another one.
///
/// It refuses exactly the lists [`multipart_etag`] refuses, with the same errors (both go
/// through `canonical_named_parts`), so no identity is ever taken of a request that could not
/// have been published.
pub fn complete_fingerprint(named: &[(PartNumber, Digest)]) -> Result<Digest, RecordError> {
    let named = canonical_named_parts(named)?;
    Ok(Digest::sha256(|hasher| {
        for (part_number, digest) in named {
            hasher.update(part_number.get().to_be_bytes());
            hasher.update(digest.as_bytes());
        }
    }))
}

// ===========================================================================
// 12. Typed outcomes — every answer is a value, never "an error", and none names an HTTP
//     status (`0016:969-978`; the S3 status/XML mapping is #508's)
// ===========================================================================
//
// No enum in this section or the next is `#[non_exhaustive]`, deliberately. Every consumer is
// in this workspace (`publish = false`), and each protocol gateway maps these values onto its
// own wire answers: a new variant must break every such mapping at compile time, never fall
// into a `_ =>` arm that answers it with a silently wrong status.

/// Why a part a Complete named is invalid (`0016:993-999`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidPart {
    /// The named part has no `part:` record — never committed, or superseded.
    Absent,
    /// The named part's recorded digest is not the one the client named.
    DigestMismatch,
    /// The named part numbers are not strictly ascending — out of order, or one named twice
    /// (the protocol answer to [`RecordError::PartsOutOfOrder`] and
    /// [`RecordError::DuplicatePart`]).
    OutOfOrder,
}

/// Which ceiling produced a backpressure refusal — designed behaviour, not a failure
/// (`0016:130-136`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backpressure {
    /// The fleet admission ledger is at its stored `max_sessions`.
    SessionCap {
        /// The live session count the ledger records.
        count: u64,
        /// The limit **the ledger** was checked against — never a local derivation.
        max_sessions: u64,
    },
    /// Every `MAX_INFLIGHT_PARTS` slot index of this session is taken — by live parts, or by
    /// the residue of parts that crashed mid-stream and never released their slot
    /// (`0016:349`, F11a).
    InflightParts {
        /// The size of the session's slot key space.
        max_inflight_parts: u32,
    },
    /// The create lost the serialized admission CAS this many times.
    AdmissionContention {
        /// Attempts spent.
        attempts: u32,
    },
}

/// A **typed** protocol answer other than success. #508 maps each to its S3 status and error
/// code; this module pins the answers and names no status code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// No session can serve this request: the upload id is unknown, the session has left the
    /// states this verb acts on (which, per verb, is [`answer`]'s table, `0016:972-978`), or it
    /// already completed a **different** assembly.
    NoSuchUpload,
    /// The target bucket has no `bucket:` record (ADR-0046 §4).
    NoSuchBucket,
    /// The session is fenced into `Completing` by a publisher that still owns it. There is
    /// **no** client path that resumes a `Completing` session (`0016:980-986`).
    OperationAborted,
    /// The named-part list is invalid (`0016:993-999`): a named part is absent from the frozen
    /// part set or carries another digest, or the part numbers are not strictly ascending. The
    /// fence is **released** before this is answered, so a client typo never wedges a session.
    InvalidPart {
        /// The offending part number.
        part_number: PartNumber,
        /// What was wrong with it.
        reason: InvalidPart,
    },
    /// A part, or the session's cumulative staged chunks, is past a ceiling the protocol
    /// enforces by **refusal** rather than by an over-envelope commit. The session stays
    /// usable and abortable, and anything already staged is compensated.
    EntityTooLarge {
        /// The chunk count the request would have installed.
        chunks: u64,
        /// The ceiling it crossed.
        limit: u64,
    },
    /// Designed backpressure.
    SlowDown {
        /// Which ceiling engaged.
        pressure: Backpressure,
    },
    /// This process's [`Budget`] disagrees with the one stored in `mpuctl`, so it **refuses
    /// to admit and alarms** rather than silently deferring to either value (`0016:348`).
    ProfileSkew {
        /// What this process derives from its own configuration.
        local: Budget,
        /// What the ledger records.
        ledger: Budget,
    },
    /// The session has spent all `MAX_COMPLETE_ATTEMPTS` fences; its only exit is Abort
    /// (`0016:1477`).
    CompleteAttemptsExhausted,
}

/// What a publication established — a winning flip's, or the one a `Completed` tombstone
/// recorded and hands back to an identical retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publication {
    /// The inode the object key resolves to.
    pub inode: InodeId,
    /// The version the flip recorded — `prior.version + 1` computed from the **re-read**
    /// prior at that attempt, never a fence-frozen number (`0016:925-932`).
    pub version: u64,
    /// The published object's ETag, `-N` suffix included — what the client is answered.
    pub etag: MultipartEtag,
    /// When the flip landed (logical milliseconds).
    pub completed_at_millis: u64,
}

impl Publication {
    /// The publication a `Completed` tombstone recorded, field for field. The ETag is the
    /// recorded one, never recomputed: the part records it was composed from may already be
    /// retired (see [`Completion`]).
    pub fn of(completion: &Completion) -> Self {
        Self {
            inode: completion.inode,
            version: completion.version,
            etag: completion.etag,
            completed_at_millis: completion.completed_at_millis,
        }
    }
}

/// What `CreateMultipartUpload` answers (#656) — named here so #656 answers in this
/// vocabulary rather than inventing its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOutcome {
    /// A session record and its `seggrp:` reservation are durable and the admission counter
    /// has been incremented exactly once.
    Created {
        /// The session's id.
        upload_id: UploadId,
    },
    /// A typed refusal.
    Refused(Refusal),
}

/// What a slot reservation answers (#657).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReserveOutcome {
    /// An in-flight slot index is held by this attempt.
    Reserved {
        /// The index claimed.
        index: SlotIndex,
        /// The attempt holding it.
        attempt_id: AttemptId,
    },
    /// A typed refusal.
    Refused(Refusal),
}

/// What `UploadPart` answers once a part body has been staged and validated (#657) — the
/// body half of the verb; the *lifecycle* half (whether the session may accept a part at all)
/// is [`UploadPartAnswer`], decision 3's table cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadPartOutcome {
    /// The part is durable: its `part:` + `psum:` records exist, its slot is released and its
    /// owned `sidx:` entries are gone — all in one batch.
    Committed {
        /// The part number.
        part_number: PartNumber,
        /// Its content digest.
        digest: Digest,
    },
    /// A typed refusal. Anything this attempt staged has been **compensated** whenever the
    /// session was still live enough to own the cleanup.
    Refused(Refusal),
}

/// What `CompleteMultipartUpload` answers once its named-part list has been validated against
/// the frozen part set (#658) — the assembly half of the verb; the *lifecycle* half is
/// [`CompleteAnswer`], decision 3's table cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompleteOutcome {
    /// The object is published: this call's flip won.
    Published(Publication),
    /// A `Completed` tombstone answered an **identical** retry inside its window, with the
    /// **recorded** publication (`0016:898-908`).
    AlreadyCompleted(Publication),
    /// A typed refusal; nothing was published and the session is not left fenced.
    Refused(Refusal),
}

/// What `AbortMultipartUpload` answers (#656) — the body half of the verb; the *lifecycle*
/// half is [`AbortAnswer`], decision 3's table cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbortOutcome {
    /// The fence landed: the session is `Aborting` and its teardown obligation is durable.
    /// **This is the response** — byte reclamation is the drain's, asynchronously and in
    /// bounded batches, so a 10,000-part teardown never rides inside one request
    /// (`0016:1000-1003`).
    Fenced,
    /// The session was already `Aborting` — idempotent success.
    AlreadyAborting,
    /// A typed refusal.
    Refused(Refusal),
}

// ===========================================================================
// 13. Decision 3 — the verb × state answer table, as total pure functions
//     (`0016:894-1037`, the table at `0016:969-978`)
// ===========================================================================

/// The five multipart verbs decision 3 answers (`0016:969-978`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Verb {
    /// Stage one part's bytes against an `Open` session.
    UploadPart,
    /// Fence and assemble the named parts into the published object.
    CompleteMultipartUpload,
    /// Fence for teardown without publishing.
    AbortMultipartUpload,
    /// List the parts staged (or frozen) under one session.
    ListParts,
    /// List the fleet's in-progress sessions.
    ListMultipartUploads,
}

impl Verb {
    /// Every verb the table answers, in its row order, so a caller or test can enumerate the
    /// product without re-deriving it. The compiler does not check this list; it checks
    /// [`answer`], whose match is exhaustive, so a new verb cannot compile until it is answered
    /// there — and the one answering it there adds it here.
    pub const ALL: [Self; 5] = [
        Self::UploadPart,
        Self::CompleteMultipartUpload,
        Self::AbortMultipartUpload,
        Self::ListParts,
        Self::ListMultipartUploads,
    ];
}

/// What `UploadPart` answers in a state (`0016:974`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadPartAnswer {
    /// The session is `Open`: the part is accepted.
    Accepted,
    /// A typed refusal.
    Refused(Refusal),
}

/// What `CompleteMultipartUpload` answers in a state (`0016:975`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompleteAnswer {
    /// The session is `Open`: this call may fence and publish.
    Fences,
    /// A `Completed` tombstone whose recorded `complete_fingerprint` **matches** the
    /// request's: the identical retry is answered with the **recorded** publication — its
    /// ETag exactly as the original Complete answered it (`0016:898-908`).
    AlreadyCompleted(Publication),
    /// A typed refusal.
    Refused(Refusal),
}

/// What `AbortMultipartUpload` answers in a state (`0016:976`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbortAnswer {
    /// The session is `Open`: the fence commit **is** the response.
    Fences,
    /// The session is already `Aborting`: idempotent success.
    AlreadyAborting,
    /// A typed refusal.
    Refused(Refusal),
}

/// What `ListParts` answers in a state (`0016:977`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListPartsAnswer {
    /// The session is `Open`: the still-growing part set.
    OpenSet,
    /// The session is `Completing`: the frozen part set the fence read.
    FrozenSet,
    /// A typed refusal.
    Refused(Refusal),
}

/// Whether `ListMultipartUploads` lists a session in a state (`0016:978`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListUploadsAnswer {
    /// In progress (`Open` or `Completing`): listed.
    Listed,
    /// Not in progress (`Aborting`, `Completed`, or no record): not listed.
    NotListed,
}

/// One cell of decision 3's table, so the whole verb × state product can be enumerated
/// through a single entry point ([`answer`]) and asserted **total**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// `UploadPart`'s answer.
    UploadPart(UploadPartAnswer),
    /// `CompleteMultipartUpload`'s answer.
    Complete(CompleteAnswer),
    /// `AbortMultipartUpload`'s answer.
    Abort(AbortAnswer),
    /// `ListParts`' answer.
    ListParts(ListPartsAnswer),
    /// `ListMultipartUploads`'s answer.
    ListUploads(ListUploadsAnswer),
}

/// What `UploadPart` answers against `state` (`None` = the record is absent).
///
/// A part accepted after the fence would be invisible to the publication that already read
/// the set — a silently lost part — so every state but `Open` refuses (`0016:1030`).
pub fn upload_part_answer(state: Option<&SessionState>) -> UploadPartAnswer {
    match state {
        Some(SessionState::Open {}) => UploadPartAnswer::Accepted,
        Some(
            SessionState::Completing { .. }
            | SessionState::Aborting {}
            | SessionState::Completed { .. },
        )
        | None => UploadPartAnswer::Refused(Refusal::NoSuchUpload),
    }
}

/// What `CompleteMultipartUpload` answers against `state` (`None` = the record is absent),
/// for a request whose named parts fingerprint to `request_fingerprint` — `None` when the
/// request has no valid named-part list to fingerprint ([`complete_fingerprint`] refused it),
/// which can match no recorded fingerprint.
///
/// The tombstone cell is the one 0016 spends most words on, and the **only** cell with a
/// condition: within its window a `Completed` session answers the recorded publication when
/// the request's fingerprint matches the recorded one, and `NoSuchUpload` otherwise — the
/// honest answer, since the upload id is consumed and no session exists that could publish
/// the assembly being asked for (`0016:898-908`).
///
/// **The one function [`answer`] delegates to for this verb**, never a second inline copy of
/// the same table: a duplicate would let a fix to one copy leave the other's cell silently
/// wrong.
pub fn complete_answer(
    state: Option<&SessionState>,
    request_fingerprint: Option<&Digest>,
) -> CompleteAnswer {
    match state {
        Some(SessionState::Open {}) => CompleteAnswer::Fences,
        Some(SessionState::Completing { .. }) => CompleteAnswer::Refused(Refusal::OperationAborted),
        Some(SessionState::Completed { completion })
            if request_fingerprint == Some(&completion.complete_fingerprint) =>
        {
            CompleteAnswer::AlreadyCompleted(Publication::of(completion))
        }
        Some(SessionState::Aborting {} | SessionState::Completed { .. }) | None => {
            CompleteAnswer::Refused(Refusal::NoSuchUpload)
        }
    }
}

/// What `AbortMultipartUpload` answers against `state` (`None` = the record is absent).
///
/// A `Completing` session refuses an Abort with `OperationAborted`, as it does a Complete: an
/// Abort that could preempt a live publisher would race the flip it is fencing against
/// (`0016:976`, `:980-986`).
pub fn abort_answer(state: Option<&SessionState>) -> AbortAnswer {
    match state {
        Some(SessionState::Open {}) => AbortAnswer::Fences,
        Some(SessionState::Completing { .. }) => AbortAnswer::Refused(Refusal::OperationAborted),
        Some(SessionState::Aborting {}) => AbortAnswer::AlreadyAborting,
        Some(SessionState::Completed { .. }) | None => AbortAnswer::Refused(Refusal::NoSuchUpload),
    }
}

/// What `ListParts` answers against `state` (`None` = the record is absent).
pub fn list_parts_answer(state: Option<&SessionState>) -> ListPartsAnswer {
    match state {
        Some(SessionState::Open {}) => ListPartsAnswer::OpenSet,
        Some(SessionState::Completing { .. }) => ListPartsAnswer::FrozenSet,
        Some(SessionState::Aborting {} | SessionState::Completed { .. }) | None => {
            ListPartsAnswer::Refused(Refusal::NoSuchUpload)
        }
    }
}

/// Whether `ListMultipartUploads` lists a session in `state` (`None` = the record is
/// absent). A session the reaper fenced at `W_session` is `Aborting`, so it drops out of the
/// listing — the S3-visible signal that the upload expired (`0016:988-991`).
pub fn list_uploads_answer(state: Option<&SessionState>) -> ListUploadsAnswer {
    match state {
        Some(SessionState::Open {} | SessionState::Completing { .. }) => ListUploadsAnswer::Listed,
        Some(SessionState::Aborting {} | SessionState::Completed { .. }) | None => {
            ListUploadsAnswer::NotListed
        }
    }
}

/// Decision 3's answer table (`0016:969-978`) as **one total pure function** over the 5-verb
/// × 5-state product — `Open` / `Completing` / `Aborting` / `Completed` / absent.
///
/// `request_fingerprint` is consulted **only** for the one conditional cell
/// (`CompleteMultipartUpload` against a `Completed` tombstone) and ignored everywhere else, so
/// the function can be called uniformly over the whole product.
///
/// Every match below and in the per-verb functions is exhaustive, with no wildcard arm, so
/// answering a new verb or state is a compile-time obligation rather than a silent gap.
pub fn answer(
    verb: Verb,
    state: Option<&SessionState>,
    request_fingerprint: Option<&Digest>,
) -> Answer {
    match verb {
        Verb::UploadPart => Answer::UploadPart(upload_part_answer(state)),
        Verb::CompleteMultipartUpload => {
            Answer::Complete(complete_answer(state, request_fingerprint))
        }
        Verb::AbortMultipartUpload => Answer::Abort(abort_answer(state)),
        Verb::ListParts => Answer::ListParts(list_parts_answer(state)),
        Verb::ListMultipartUploads => Answer::ListUploads(list_uploads_answer(state)),
    }
}
