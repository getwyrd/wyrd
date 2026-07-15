//! The shared **reconstruction / repair queue** — the durable seam a corruption
//! finding lands on, whether discovered **proactively by scrub** (the custodian's
//! scrub loop) or **reactively on read** (the client read path's read-time checksum
//! verification). Proposal 0005 §Scrub (`0005:262-267`) and the read-time-failure
//! feed (`0005:174-176`) require **one shared queue** that both producers enqueue
//! onto and the reconstruction loop (slice 6, `0005:531-536`) later drains.
//!
//! The queue lives in `core` rather than `custodian` because the read path is in
//! `core` and `core` must not depend on `custodian` (the dependency rule runs
//! `custodian → core`, ADR-0010 `0005:421-422`). Placing the key + the enqueue +
//! the verify here makes "the same queue" true **by construction**: scrub
//! (`custodian`, which depends on `core`) and the read path call the *same*
//! [`enqueue_repair`] against the *same* [`repair_key`].
//!
//! This slice only **produces** repair obligations; dequeuing and rebuilding is the
//! reconstruction custodian (slice 6, out of scope here). The concrete ledger
//! representation (key/encoding) is ILLUSTRATIVE; the shared-queue feed is BINDING.

use wyrd_chunk_format::{EcSchemeType, FragmentHeader};
use wyrd_traits::{ChunkId, FragmentId, MetadataStore, Result, WriteBatch};

use crate::metadata::EcScheme;

/// Key prefix for the **repair queue** ledger — the chunks a corruption finding has
/// flagged for reconstruction. Mirrors the `pending:` / `orphan:` ledger pattern
/// (architecture §5).
pub const REPAIR_PREFIX: &[u8] = b"repair:";

/// Key for a repair-queue entry: `repair:<chunk_id>`. Keyed by chunk so a repair
/// obligation is a **set** — enqueuing the same chunk twice (scrub and a read both
/// catching it) collapses to one obligation, never a duplicate rebuild.
pub fn repair_key(chunk: ChunkId) -> Vec<u8> {
    format!("repair:{chunk}").into_bytes()
}

/// Parse a repair-queue key back to the chunk id it enqueues.
pub fn parse_repair_key(key: &[u8]) -> Option<ChunkId> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("repair:")?
        .parse()
        .ok()
}

/// Whether a decoded fragment `header` proves the **FULL identity** a read/repair/
/// maintenance path requested against the committed chunk map: it names the expected
/// `chunk`, sits at the expected `ec_fragment_index`, and (for Reed-Solomon) carries an
/// EC tuple (`ec_scheme_type`/`ec_k`/`ec_m`) consistent with the committed `scheme`.
///
/// Verification is "against the chunk map", not against half of it (`0005:262-267`): a
/// valid same-chunk shard for the WRONG index — or a fragment whose header EC tuple
/// disagrees with the committed scheme — is a misplaced / misencoded fragment and must
/// be rejected before it is ever fed to the decoder under the requested index. The
/// store-level precedent is `FsChunkStore::verify` (chunk **and** index,
/// `crates/chunkstore-fs/src/lib.rs:117-130`); this widens it with the committed EC
/// tuple only the shared core layer can check, so the never-wrong-bytes assurance holds
/// for ANY backend rather than being delegated to backend goodwill.
pub fn header_matches_identity(
    header: &FragmentHeader,
    expected: FragmentId,
    scheme: EcScheme,
) -> bool {
    if header.chunk_id != expected.chunk || header.ec_fragment_index != expected.index {
        return false;
    }
    match scheme {
        // A single-copy (`none`) chunk: one fragment at the expected index, no erasure
        // coding. TWO v1 code points legitimately stamp single-copy data: `none` (what
        // this repo's writer stamps, `write.rs:133`) and `replication` — the format spec
        // defines both with `ec_k = 1, ec_m = 0` (chunk-format v1 §header, and the
        // `replication-fragment` conformance vector), the CLI already parses
        // `replication(1)` as `EcScheme::None` (`cli.rs:378-380`), and the spec's
        // mixed-era rule says a reader MUST honour the scheme the fragment records.
        // Rejecting `replication` here would make spec-valid single-copy data unreadable
        // and have scrub enqueue it as corrupt forever (Codex, PR #564).
        EcScheme::None => {
            matches!(
                header.ec_scheme_type,
                EcSchemeType::None | EcSchemeType::Replication
            ) && header.ec_k == 1
                && header.ec_m == 0
        }
        // A Reed-Solomon chunk: the header must name the SAME `k`/`m` stripe geometry
        // the committed `ChunkRef.scheme` records, so a shard from a differently-coded
        // chunk (same id, incompatible stripe) is never mistaken for a survivor.
        EcScheme::ReedSolomon { k, m } => {
            header.ec_scheme_type == EcSchemeType::ReedSolomon
                && header.ec_k == k
                && header.ec_m == m
        }
    }
}

/// Verify a stored fragment's **self-describing checksum** and that it is the fragment
/// the committed chunk map expects at `expected` under `scheme`. Returns `true` only
/// when the bytes decode cleanly (header + payload crc32c verified by
/// [`wyrd_chunk_format::decode`]) **and** the decoded header proves the FULL identity
/// ([`header_matches_identity`] — chunk id, `ec_fragment_index`, and the RS EC tuple).
/// A `false` is bit rot / a misplaced / misencoded fragment: it must be **excluded**
/// from the decoder and its `expected.chunk` enqueued for reconstruction
/// ([`enqueue_repair`]) — the load-bearing invariant (`0005:262-267`, `0005:174-176`).
///
/// This is the one verify both producers share: the read path decodes for the same
/// effect inline (`crates/core/src/read.rs`, via [`header_matches_identity`]), and
/// scrub calls this against each referenced fragment it walks.
pub fn fragment_intact(bytes: &[u8], expected: FragmentId, scheme: EcScheme) -> bool {
    matches!(
        wyrd_chunk_format::decode(bytes),
        Ok(decoded) if header_matches_identity(&decoded.header, expected, scheme)
    )
}

/// Decode a survivor fragment to its **shard payload** iff it is intact and is the
/// fragment the committed chunk map expects at `expected` under `scheme` — the gather
/// step of the **reconstruction** custodian (`0005:275`). Returns `None` for a missing,
/// checksum-failing, misplaced, or misencoded fragment (full-identity check via
/// [`header_matches_identity`]), which is then **excluded** from the decoder (never fed
/// to it) and rebuilt around.
///
/// This is [`fragment_intact`]'s payload-returning sibling: it lives here (with the
/// shared verify) so the reconstruction loop in `custodian` recovers a survivor's bytes
/// **without** depending on the on-disk fragment format directly (ADR-0010,
/// `0005:421-422`) — `core` owns the format reader.
pub fn intact_shard(bytes: &[u8], expected: FragmentId, scheme: EcScheme) -> Option<Vec<u8>> {
    match wyrd_chunk_format::decode(bytes) {
        Ok(decoded) if header_matches_identity(&decoded.header, expected, scheme) => {
            Some(decoded.payload)
        }
        _ => None,
    }
}

/// Enqueue `chunk` for reconstruction onto the shared, durable repair queue.
/// **Idempotent** — a chunk already queued stays a single obligation (the key
/// dedups). `detected_by` records the producer (`"scrub"` | `"read"`) for the
/// durability-plane audit trail (`0005:336-340`); the reconstruction loop reads only
/// the key set.
pub async fn enqueue_repair(
    meta: &dyn MetadataStore,
    chunk: ChunkId,
    detected_by: &str,
) -> Result<()> {
    meta.commit(WriteBatch::new().put(repair_key(chunk), detected_by.as_bytes().to_vec()))
        .await?;
    Ok(())
}

/// Every chunk currently enqueued on the repair queue. The reconstruction loop's
/// future entry point; here it is the in-process read-back that proves both
/// producers feed the **same** queue.
pub async fn queued_repairs(meta: &dyn MetadataStore) -> Result<Vec<ChunkId>> {
    let mut chunks = Vec::new();
    for (key, _value) in meta.scan(REPAIR_PREFIX).await? {
        if let Some(chunk) = parse_repair_key(&key) {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wyrd_chunk_format::{encode, EcSchemeType, FragmentHeader};

    /// `intact_shard` decodes a survivor's payload ONLY when the fragment's header
    /// proves the FULL identity the chunk map expects: chunk id, `ec_fragment_index`,
    /// and the EC tuple. A checksum-valid fragment of the SAME chunk at the WRONG index,
    /// or whose header EC tuple disagrees with the committed scheme, is a misplaced /
    /// misencoded shard and must be rejected (`None`), never fed to the decoder
    /// (`0005:262-267`).
    #[test]
    fn intact_shard_accepts_the_expected_fragment_and_rejects_wrong_identity() {
        let chunk: ChunkId = 0xABCD;
        let payload = b"a survivor shard's bytes";
        // A `none`-scheme fragment at index 0 (what `new_v1` stamps).
        let bytes = encode(
            &FragmentHeader::new_v1(chunk, payload.len() as u64),
            payload,
        );
        let at0 = FragmentId { chunk, index: 0 };

        assert_eq!(
            intact_shard(&bytes, at0, EcScheme::None).as_deref(),
            Some(payload.as_slice()),
            "a fragment whose header proves the full requested identity yields its payload"
        );
        assert_eq!(
            intact_shard(
                &bytes,
                FragmentId {
                    chunk: 0x9999,
                    index: 0
                },
                EcScheme::None
            ),
            None,
            "the same bytes against a DIFFERENT chunk id are a misplaced fragment: rejected"
        );
        assert_eq!(
            intact_shard(&bytes, FragmentId { chunk, index: 1 }, EcScheme::None),
            None,
            "a valid same-chunk fragment at the WRONG ec_fragment_index is rejected"
        );
        assert_eq!(
            intact_shard(&bytes, at0, EcScheme::ReedSolomon { k: 2, m: 1 }),
            None,
            "a header EC tuple (none/1/0) that disagrees with the committed RS scheme is rejected"
        );

        // Same SCHEME TYPE (ReedSolomon), correct chunk id and index, but a DIFFERENT
        // stripe geometry: an RS(3,1) header against a committed RS(2,1) chunk. This pins
        // the `ec_k`/`ec_m` conjuncts specifically — the `ec_scheme_type` match alone
        // passes here, so only the k/m compare can reject it. A shard from a differently-
        // coded chunk (same id, incompatible stripe) is never mistaken for a survivor.
        let mut rs31 = FragmentHeader::new_v1(chunk, payload.len() as u64);
        rs31.ec_scheme_type = EcSchemeType::ReedSolomon;
        rs31.ec_k = 3;
        rs31.ec_m = 1;
        rs31.ec_fragment_index = 0;
        let rs31_bytes = encode(&rs31, payload);
        assert_eq!(
            intact_shard(&rs31_bytes, at0, EcScheme::ReedSolomon { k: 2, m: 1 }),
            None,
            "an RS header with the WRONG stripe geometry (k/m) — same scheme type — is rejected"
        );
        assert_eq!(
            intact_shard(&rs31_bytes, at0, EcScheme::ReedSolomon { k: 3, m: 1 }).as_deref(),
            Some(payload.as_slice()),
            "the MATCHING RS(3,1) geometry admits the same fragment — proving the k/m \
             compare, not the scheme type alone, is what gates it"
        );
    }

    /// The v1 format defines TWO single-copy code points — `none` (0) and `replication`
    /// (1), both with `ec_k = 1, ec_m = 0` (chunk-format spec v1 §header; the
    /// `replication-fragment` conformance vector) — and the CLI parses `replication(1)`
    /// as `EcScheme::None`. A spec-conforming replication(1) fragment whose chunk id and
    /// index match MUST be admitted under a committed `EcScheme::None`, exactly like a
    /// `none`-stamped one; a replication header with a non-single-copy tuple must not.
    #[test]
    fn replication_single_copy_fragment_is_admitted_under_scheme_none() {
        let chunk: ChunkId = 0xABCD;
        let payload = b"a single copy stamped with the replication code point";
        let mut header = FragmentHeader::new_v1(chunk, payload.len() as u64);
        header.ec_scheme_type = EcSchemeType::Replication;
        header.ec_k = 1;
        header.ec_m = 0;
        let bytes = encode(&header, payload);
        let at0 = FragmentId { chunk, index: 0 };

        assert_eq!(
            intact_shard(&bytes, at0, EcScheme::None).as_deref(),
            Some(payload.as_slice()),
            "a replication(1) header (k=1, m=0) proves single-copy identity under \
             EcScheme::None — the mixed-era/interoperable code point stays readable"
        );
        assert!(
            fragment_intact(&bytes, at0, EcScheme::None),
            "the shared verify admits the same replication(1) fragment"
        );

        // A replication header whose tuple is NOT single-copy stays rejected: the
        // scheme-type widening admits exactly k=1/m=0, nothing else.
        let mut multi = FragmentHeader::new_v1(chunk, payload.len() as u64);
        multi.ec_scheme_type = EcSchemeType::Replication;
        multi.ec_k = 1;
        multi.ec_m = 2;
        let multi_bytes = encode(&multi, payload);
        assert_eq!(
            intact_shard(&multi_bytes, at0, EcScheme::None),
            None,
            "a replication header with a non-single-copy tuple (m != 0) is rejected"
        );
    }
}
