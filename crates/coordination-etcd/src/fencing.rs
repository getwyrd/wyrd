//! Pure fencing-token derivation. No etcd dependency, so it is unit-tested on
//! every build.
//!
//! etcd's mvcc **revision** is a single cluster-global counter, bumped on every
//! write across the whole keyspace. So the revision at which a leader's candidate
//! key was created, and the revision a lock's acquiring txn committed at, are both
//! drawn from that one rising source — any later grant fences any earlier one,
//! regardless of which kind it is (the [`FencingToken`] contract; the custodian's
//! single-active guard depends on it, M3.3/#141).

use wyrd_traits::FencingToken;

/// Map an etcd revision to a [`FencingToken`]. A real commit revision is always a
/// positive `i64`; a non-positive value (spec-impossible, e.g. an empty header)
/// floors at 0 rather than wrapping through the `as u64` cast.
pub fn token_from_revision(revision: i64) -> FencingToken {
    revision.max(0) as FencingToken
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_track_the_revision_monotonically() {
        assert!(token_from_revision(2) > token_from_revision(1));
        assert!(token_from_revision(1_000_000) > token_from_revision(999_999));
        assert_eq!(token_from_revision(7), 7);
    }

    #[test]
    fn a_non_positive_revision_floors_at_zero_not_wraps() {
        assert_eq!(token_from_revision(0), 0);
        assert_eq!(token_from_revision(-1), 0, "must not wrap to u64::MAX");
        assert_eq!(token_from_revision(i64::MIN), 0);
    }
}
