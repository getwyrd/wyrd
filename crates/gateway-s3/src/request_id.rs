//! The **request id** — the join key between a client's failure and the server's record of
//! it (issue #529).
//!
//! Before this, an S3 error carried a `<Code>` and a `<Message>` and nothing else: no
//! `<RequestId>`, no `x-amz-request-id` header, no correlation id anywhere in the process.
//! A field tester reporting *"my upload failed at 14:32"* handed you a wall-clock timestamp
//! and a 5xx, and there was no way to find what the server had been doing for **that**
//! request. Every S3 SDK surfaces the request id on failure; every S3 runbook starts from
//! it. This mints one, returns it on every response, records it on every log line emitted
//! under the request.
//!
//! **Not yet: propagating the id into gRPC metadata so the *D-servers'* own logs carry it.**
//! Doing that properly needs a task-local threaded across the `ObjectGateway` seam and
//! re-scoped at every `tokio::spawn` — a larger change than everything here put together. It is
//! an increment rather than a blocker: the gateway already records *which* D-server faulted
//! (#530), and does so under this span, so "which node misbehaved" is answerable from the
//! gateway's logs alone. Tracked on #529; deliberately not half-built here, and deliberately
//! not pre-announced with a constant naming a metadata key nothing sends (#532 review).
//!
//! ## The id scheme — deliberately the chunk-id scheme
//!
//! A per-process random 64-bit epoch (top bit set) forms the high half; a monotonic counter
//! forms the low half. This is exactly `Gateway::mint_chunk_id`'s coordination-free scheme
//! (ADR-0019), reused rather than reinvented, and it inherits its properties:
//!
//! - **No new dependency.** The entropy is [`std::collections::hash_map::RandomState`], which
//!   the standard library seeds from the OS RNG. No `uuid`, no `rand` in the gateway binary.
//! - **Unique across concurrent gateways** without any shared allocator: two gateway processes
//!   draw independent epochs, so their id ranges are disjoint.
//! - **Never repeats within a process**, because the counter is monotonic.
//!
//! Rendered as 32 lowercase hex characters — the same canonical form as
//! [`wyrd_traits::chunk_hex`], so every identifier in a wyrd log reads alike.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// The header every S3 response carries the id in. The name is the AWS one, so an SDK's
/// own error reporting and its logs surface it without any client-side change.
pub const HEADER: &str = "x-amz-request-id";

/// Mints request ids for one gateway process.
#[derive(Debug)]
pub struct RequestIds {
    epoch: u64,
    next: AtomicU64,
}

impl Default for RequestIds {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestIds {
    /// Draw this process's epoch from OS entropy (via `RandomState`, as the chunk-id
    /// minter does) so concurrent gateways cannot collide.
    pub fn new() -> Self {
        use std::hash::{BuildHasher, Hasher};
        let raw = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        Self {
            epoch: raw | (1u64 << 63),
            next: AtomicU64::new(0),
        }
    }

    /// Mint the next id. Monotonic within the process, disjoint across processes.
    pub fn mint(&self) -> RequestId {
        let seq = self.next.fetch_add(1, Ordering::Relaxed);
        RequestId((u128::from(self.epoch) << 64) | u128::from(seq))
    }
}

/// One request's id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId(u128);

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn ids_never_repeat_within_a_process() {
        let ids = RequestIds::new();
        let minted: HashSet<String> = (0..1000).map(|_| ids.mint().to_string()).collect();
        assert_eq!(minted.len(), 1000, "a repeated id would alias two requests");
    }

    #[test]
    fn two_gateways_draw_disjoint_ranges() {
        // The multi-writer property the chunk-id scheme exists for (#477), inherited here:
        // two gateway processes fronting one cluster must not mint the same request id, or
        // a correlated log search returns another node's request.
        let (a, b) = (RequestIds::new(), RequestIds::new());
        assert_ne!(
            a.mint().to_string(),
            b.mint().to_string(),
            "independent epochs must keep concurrent gateways' ids apart"
        );
    }

    #[test]
    fn the_rendering_is_canonical_32_char_hex() {
        let id = RequestIds::new().mint().to_string();
        assert_eq!(id.len(), 32, "{id}");
        assert!(id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
    }
}
