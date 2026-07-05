//! Pure keep-alive cadence. No etcd dependency, so it is unit-tested on every
//! build.
//!
//! A leadership/lock lease is granted for `ttl` seconds and renewed by a
//! background task at [`keepalive_interval`] — a third of the TTL, so two
//! consecutive missed renewals still leave headroom before the lease lapses (the
//! etcd-recipe cadence). Floored at one second so a tiny TTL never spins the
//! renewer into a busy loop.

use std::time::Duration;

/// The renewal cadence for a lease of `ttl_secs`: `ttl/3`, floored at 1s.
pub fn keepalive_interval(ttl_secs: i64) -> Duration {
    let ttl = ttl_secs.max(1);
    Duration::from_secs((ttl / 3).max(1) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renews_at_a_third_of_the_ttl_with_headroom() {
        assert_eq!(keepalive_interval(6), Duration::from_secs(2));
        assert_eq!(keepalive_interval(30), Duration::from_secs(10));
        // Two missed renewals (2 * ttl/3) still precede expiry (ttl).
        let ttl = 9;
        assert!(keepalive_interval(ttl).as_secs() * 2 < ttl as u64);
    }

    #[test]
    fn a_tiny_or_bogus_ttl_is_floored_at_one_second() {
        assert_eq!(keepalive_interval(1), Duration::from_secs(1));
        assert_eq!(keepalive_interval(2), Duration::from_secs(1));
        assert_eq!(keepalive_interval(0), Duration::from_secs(1));
        assert_eq!(keepalive_interval(-5), Duration::from_secs(1));
    }
}
