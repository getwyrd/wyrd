//! Pure etcd key-layout for the coordination backend. No etcd dependency, so it
//! is unit-tested on every build (including the default feature-off `ci`).
//!
//! Every key is placed under a per-instance `ns` (namespace) prefix so several
//! coordinators can share one etcd cluster without colliding — the shared
//! contract suite drives a fresh namespace per clause, exactly as the TiKV suite
//! scopes a fresh keyspace per clause. Within a namespace the four concerns get
//! disjoint sub-prefixes so a registration can never be mistaken for a lock, an
//! election, or a config value.

/// Registrations (leased service discovery).
const REG: &str = "reg/";
/// Distributed locks.
const LOCK: &str = "lock/";
/// Leader-election groups.
const ELECT: &str = "elect/";
/// Zone-wide config.
const CONFIG: &str = "cfg/";

/// Percent-encode the two characters that would otherwise break the
/// registration keyspace: `/` (which a raw hierarchical key would use to forge an
/// etcd key-prefix nesting) and `%` (so the encoding is reversible / injective).
/// The result contains **no `/`**, so a discovery key becomes a single opaque
/// segment that the trailing `/` in [`registration_member`] / [`registration_prefix`]
/// delimits exactly — `discover("a")` can never match a member of `"a/b"`.
fn encode_segment(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for ch in key.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            c => out.push(c),
        }
    }
    out
}

/// The member key a single registration under `key` (with lease `lease_id`)
/// occupies. The lease id disambiguates several members of the same discovery
/// key; the trailing separator on [`registration_prefix`] keeps `svc/d` from
/// matching `svc/deep`, and the [`encode_segment`] of the logical key keeps a
/// nested key `a/b` from being discovered under its parent `a`.
pub fn registration_member(ns: &str, key: &str, lease_id: i64) -> String {
    format!("{ns}{REG}{}/{lease_id:016x}", encode_segment(key))
}

/// The prefix that discovers every member registered under `key`.
pub fn registration_prefix(ns: &str, key: &str) -> String {
    format!("{ns}{REG}{}/", encode_segment(key))
}

/// The single key a distributed lock on `key` occupies.
pub fn lock_key(ns: &str, key: &str) -> String {
    format!("{ns}{LOCK}{key}")
}

/// The election name to campaign under for leadership of `key`.
///
/// etcd's election recipe campaigns under `<name>/<lease>` and observes the leader
/// by prefix-scanning `<name>/`, so the election *name* is itself an etcd key
/// prefix — exactly like [`registration_prefix`]. A raw `/` inside the logical key
/// would therefore forge a nesting: a candidate for the nested election `a/b`
/// (`elect/a/b/<lease>`) would surface when observing the parent election `a`
/// (prefix `elect/a/`), a cross-election leader bleed. [`encode_segment`] collapses
/// the logical key to one opaque `/`-free segment so `elect(a)` and `elect(a/b)`
/// stay disjoint — the same isolation the registration keyspace already has.
pub fn election_name(ns: &str, key: &str) -> String {
    format!("{ns}{ELECT}{}", encode_segment(key))
}

/// The key a config value for `key` occupies.
pub fn config_key(ns: &str, key: &str) -> String {
    format!("{ns}{CONFIG}{key}")
}

/// The prefix spanning all config keys (used to read the cluster revision that
/// backs `config_revision`).
pub fn config_prefix(ns: &str) -> String {
    format!("{ns}{CONFIG}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concerns_live_under_disjoint_prefixes() {
        let ns = "wyrd/coord/A/";
        let reg = registration_member(ns, "svc/d", 1);
        let lock = lock_key(ns, "svc/d");
        let elect = election_name(ns, "svc/d");
        let cfg = config_key(ns, "svc/d");
        // No concern's key is a prefix of another concern's key — a registration
        // can never be read as a lock/election/config and vice versa.
        for (a, b) in [(&reg, &lock), (&reg, &elect), (&reg, &cfg), (&lock, &elect)] {
            assert!(
                !a.starts_with(b.as_str()) && !b.starts_with(a.as_str()),
                "{a} vs {b}"
            );
        }
    }

    #[test]
    fn discovery_prefix_isolates_sibling_keys() {
        let ns = "N/";
        // `svc/d`'s members must not be discovered under `svc/deep`.
        let member = registration_member(ns, "svc/d", 42);
        assert!(member.starts_with(&registration_prefix(ns, "svc/d")));
        assert!(!member.starts_with(&registration_prefix(ns, "svc/deep")));
        assert!(!member.starts_with(&registration_prefix(ns, "svc/other")));
    }

    #[test]
    fn discovery_prefix_isolates_hierarchical_keys() {
        let ns = "N/";
        // A member registered under the NESTED key "a/b" must NOT be discovered
        // under its parent key "a": the `/` inside the logical key must not forge an
        // etcd key-prefix nesting (else `discover("a")` leaks every member of
        // "a/b", "a/b/c", … — cross-key discovery bleed). Regression for the
        // prefix-collision bug: raw keys made `reg/a/b/<lease>` start with
        // `reg/a/`.
        let nested = registration_member(ns, "a/b", 1);
        assert!(
            !nested.starts_with(&registration_prefix(ns, "a")),
            "discover(\"a\") must not match a member of \"a/b\": {nested}"
        );
        // The deeper key still discovers its OWN member.
        assert!(
            nested.starts_with(&registration_prefix(ns, "a/b")),
            "\"a/b\" must still discover its own member: {nested}"
        );
        // And a key that is a raw-string prefix of another ("a" vs "ab") stays
        // isolated too — the encoded segment plus its delimiter keep them disjoint.
        let sibling = registration_member(ns, "ab", 2);
        assert!(!sibling.starts_with(&registration_prefix(ns, "a")));
    }

    #[test]
    fn election_prefix_isolates_hierarchical_keys() {
        let ns = "N/";
        // etcd's election recipe campaigns under `<name>/<lease>` and observes the
        // leader by prefix-scanning `<name>/`. A candidate for the NESTED election
        // "a/b" must NOT surface when observing the PARENT election "a" — otherwise a
        // campaign for "a/b" is read as a leader (or contender) of "a", a bogus fenced
        // term / cross-election bleed. Regression for the election prefix-collision:
        // a raw `election_name` made `elect/a/b/<lease>` start with the observe prefix
        // `elect/a/` (mirror of `discovery_prefix_isolates_hierarchical_keys`).
        let observe_a = format!("{}/", election_name(ns, "a"));
        let candidate_ab = format!("{}/{:016x}", election_name(ns, "a/b"), 7);
        assert!(
            !candidate_ab.starts_with(&observe_a),
            "observing election \"a\" must not match a candidate of \"a/b\": {candidate_ab}"
        );
        // The nested election still observes its OWN candidate.
        let observe_ab = format!("{}/", election_name(ns, "a/b"));
        assert!(
            candidate_ab.starts_with(&observe_ab),
            "election \"a/b\" must still observe its own candidate: {candidate_ab}"
        );
        // A raw-string sibling ("a" vs "ab") stays isolated too — etcd's own trailing
        // `/` handles this one, but the encoded segment keeps it robust.
        let candidate_ab_sib = format!("{}/{:016x}", election_name(ns, "ab"), 8);
        assert!(!candidate_ab_sib.starts_with(&observe_a));
    }

    #[test]
    fn namespaces_do_not_collide() {
        assert_ne!(
            registration_member("A/", "k", 1),
            registration_member("B/", "k", 1)
        );
        assert!(lock_key("A/", "k").starts_with("A/"));
        assert_eq!(config_prefix("A/"), "A/cfg/");
        assert!(config_key("A/", "zone").starts_with(&config_prefix("A/")));
    }

    #[test]
    fn member_key_is_stable_and_lease_scoped() {
        assert_eq!(
            registration_member("", "svc", 255),
            "reg/svc/00000000000000ff"
        );
        assert_eq!(registration_prefix("", "svc"), "reg/svc/");
    }
}
