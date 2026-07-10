//! #441 Success criterion — binds `wyrd_metadata_fdb::preflight` as a non-feature-gated,
//! **pure** module.
//!
//! This is its own test binary — not a `#[cfg(test)] mod` co-located inside a modified
//! `src/lib.rs` — specifically so `cargo test -p wyrd-metadata-fdb --test preflight`
//! exercises it on the **default build**: no `fdb` feature, no `libfdb_c`, no Docker, stock
//! toolchain (#441's Falsifiability + Test file fields; `engine/scripts/run-verify.sh`'s
//! `_is_test_file`, `:69`, only recognizes an ADDED `*/tests/*.rs` as earning a full
//! red→green — a co-located test would degrade `C4-verify` to green-only, `:244-254`).
//!
//! `use wyrd_metadata_fdb::preflight::*` compiling at all, with no `--features fdb`, IS
//! assertion 1: `preflight` is `pub`, non-feature-gated, and takes no `foundationdb` type in
//! its signature — sibling to `classify` (`crates/metadata-fdb/src/lib.rs:141`) and `config`
//! (`:384`; `:382` before this patch inserted `EXTERNAL_CLIENT_DIR_ENV` above it).
//!
//! What this binary does **not** cover, deliberately: the live probe that feeds `verdict`
//! (`FdbMetadataStore::preflight`, `crates/metadata-fdb/src/lib.rs`) is feature-gated and
//! links `libfdb_c`, so it is driven from inside a Tokio runtime by
//! `tests/timeout.rs::preflight_against_an_unreachable_cluster_is_err_not_a_panic` and
//! `tests/conformance.rs::connect_probes_the_real_cluster_from_inside_a_runtime`, both run
//! by `cargo xtask fdb-conformance`.

use std::time::Duration;

use wyrd_metadata_fdb::preflight::{message, verdict, ClientStatus, Verdict};

/// A ~5s bound, matching #441's design proposal: all three captured fixtures below settled
/// within ~2s against a live `libfdb_c` 7.3.77 (`LastConnectTime` 1.93s in the skew run),
/// so a 5s deadline separates every case.
const DEADLINE: Duration = Duration::from_secs(5);

/// #441's design proposal, §Design 1's fixture table, row **skew**: a 7.3 client against
/// `foundationdb:7.1.61`. Captured JSON: `Healthy: false`, `Connections[0].Status:
/// "connected"`, `Connections[0].Compatible: false`, `Connections[0].ProtocolVersion:
/// "fdb00b071010000"` (the *cluster's*), `NumConnectionsFailed: 0`. `store::client_status`
/// (the feature-gated JSON→`ClientStatus` reduction this fixture stands in for) sets
/// `cluster_protocol` only when the live JSON reported a protocol version for an
/// incompatible connection — exactly this shape.
fn skew_fixture() -> ClientStatus {
    ClientStatus {
        healthy: false,
        coordinators_reachable: true,
        client_version: "api 730 (fdb-7_3 pin)".to_string(),
        cluster_protocol: Some("fdb00b071010000".to_string()),
    }
}

/// #441's design proposal, §Design 1's fixture table, row **unreachable**: nothing listening.
/// Captured JSON: `Healthy: false`, `Connections[0].Status: "failed"`,
/// `Connections[0].Compatible: true` (meaningless here — no protocol was ever exchanged),
/// `Connections[0].ProtocolVersion` **absent**, `NumConnectionsFailed: 1`. Note: the
/// `Coordinators` list is a real, populated field of this JSON too — deliberately not
/// modelled in [`ClientStatus`], because the design proposal found it does NOT distinguish
/// this case from skew (both keep it populated); `coordinators_reachable` here is the
/// *connection status* signal, not the coordinator list.
fn unreachable_fixture() -> ClientStatus {
    ClientStatus {
        healthy: false,
        coordinators_reachable: false,
        client_version: "api 730 (fdb-7_3 pin)".to_string(),
        cluster_protocol: None,
    }
}

/// #441's design proposal, §Design 1's fixture table, row **healthy**: a 7.3 client against
/// `foundationdb:7.3.77`. Captured JSON: `Healthy: true`, `Connections[0].Status:
/// "connected"`, `Connections[0].Compatible: true`, `Connections[0].ProtocolVersion:
/// "fdb00b073000000"`, `NumConnectionsFailed: 0`. `cluster_protocol` is `None` here because
/// `store::client_status` only carries a protocol version for an *incompatible* connection
/// (there is nothing to report when client and cluster already agree).
fn healthy_fixture() -> ClientStatus {
    ClientStatus {
        healthy: true,
        coordinators_reachable: true,
        client_version: "api 730 (fdb-7_3 pin)".to_string(),
        cluster_protocol: None,
    }
}

/// Assertion 3 (skew row): a connected-but-incompatible status classifies as
/// `VersionSkew`, naming the **cluster's** protocol version — never the client's, which
/// would misdiagnose the mismatch's direction.
#[test]
fn skew_fixture_is_version_skew() {
    let status = skew_fixture();
    let v = verdict(Some(&status), Duration::from_millis(1930), DEADLINE);
    assert_eq!(
        v,
        Verdict::VersionSkew {
            client: status.client_version.clone(),
            cluster: Some("fdb00b071010000".to_string()),
        },
        "a connected-but-incompatible status must classify as VersionSkew: {v:?}"
    );
}

/// Assertion 3 (unreachable row): a failed connection with no protocol version classifies
/// as `Unreachable`, not skew.
#[test]
fn unreachable_fixture_is_unreachable() {
    let status = unreachable_fixture();
    let v = verdict(Some(&status), Duration::from_millis(50), DEADLINE);
    assert_eq!(
        v,
        Verdict::Unreachable {
            waited: Duration::from_millis(50)
        },
        "a failed connection with no protocol version must classify as Unreachable: {v:?}"
    );
}

/// Assertion 3 (healthy row): a healthy, connected, compatible status classifies as
/// `Ready`.
#[test]
fn healthy_fixture_is_ready() {
    let status = healthy_fixture();
    let v = verdict(Some(&status), Duration::from_millis(120), DEADLINE);
    assert_eq!(
        v,
        Verdict::Ready,
        "a healthy, connected, compatible client must be Ready: {v:?}"
    );
}

/// Assertion 2: `message(VersionSkew)` contains `protocol`, the **cluster's** protocol
/// version, and a pointer to the multi-version external-client-directory upgrade
/// procedure — an operator must be able to act on this without reading source.
#[test]
fn version_skew_message_names_the_cluster_protocol_and_the_upgrade_path() {
    let v = Verdict::VersionSkew {
        client: "api 730 (fdb-7_3 pin)".to_string(),
        cluster: Some("fdb00b071010000".to_string()),
    };
    let msg = message(&v);

    assert!(
        msg.contains("protocol"),
        "names the axis of the mismatch: {msg}"
    );
    assert!(
        msg.contains("fdb00b071010000"),
        "names the CLUSTER's protocol version (the field that identifies the mismatch, \
         #441's design proposal), not just the client's: {msg}"
    );
    assert!(
        msg.contains("WYRD_FDB_EXTERNAL_CLIENT_DIR")
            || msg.to_lowercase().contains("external-client")
            || msg.to_lowercase().contains("multi-version"),
        "points at the multi-version external-client-directory upgrade procedure: {msg}"
    );
}

/// Assertion 2: `message(Unreachable)` must NOT claim version skew — an operator whose
/// cluster is genuinely down must not be told (falsely) that their client is mismatched.
#[test]
fn unreachable_message_does_not_claim_version_skew() {
    let msg = message(&Verdict::Unreachable {
        waited: Duration::from_secs(5),
    });
    assert!(
        !msg.contains("mismatch"),
        "an Unreachable verdict must not claim a version mismatch: {msg}"
    );
}

/// Fail-honest (§Design 1): a status this probe cannot positively call skew — connected,
/// unhealthy, but with no reported cluster protocol version at all — degrades to
/// `Unreachable` rather than guessing `VersionSkew`. This is the shape an unparsable or
/// novel `get_client_status()` payload reduces to.
#[test]
fn an_ambiguous_status_degrades_to_unreachable_never_a_guessed_skew() {
    let ambiguous = ClientStatus {
        healthy: false,
        coordinators_reachable: true,
        client_version: "api 730 (fdb-7_3 pin)".to_string(),
        cluster_protocol: None,
    };
    let v = verdict(Some(&ambiguous), Duration::from_millis(100), DEADLINE);
    assert_eq!(
        v,
        Verdict::Unreachable {
            waited: Duration::from_millis(100)
        },
        "a status with no cluster protocol version must never guess VersionSkew: {v:?}"
    );
}
