//! Xtask orchestration helpers exposed as a **library target** so the
//! integration tests under `xtask/tests/` can import and unit-test the
//! host-independent parts without a privileged environment.
//!
//! This target is the born-at-tier flippable coverage seam (ADR-0016 /
//! `templates/brief.md.tpl` "deferred ≠ unbuilt"): when the helpers are
//! removed or stubbed, `cargo test -p xtask --test disk_faults_orchestration`
//! goes RED, proving the seam is load-bearing; with them implemented it goes
//! GREEN. The privileged scenario itself (`crates/custodian/tests/
//! tier1_disk_faults.rs`) is compiled and type-checked by `cargo test
//! --workspace` but is `#[ignore]`d — its body runs only in the off-Check
//! Tier-1 CI job (`.github/workflows/tier1-disk-faults.yml`).

#![forbid(unsafe_code)]

pub mod consistency_run;
pub mod deploy_guard;
pub mod disk_faults;
pub mod dist;
pub mod fdb_doctor;
pub mod metadata_faults;
pub mod nemesis;
pub mod repo_guard;

/// Opt the `tikv` feature's type-check into `cargo xtask ci`. Set only by the privileged
/// Tier CI job that has the pre-1.0 `tikv-client` build toolchain (grpcio/protoc).
pub const TIKV_TOOLCHAIN_ENV: &str = "WYRD_TIKV_TOOLCHAIN";

/// Opt the `fdb` feature's type-checks into `cargo xtask ci`. Set only by
/// `.github/workflows/fdb-conformance.yml`, the one runner that installs the
/// FoundationDB client package (`libfdb_c`, linked at build time).
///
/// **Separate from [`TIKV_TOOLCHAIN_ENV`], and that is the point** (#439): the two
/// backends need different privileged environments, so neither may imply the other.
/// `main.rs`'s `run_ci_steps` reads both names *itself* from an injected environment
/// lookup, so no call site is left in which one gate's boolean could be passed for the
/// other's.
pub const FDB_TOOLCHAIN_ENV: &str = "WYRD_FDB_TOOLCHAIN";

/// The dedicated `cargo clippy --features …` runs `run_ci` makes to compile AND
/// lint feature-gated code the default `--workspace` build never touches.
///
/// `run_ci`'s `build`/`test`/`clippy` all run `--workspace` with **default**
/// features. `--all-targets` widens the *target kinds* (bins, tests, benches) but
/// **not the feature set**, so any `#[cfg(feature = "…")]` body that is off by
/// default is compiled by **none** of those steps — a type error inside it passes
/// the whole gate silently.
///
/// Two backends sit behind such a feature today, and **each carries its own toolchain
/// gate**, because each needs a *different* privileged build environment:
///
/// * `tikv` — the M4.6 (#257) Tier-1/Tier-2 metadata scenarios
///   (`crates/metadata-tikv/tests/tier1_metadata_consistency.rs`, `tier2_metadata_io.rs`):
///   their live bodies (the `SymmetricPartition` fault, its `Drop` heal, the PD-side
///   fault-effect oracle) sit behind `#[cfg(feature = "tikv")]`. Compiling them means
///   compiling the pre-1.0 `tikv-client` tree (grpcio/protoc), which the default
///   container-free / offline `cargo xtask ci` must never touch. Opted in with
///   [`TIKV_TOOLCHAIN_ENV`].
/// * `fdb` — the ADR-0042 production metadata backend (#438, #440, #439): the whole
///   `#[cfg(feature = "fdb")]` `store` module of `crates/metadata-fdb/src/lib.rs` and
///   every `#[cfg(feature = "fdb")]` backend-selection arm in `crates/server/src/cli.rs`
///   (`:101`, `:120`, `:168`, `:373`, `:459`, `:714`, `:840`, `:1480`, `:1486`).
///   Compiling them means linking the system `libfdb_c`, which a plain laptop or PDCA
///   worktree does not have. Opted in with [`FDB_TOOLCHAIN_ENV`].
///
/// **The two gates are independent.** A single zero-argument `feature_gated_checks()`
/// gated on one boolean — the shape this replaced (`main.rs`'s `if tikv_toolchain { … }`)
/// — would have fired the FDB typecheck only when `WYRD_TIKV_TOOLCHAIN` happened to be
/// set: a silent, wrong coupling between two unrelated backends.
///
/// Lives in the **lib** target (like [`deploy_guard`]) so `xtask/tests/fdb_harness.rs`
/// can call it directly and assert each set of rows appears independently of the other.
/// `main.rs`'s `run_ci_steps` is its only production caller; that call site derives both
/// booleans from the two env names above and is covered by `run_ci_steps`' own
/// recording-executor unit test, so reading one gate's variable for the other flips red.
/// `clippy`, not `check` (#619): the workspace clippy step never enables these
/// features, so a `#[cfg(feature = ...)]` body was compiled by the anti-rot bar
/// but linted by nothing — `clippy.toml`'s wall-clock ban and the whole
/// `clippy.all = "deny"` policy stopped at the feature boundary. Clippy also
/// type-checks, so this is a strict superset of what `check` covered.
pub fn feature_gated_checks(tikv: bool, fdb: bool) -> Vec<Vec<&'static str>> {
    let mut checks = Vec::new();
    if tikv {
        checks.push(vec![
            "clippy",
            "-p",
            "wyrd-metadata-tikv",
            "--features",
            "tikv",
            "--tests",
        ]);
        // The backend crate alone is not the whole `tikv` surface: the `Tikv` variant of
        // `MetadataBackend` and its selection arms in `crates/server/src/cli.rs` are all
        // `#[cfg(feature = "tikv")]`, so checking only `wyrd-metadata-tikv` leaves the CLI
        // wiring uncompiled and free to rot. #443 keeps that variant as part of the retained
        // fallback, so the anti-rot bar has to cover it.
        //
        // `tikv,etcd`, NOT `tikv` alone — the pairing is the point. The S3 gateway's
        // metadata×coordination dispatch arm is `#[cfg(all(feature = "tikv", feature =
        // "etcd"))]` (cli.rs), so a `--features tikv` build cfg's it out entirely: the bar
        // could stay green while that combination rotted. And it is not a hypothetical
        // combination — it is exactly what the retained fallback stack builds
        // (`deploy/small-multi-node/docker-compose.yml`, `FEATURES: "tikv,etcd"`, whose
        // gateways run `--metadata-backend tikv --coordination-backend etcd`). Compiling the
        // pair also compiles every `tikv`-only arm, so it is a superset, not a trade.
        checks.push(vec![
            "clippy",
            "-p",
            "wyrd-server",
            "--features",
            "tikv,etcd",
            "--tests",
        ]);
    }
    if fdb {
        checks.push(vec![
            "clippy",
            "-p",
            "wyrd-metadata-fdb",
            "--features",
            "fdb",
            "--tests",
        ]);
        // `fdb,etcd` for the same reason as the tikv row above: the dispatch arm is
        // `#[cfg(all(feature = "fdb", feature = "etcd"))]`, and `deploy/small-multi-node-fdb/`
        // builds `FEATURES: "fdb,etcd"`. A plain `--features fdb` check left that arm — the
        // one the CANONICAL production stack actually executes — compiled by no CI job at all.
        checks.push(vec![
            "clippy",
            "-p",
            "wyrd-server",
            "--features",
            "fdb,etcd",
            "--tests",
        ]);
    }
    checks
}

/// The **dependency wall** (ADR-0003 §2), as the three `cargo deny` invocations
/// [`cargo_deny_check`](../main.rs) makes — lifted here so a test can assert the wall's SHAPE,
/// not just that some deny command ran.
///
/// One invocation is not enough, because `cargo deny` audits the graph as resolved for the
/// features it is given, and both backend features are off by default:
///
/// 1. the DEFAULT graph — the artifact we ship. Everything, zero tolerance, no backend
///    exceptions.
/// 2. the OFF-BY-DEFAULT trees' ADVISORIES, from a separate config whose `ignore` entries must
///    never reach the shipped wall (#543): an advisory ignore SUPPRESSES, and cargo-deny keys
///    it by ID alone, so parked in `deny.toml` it would also mask that ID if a future default
///    dependency pulled an affected version.
/// 3. the same trees' LICENCES / bans / sources, from `deny.toml`'s policy (#547). ADR-0003 §2
///    judges *linked* crates, and a `--features fdb` / `--features tikv` build links crates the
///    default graph never sees — so without this an AGPL/BSL dependency could enter either
///    optional tree and pass CI.
///
/// Steps 2 and 3 use different configs, and that is NOT duplication: an advisory `ignore`
/// suppresses (so it must be quarantined), while a licence `allow` is an allowlist (so applying
/// it to a wider graph can only reject MORE). The ADR-0003 allowlist therefore stays
/// single-source in `deny.toml`. Two allowlists would drift, and a drifted licence wall is
/// worse than one wall.
pub fn dependency_wall_invocations() -> Vec<Vec<&'static str>> {
    vec![
        vec!["deny", "check"],
        vec![
            "deny",
            "--all-features",
            "--config",
            "deny-all-features.toml",
            "check",
            "advisories",
        ],
        vec![
            "deny",
            "--all-features",
            "check",
            "licenses",
            "bans",
            "sources",
        ],
    ]
}
