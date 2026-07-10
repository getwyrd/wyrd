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

pub mod deploy_guard;
pub mod disk_faults;
pub mod fdb_doctor;
pub mod metadata_faults;

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

/// The dedicated `cargo check --features …` runs `run_ci` makes to type-check
/// feature-gated code the default `--workspace` build never compiles.
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
pub fn feature_gated_checks(tikv: bool, fdb: bool) -> Vec<Vec<&'static str>> {
    let mut checks = Vec::new();
    if tikv {
        checks.push(vec![
            "check",
            "-p",
            "wyrd-metadata-tikv",
            "--features",
            "tikv",
            "--tests",
        ]);
    }
    if fdb {
        checks.push(vec![
            "check",
            "-p",
            "wyrd-metadata-fdb",
            "--features",
            "fdb",
            "--tests",
        ]);
        checks.push(vec![
            "check",
            "-p",
            "wyrd-server",
            "--features",
            "fdb",
            "--tests",
        ]);
    }
    checks
}
