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
