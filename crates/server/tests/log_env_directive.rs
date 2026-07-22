//! **`RUST_LOG` is held to the same standard as `--log-level`** (#531 review).
//!
//! The rule the whole logging feature rests on — a malformed log option **stops** the process
//! rather than degrading it — applied to `--log-level` and not to `RUST_LOG`. A typo'd directive
//! was swallowed, `EnvFilter` fell back to the default, and the operator got a running process
//! whose log configuration *looked* applied and was not. `RUST_LOG` is the route an operator is
//! far more likely to take, so that was the hole that mattered.
//!
//! # Why this is its own test binary
//!
//! It mutates `RUST_LOG`, which is **process-global**. `cargo test` runs a crate's unit tests as
//! threads of ONE process, so setting it there made sibling tests — any that construct a
//! `LogConfig` — observe the bad value and fail. (They did: two went red the first time this was
//! written as a unit test. The contamination is the reason, not a flake.) An integration test is
//! its own process, so the mutation cannot reach anything else.
//!
//! The rejection RULE itself is pinned by a pure unit test (`validate_env_directive`) in the
//! module; this pins the WIRING — that `LogConfig::new` actually consults the environment. Without
//! it, a `new` that never read `RUST_LOG` would keep the pure test green.

#![forbid(unsafe_code)]

use wyrd_server::logging::LogConfig;

#[test]
fn a_malformed_rust_log_fails_the_process_instead_of_installing_a_filter_nobody_asked_for() {
    std::env::set_var("RUST_LOG", "=bad");

    let rejected = LogConfig::new(None, None);
    assert!(
        rejected.is_err(),
        "a malformed RUST_LOG must be REFUSED: with no --log-level it is the directive that \
         decides the filter, and silently substituting the default leaves the operator running \
         a log configuration they never asked for",
    );
    let message = rejected.unwrap_err().to_string();
    assert!(
        message.contains("RUST_LOG"),
        "the error must name RUST_LOG, or the operator cannot tell WHICH log option they \
         fat-fingered: {message}",
    );

    // An explicit `--log-level` wins outright and RUST_LOG is never consulted — so a bad
    // RUST_LOG must not fail a run that does not use it. (Without this, "reject everything when
    // RUST_LOG is bad" would also pass the assertion above.)
    assert!(
        LogConfig::new(Some("warn"), None).is_ok(),
        "an explicit --log-level overrides RUST_LOG entirely; a bad RUST_LOG must not fail a run \
         that never reads it",
    );

    std::env::remove_var("RUST_LOG");

    // …and with RUST_LOG unset, the default stands: the rejection is not a blanket refusal.
    assert!(
        LogConfig::new(None, None).is_ok(),
        "an unset RUST_LOG takes the default level, as it always did",
    );
}
