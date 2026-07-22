//! Regression guard for the README "Development & testing" on-ramp (issue #152).
//!
//! The root `README.md` is not covered by `cargo xtask ci` (Rust-only) or the
//! `docs/` linter, so its on-ramp can silently drift from the code it documents.
//! This test pins the section to the real command surface: every `cargo xtask`
//! entry point and the `wyrd demo` try-it line the README names must be backed by
//! the actual dispatch in `xtask/src/main.rs` and `crates/server/src/cli.rs`.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

/// The workspace root (`<root>/xtask` is this crate's manifest dir).
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate is nested under the workspace root")
        .to_path_buf()
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn readme_documents_development_and_testing() {
    let readme = read("README.md");
    let main_rs = read("xtask/src/main.rs");

    // A dedicated on-ramp section exists.
    assert!(
        readme.contains("## Development & testing"),
        "README is missing the `## Development & testing` section"
    );

    // (a) every xtask entry point is documented AND backed by the real dispatch,
    //     so the README cannot name a command `cargo xtask` does not have.
    for sub in ["ci", "integration", "bench", "dst", "conformance"] {
        let cmd = format!("cargo xtask {sub}");
        assert!(readme.contains(&cmd), "README does not document `{cmd}`");
        assert!(
            main_rs.contains(&format!("Some(\"{sub}\")")),
            "xtask/src/main.rs does not dispatch `{sub}` — \
             the README would document a non-existent command"
        );
    }

    // (b) the Docker + compose-plugin prerequisite for the integration tier.
    assert!(
        readme.contains("Docker") && readme.to_lowercase().contains("compose"),
        "README does not state the Docker + Compose-plugin prerequisite for \
         `cargo xtask integration`"
    );

    // (c) WYRD_DSERVER_COUNT — documented, and still read by the integration tier.
    assert!(
        readme.contains("WYRD_DSERVER_COUNT"),
        "README does not document the WYRD_DSERVER_COUNT knob"
    );
    assert!(
        main_rs.contains("WYRD_DSERVER_COUNT"),
        "xtask/src/main.rs no longer reads WYRD_DSERVER_COUNT — README would be stale"
    );

    // (d) the "Try it" line, and the package/bin/subcommand it names are all real.
    let demo = "cargo run -p wyrd-server --bin wyrd -- demo";
    assert!(
        readme.contains(demo),
        "README is missing the `{demo}` try-it line"
    );

    let cli_rs = read("crates/server/src/cli.rs");
    assert!(
        cli_rs.contains("Some(\"demo\")"),
        "crates/server/src/cli.rs does not dispatch the `demo` subcommand"
    );
    let server_manifest = read("crates/server/Cargo.toml");
    assert!(
        server_manifest.contains("name = \"wyrd-server\""),
        "crates/server is not package `wyrd-server` — the `-p` flag would be wrong"
    );
    assert!(
        server_manifest.contains("name = \"wyrd\""),
        "crates/server does not build a `wyrd` binary — the `--bin` flag would be wrong"
    );
}
