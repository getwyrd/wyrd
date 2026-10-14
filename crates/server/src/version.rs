//! The build identity of this `wyrd` binary (#778): which checkout it was compiled from.
//!
//! [`BUILD_IDENTITY`] is baked in at compile time by `crates/server/build.rs`, which
//! resolves it through [`derivation::resolve`] — the same module `cargo xtask dist` uses
//! to name its artifacts, so there is one derivation with two compiled consumers. The
//! rungs, in order:
//!
//! 1. `WYRD_VERSION` from the build environment, verbatim (after validation). This is how
//!    `cargo xtask dist` hands the version it wrote to the tarball's `VERSION` into the
//!    image build, whose context excludes `.git/` (`.dockerignore`), so a released binary
//!    carries `dist`'s word, `.dirty` suffix included when `dist` ran on a dirty tree. A
//!    set value that does not validate, or is not UTF-8, fails the build; it is never
//!    replaced by another rung's answer.
//! 2. Otherwise `git describe --tags --always` of the workspace's OWN repository (a
//!    checkout nested inside an unrelated repository never bakes the outer commit),
//!    normalized by [`derivation::normalize_describe`]. A describe output that would not
//!    make a usable identity — would carry the word `dirty`, or is not UTF-8 — or no
//!    describe answer at all fails closed to `0.0.0+git.<short sha>`, with a build
//!    warning.
//! 3. Otherwise `0.0.0+git.unknown`, with a build warning: any build that cannot name its
//!    commit (an image build that was not handed `WYRD_VERSION`, an unpacked source
//!    tree). Such a build re-runs the build script on every build, so the identity
//!    recovers as soon as git can answer instead of staying `unknown` until a
//!    `cargo clean`.
//!
//! **The limit, stated plainly:** a hand-built binary names its base commit; a released
//! one carries `dist`'s word. Rung 2 runs WITHOUT `--dirty`, so the identity names the
//! COMMIT the build was made from. It is not an attestation that the tree was
//! unmodified: a binary built from an edited tree advertises the commit it was edited on
//! top of. That is deliberate — the build script re-runs when `HEAD`, the refs, or the
//! shallow boundary move, appear, or disappear ([`derivation::watch_candidates`]), never
//! on an edit or a `git status`, so the identity follows the commit and the ordinary
//! edit→test loop does not relink. Equality with a tarball's `VERSION` is a claim about
//! a `dist`-built PAIR; a local `cargo build` produces no tarball to be unequal to.
//!
//! On a checkout with no tag reachable the identity has the `0.0.0+git.<sha>` shape: a
//! real build identity, not a release version.

pub mod derivation;

// Unix-only, like the build script's appearance watch they exercise (and every CI runner).
#[cfg(all(test, unix))]
mod build_script_tests;

/// The build identity of this binary — see the module docs for how it is resolved and
/// what it does and does not claim. Recorded as the `version` field of every `wyrd s3`
/// `role started` event.
pub const BUILD_IDENTITY: &str = env!("WYRD_BUILD_IDENTITY");
