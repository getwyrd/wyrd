//! End-to-end test of the `wyrd` CLI (#90): drive the compiled binary across
//! *separate* invocations sharing a `--data-dir`, so the persisted on-disk
//! backends (redb + filesystem) and the persisted inode allocator carry state
//! from one process to the next.

#![forbid(unsafe_code)]

use std::path::Path;
use std::process::{Command, Output};

const WYRD: &str = env!("CARGO_BIN_EXE_wyrd");

fn run(args: &[&str]) -> Output {
    Command::new(WYRD)
        .args(args)
        .output()
        .expect("run the wyrd binary")
}

fn data_dir_arg(dir: &Path) -> String {
    dir.to_str().expect("utf-8 path").to_string()
}

#[test]
fn put_then_get_round_trips_across_separate_invocations() {
    let work = tempfile::tempdir().expect("temp dir");
    let data_dir = data_dir_arg(work.path());
    let input = work.path().join("input.bin");
    let output = work.path().join("output.bin");
    // Larger than the chunk size below, so the object spans several chunks.
    let payload = b"the wyrd cli round-trips an object across two processes".repeat(4);
    std::fs::write(&input, &payload).unwrap();

    // PUT (one process).
    let put = run(&[
        "put",
        input.to_str().unwrap(),
        "--key",
        "obj/one",
        "--data-dir",
        &data_dir,
        "--chunk-size",
        "16",
    ]);
    assert!(put.status.success(), "put failed: {put:?}");

    // GET (a separate process) reads it back byte-identical.
    let get = run(&[
        "get",
        "obj/one",
        "--data-dir",
        &data_dir,
        "--out",
        output.to_str().unwrap(),
    ]);
    assert!(get.status.success(), "get failed: {get:?}");
    assert_eq!(
        std::fs::read(&output).unwrap(),
        payload,
        "round-trip must be byte-identical"
    );

    // A second object gets a distinct (persisted) inode id and round-trips too.
    let other = work.path().join("two.bin");
    std::fs::write(&other, b"second object").unwrap();
    assert!(run(&[
        "put",
        other.to_str().unwrap(),
        "--key",
        "obj/two",
        "--data-dir",
        &data_dir
    ])
    .status
    .success());
    let got_two = run(&["get", "obj/two", "--data-dir", &data_dir]);
    assert!(got_two.status.success());
    assert_eq!(got_two.stdout, b"second object");
}

#[test]
fn get_of_a_missing_key_exits_nonzero() {
    let work = tempfile::tempdir().expect("temp dir");
    let got = run(&["get", "absent", "--data-dir", &data_dir_arg(work.path())]);
    assert!(!got.status.success(), "a missing key must exit non-zero");
    assert!(
        String::from_utf8_lossy(&got.stderr).contains("not found"),
        "the diagnostic must go to stderr"
    );
}

#[test]
fn re_putting_an_existing_key_exits_nonzero() {
    let work = tempfile::tempdir().expect("temp dir");
    let data_dir = data_dir_arg(work.path());
    let input = work.path().join("input.bin");
    std::fs::write(&input, b"once").unwrap();

    assert!(run(&[
        "put",
        input.to_str().unwrap(),
        "--key",
        "dup",
        "--data-dir",
        &data_dir
    ])
    .status
    .success());
    let again = run(&[
        "put",
        input.to_str().unwrap(),
        "--key",
        "dup",
        "--data-dir",
        &data_dir,
    ]);
    assert!(
        !again.status.success(),
        "re-putting an existing key must fail"
    );
    assert!(String::from_utf8_lossy(&again.stderr).contains("already exists"));
}

#[test]
fn demo_exits_zero() {
    let demo = run(&["demo"]);
    assert!(demo.status.success(), "demo failed: {demo:?}");
    assert!(String::from_utf8_lossy(&demo.stdout).contains("round-trip ok"));
}

#[test]
fn no_subcommand_prints_usage_and_exits_two() {
    let out = run(&[]);
    assert_eq!(out.status.code(), Some(2), "no subcommand is a usage error");
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage:"));
}
