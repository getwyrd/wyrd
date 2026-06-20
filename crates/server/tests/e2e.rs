//! End-to-end test of the M0 walking skeleton: an S3 PUT through the four-phase
//! commit, then a GET back, byte-identical, in one process — against the real
//! redb + filesystem backends the gateway composes.

use pollster::block_on;
use wyrd_chunkstore_fs::FsChunkStore;
use wyrd_coordination_mem::MemCoordination;
use wyrd_metadata_redb::RedbMetadataStore;
use wyrd_server::Gateway;

/// A gateway over in-memory redb + a temp-dir chunk store + in-memory
/// coordination, with a small chunk size so tests exercise multi-chunk objects.
fn gateway() -> (
    Gateway<RedbMetadataStore, FsChunkStore, MemCoordination>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("temp dir");
    let gateway = Gateway::new(
        RedbMetadataStore::in_memory().expect("redb"),
        FsChunkStore::open(dir.path()).expect("fs store"),
        MemCoordination::new(),
    )
    .with_chunk_size(4);
    (gateway, dir)
}

#[test]
fn put_then_get_is_byte_identical() {
    block_on(async {
        let (gw, _dir) = gateway();
        for (key, data) in [
            ("empty", &b""[..]),
            ("small", &b"abc"[..]),
            ("multi", &b"an object that spans several chunks"[..]),
        ] {
            gw.put_object(key, data).await.unwrap();
            assert_eq!(
                gw.get_object(key).await.unwrap().as_deref(),
                Some(data),
                "{key}: PUT/GET must be byte-identical"
            );
        }

        // Overwrite returns the new bytes; an unknown key returns None.
        gw.put_object("small", b"replaced").await.unwrap();
        assert_eq!(
            gw.get_object("small").await.unwrap().as_deref(),
            Some(&b"replaced"[..])
        );
        assert!(gw.get_object("absent").await.unwrap().is_none());
    });
}

#[test]
fn gateway_composes_the_coordination_backend() {
    block_on(async {
        let (gw, _dir) = gateway();
        assert!(gw.nodes().await.unwrap().is_empty());
        gw.announce("node-1").await.unwrap();
        let nodes = gw.nodes().await.unwrap();
        assert!(
            nodes.iter().any(|n| n.as_ref() == b"node-1"),
            "the announced node must be discoverable"
        );
    });
}

#[test]
fn binary_runs_the_round_trip_in_one_process() {
    // `wyrd demo` is the in-memory PUT/GET round trip in one process.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_wyrd"))
        .arg("demo")
        .output()
        .expect("run the wyrd binary");
    assert!(
        output.status.success(),
        "wyrd exited with {:?}",
        output.status
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("round-trip ok"),
        "unexpected output: {stdout}"
    );
}
