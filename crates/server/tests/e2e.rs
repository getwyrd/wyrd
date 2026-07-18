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

/// A **buffered** PUT is a content publication like any other (ADR-0047): it stamps
/// the digest and the publication instant, so the S3 surface serves an ETag and
/// Last-Modified for objects written through `Gateway::put_object` too — and a
/// buffered OVERWRITE of a metadata-carrying object re-stamps rather than either
/// serving the prior version's digest for the new bytes or silently dropping the
/// trio (Codex review: the buffered path committed the plan's default all-`None`
/// metadata). No content type is declared on this path, so `content_type` stays
/// `None` and GET falls back to the S3 default.
///
/// `#[tokio::test]` (not `pollster`): `get_object_streaming` spawns its chunk-reader
/// task onto the runtime.
#[tokio::test]
async fn buffered_put_stamps_publication_metadata() {
    use sha2::{Digest, Sha256};
    use std::sync::Arc;
    use wyrd_gateway_core::ObjectGateway;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    let (gw, _dir) = gateway();
    gw.put_object("obj", b"buffered bytes").await.unwrap();

    let gw = Arc::new(gw);
    let read = Arc::clone(&gw)
        .get_object_streaming("obj")
        .await
        .unwrap()
        .expect("the object just written reads back");
    assert_eq!(
        read.etag.as_deref(),
        Some(hex(&Sha256::digest(b"buffered bytes")).as_str()),
        "a buffered PUT stamps the content digest as the ETag (ADR-0047)"
    );
    assert!(
        read.modified.is_some(),
        "a buffered PUT stamps the publication time"
    );
    assert_eq!(
        read.content_type, None,
        "no content type is declared on the buffered path"
    );

    // A buffered overwrite is a FRESH publication: the digest tracks the new bytes.
    gw.put_object("obj", b"replaced bytes").await.unwrap();
    let read = Arc::clone(&gw)
        .get_object_streaming("obj")
        .await
        .unwrap()
        .expect("the overwritten object reads back");
    assert_eq!(
        read.etag.as_deref(),
        Some(hex(&Sha256::digest(b"replaced bytes")).as_str()),
        "a buffered overwrite re-stamps the ETag for the new content"
    );
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
