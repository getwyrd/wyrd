//! Tier-1 disk-fault harness — real-device scenario (M3, issue #195, proposal 0005
//! §13.2, `0005:405-408`). Born at M3 per the verification-posture forcing function
//! ("deferred ≠ unbuilt — the #146 Tier-1/2 gap").
//!
//! This test exercises what no in-memory DST campaign can: the **production**
//! `FsChunkStore` / `reconcile_step` / `ScrubContext` / `ReconstructionContext`
//! path against **real** block-layer misbehaviour via device-mapper `dm-error`.
//! The Tier-0 DST campaign (`crates/dst/tests/custodian.rs`) simulates bit rot and
//! fragment loss in-process; this test drives the **same fenced control point**
//! (`reconcile_step`) over a real ext4 filesystem mounted on a dm-backed device that
//! the harness switches to `dm-error` mid-test.
//!
//! The scenario proves **both** custodian legs over a real block device:
//!
//! 1. **Scrub leg**: a fragment with injected on-disk bit rot (a real byte-flip in the
//!    stored file, done while the dm device is healthy/linear) is detected by
//!    `scrub::reconcile` via `FsChunkStore::get_fragment` returning an `IntegrityFault`
//!    (`wyrd_traits::is_integrity_fault`), and the chunk is enqueued for repair on the
//!    shared repair queue. Causality-verified: after the scrub pass the repair queue
//!    must contain the faulted chunk's id.
//!
//! 2. **Reconstruction leg**: the dm device is switched to `dm-error` (every read
//!    returns `EIO`) and the page cache is **mandatorily evicted** (if caches are still
//!    warm the scenario aborts — it must exercise the block-layer EIO path, not the
//!    cached-integrity-fault path). `reconstruction::reconcile` rebuilds the faulted
//!    chunk from the ≥ `k` surviving fragments, re-placing it on a healthy D server.
//!    The block-layer `EIO` is classified as a permanent read fault via
//!    `reconstruction::is_permanent_read_fault` (the production fix from issue #251)
//!    and read around. After reconstruction the chunk is at **full redundancy** and no
//!    read error was propagated to the caller (`reconcile_step` returned `Ok`).
//!
//! **Gating policy (ADR-0016):** `#[ignore]`d so `cargo test --workspace` compiles and
//! type-checks this test (proving it is real API-bound Rust, not inert dispatch
//! scaffolding) but never runs it in the unprivileged, container-free `cargo xtask ci`
//! gate. The privileged run (root + `dmsetup`) is performed by `cargo xtask disk-faults`
//! in the dedicated off-Check Tier-1 CI job (`tier1-disk-faults.yml`), opted in via
//! `WYRD_TIER1=1`.
//!
//! **Depends-on-merged: #251** — the `is_permanent_read_fault` read-around in
//! `reconstruction.rs` must be present for the reconstruction leg to pass; without it,
//! `reconcile_step` returns `Err` on the EIO and the campaign aborts.
//!
//! Fleet topology (RS(2,1), n=3 fragments):
//! - D server 0 (domain A): healthy `FsChunkStore` in a tmpdir — holds fragment 0.
//! - D server 1 (domain B): `FsChunkStore` on an ext4 filesystem mounted on the
//!   dm-backed device — holds fragment 1 (the faulted one).
//! - D server 2 (domain C): healthy `FsChunkStore` in a tmpdir — holds fragment 2.
//! - D server 3 (domain D): healthy `FsChunkStore` in a tmpdir — the re-placement
//!   target for the rebuilt fragment after reconstruction.

use std::collections::HashMap;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use wyrd_chunk_format::CORE_HEADER_LEN;
use wyrd_chunkstore_fs::{fragment_path, FsChunkStore};
use wyrd_coordination_mem::MemCoordination;
use wyrd_core::metadata::{self, ChunkRef, EcScheme, InodeId, InodeRecord, InodeState};
use wyrd_core::placement::Topology;
use wyrd_core::repair;
use wyrd_core::write::plan_write;
use wyrd_custodian::{
    reconcile_step, Custodian, FencedZone, Reconciled, ReconstructionContext, ScrubContext,
};
use wyrd_traits::{
    ChunkId, ChunkStore, CommitOutcome, DServerId, FragmentId, MetadataStore, Result, WriteBatch,
};

// ─── RS(2,1) parameters ───────────────────────────────────────────────────────

/// Reed-Solomon RS(2,1): k=2 data shards, m=1 parity shard, n=3 total fragments.
const K: usize = 2;
const M: usize = 1;
const N: usize = K + M; // 3

const CHUNK_ID: ChunkId = 0xDEAD_BEEF_CAFE_1950;
const INODE_ID: InodeId = 1;
const ROOT: InodeId = 0;

// ─── In-memory metadata store ─────────────────────────────────────────────────

/// A simple in-memory [`MetadataStore`] for the scenario (same pattern as other
/// custodian integration tests in this directory).
#[derive(Default)]
struct MemMeta {
    kv: Mutex<HashMap<Vec<u8>, Bytes>>,
}

#[async_trait]
impl MetadataStore for MemMeta {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(self.kv.lock().unwrap().get(key).cloned())
    }

    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        Ok(self
            .kv
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn commit(&self, batch: WriteBatch) -> Result<CommitOutcome> {
        let mut kv = self.kv.lock().unwrap();
        for pre in &batch.preconditions {
            if kv.get(&pre.key).cloned() != pre.expected {
                return Ok(CommitOutcome::Conflict);
            }
        }
        for (k, v) in batch.puts {
            kv.insert(k, v);
        }
        for k in batch.deletes {
            kv.remove(&k);
        }
        Ok(CommitOutcome::Committed)
    }
}

// ─── Privileged infrastructure helpers ───────────────────────────────────────

/// Whether the harness must escalate privileged calls via `sudo`.
///
/// The Tier-1 scenario performs root-only I/O (`losetup`, `mount`, `mkfs.ext4`
/// on a dm device, `dmsetup`, and writing `/proc/sys/vm/drop_caches`). On the
/// GitHub-hosted `ubuntu-latest` runner the job runs as the unprivileged
/// `runner` user with passwordless `sudo`, so every privileged call must be
/// wrapped (issue: Codex PR #270). When already root (`sudo cargo …` or a
/// privileged container) we skip `sudo` — that also avoids requiring `sudo` to
/// be installed in a bare root image.
///
/// Cached: `id -u` is POSIX and always present, which avoids pulling a
/// `libc`/`nix` dependency into the harness just for one euid check.
fn needs_sudo() -> bool {
    static NEEDS_SUDO: OnceLock<bool> = OnceLock::new();
    *NEEDS_SUDO.get_or_init(|| {
        Command::new("id")
            .arg("-u")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim() != "0")
            .unwrap_or(true)
    })
}

/// Build a `Command` for a root-only `program`, escalating via `sudo` when the
/// harness is not already running as root. Centralises the escalation so every
/// privileged call site (and the teardown guard) goes through one place.
fn privileged(program: &str, args: &[&str]) -> Command {
    let mut cmd = if needs_sudo() {
        let mut c = Command::new("sudo");
        c.arg(program);
        c
    } else {
        Command::new(program)
    };
    cmd.args(args);
    cmd
}

/// Run a privileged command, panicking with the command's stderr on failure.
fn must_run(program: &str, args: &[&str]) {
    let out = privileged(program, args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `{program}`: {e}"));
    assert!(
        out.status.success(),
        "`{program} {}` failed (exit {}):\n{}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
}

/// Run a privileged command for its stdout (trimmed), panicking on failure.
fn must_run_stdout(program: &str, args: &[&str]) -> String {
    let out = privileged(program, args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `{program}`: {e}"));
    assert!(
        out.status.success(),
        "`{program} {}` failed (exit {}):\n{}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Run a command **without** sudo escalation, returning its trimmed stdout.
///
/// Used to read the invoking user's real uid/gid: routing `id` through
/// [`privileged`] would run `sudo id` and report root's `0:0`, defeating the
/// post-mount `chown` that hands the mount point back to the unprivileged test
/// user.
fn unprivileged_stdout(program: &str, args: &[&str]) -> String {
    let out = Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn `{program}`: {e}"));
    assert!(
        out.status.success(),
        "`{program} {}` failed (exit {}):\n{}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stderr).trim()
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// RAII guard that tears down the dm device, unmounts the filesystem, and
/// detaches the loop device on `drop` — cleanup runs even if the test panics.
struct DmGuard {
    device_name: String,
    loop_dev: String,
    mount_point: String,
}

impl Drop for DmGuard {
    fn drop(&mut self) {
        // Best-effort teardown: errors are printed but never panic here (we may
        // already be panicking). Each step is attempted unconditionally.
        let _ = privileged("umount", &[&self.mount_point]).output();
        let _ = privileged("dmsetup", &["remove", "--force", &self.device_name]).output();
        let _ = privileged("losetup", &["-d", &self.loop_dev]).output();
    }
}

// ─── dm table helpers (inlined from xtask::disk_faults to avoid cross-crate dep) ─

/// `dmsetup` table string for a linear passthrough target (the healthy phase).
fn dm_table_linear(sectors: u64, device: &str) -> String {
    format!("0 {sectors} linear {device} 0")
}

/// `dmsetup` table string for an error target (all I/O returns EIO).
fn dm_table_error(sectors: u64) -> String {
    format!("0 {sectors} error")
}

// ─── Scenario test ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Tier-1: needs root + device-mapper — run via cargo xtask disk-faults"]
async fn disk_fault_drives_custodian_to_full_redundancy_with_no_read_errors() {
    // ── 0. Constants ─────────────────────────────────────────────────────────
    const DEVICE_NAME: &str = "wyrd-tier1-fault";
    const BACKING_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB
    let sectors = BACKING_SIZE / 512;

    // ── 1. Create work directory and backing file ─────────────────────────────
    let work_dir = tempfile::tempdir().expect("tempdir for tier1 scenario");
    let backing_file = work_dir.path().join("backing.img");
    let mount_point = work_dir.path().join("dm-mount");
    std::fs::create_dir_all(&mount_point).expect("create dm-mount directory");

    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&backing_file)
            .expect("create backing file");
        f.set_len(BACKING_SIZE).expect("set backing file length");
    }
    let backing_path = backing_file.to_string_lossy().to_string();
    let mount_path = mount_point.to_string_lossy().to_string();

    // ── 2. Attach loop device ─────────────────────────────────────────────────
    let loop_dev = must_run_stdout("losetup", &["-f", "--show", &backing_path]);
    eprintln!("tier1: loop device: {loop_dev}");

    // ── 3. Create dm device (linear) + ext4 + mount ───────────────────────────
    let linear_table = dm_table_linear(sectors, &loop_dev);
    must_run(
        "dmsetup",
        &["create", DEVICE_NAME, "--table", &linear_table],
    );
    let dm_path = format!("/dev/mapper/{DEVICE_NAME}");

    // RAII guard — runs cleanup on completion or panic.
    let _guard = DmGuard {
        device_name: DEVICE_NAME.to_string(),
        loop_dev: loop_dev.clone(),
        mount_point: mount_path.clone(),
    };

    must_run(
        "mkfs.ext4",
        &["-F", "-q", "-E", "lazy_itable_init=0", &dm_path],
    );
    must_run("mount", &[&dm_path, &mount_path]);
    // `mount` ran as root (via `sudo` on the CI runner), so the freshly-created
    // ext4 root is owned by `root:root`. The `FsChunkStore` for d1 below runs as
    // the unprivileged test user and must create its `store/` subtree under this
    // mount point — without handing ownership back it fails with EACCES
    // (issue: the sudo-escalation in PR #270 left the mount root root-owned).
    // When already root (`needs_sudo()` is false) the test process owns the
    // mount and there is nothing to do.
    if needs_sudo() {
        let uid = unprivileged_stdout("id", &["-u"]);
        let gid = unprivileged_stdout("id", &["-g"]);
        must_run("chown", &[&format!("{uid}:{gid}"), &mount_path]);
    }
    eprintln!("tier1: dm device {dm_path} mounted at {mount_path}");

    // ── 4. Open D servers ─────────────────────────────────────────────────────
    // d0 (server 0, domain A): healthy FsChunkStore in a tmpdir.
    let d0_dir = tempfile::tempdir().expect("d0 tempdir");
    let d0 = FsChunkStore::open(d0_dir.path()).expect("open d0");
    // d1 (server 1, domain B): FsChunkStore on the dm-backed ext4 filesystem.
    let d1_root = mount_point.join("store");
    let d1 = FsChunkStore::open(d1_root.as_path()).expect("open d1 on dm-backed fs");
    // d2 (server 2, domain C): healthy FsChunkStore in a tmpdir.
    let d2_dir = tempfile::tempdir().expect("d2 tempdir");
    let d2 = FsChunkStore::open(d2_dir.path()).expect("open d2");
    // d3 (server 3, domain D): healthy FsChunkStore in a tmpdir (re-placement target).
    let d3_dir = tempfile::tempdir().expect("d3 tempdir");
    let d3 = FsChunkStore::open(d3_dir.path()).expect("open d3");

    // ── 5. Write RS(2,1) chunk: fragment 0→d0, 1→d1(dm), 2→d2 ───────────────
    let data = b"tier1-disk-fault-scenario: every byte of this object must survive \
                 a real block-layer EIO during reconstruction. The custodian must \
                 detect the fault, read around it, and drive the chunk back to full \
                 RS(2,1) redundancy with no read errors propagated to the caller."
        .to_vec();
    assert!(
        data.len() > K,
        "payload too short for RS({K},{M}): need > {K} bytes, got {}",
        data.len()
    );

    // Encode the object into RS(2,1) fragments using the production write path.
    let plan = plan_write(
        &data,
        data.len(), // one chunk — entire object in one piece
        EcScheme::ReedSolomon {
            k: K as u8,
            m: M as u8,
        },
        || CHUNK_ID,
    )
    .expect("RS(2,1) encode via plan_write");
    assert_eq!(plan.chunks.len(), 1, "single-chunk object");
    assert_eq!(plan.chunks[0].id, CHUNK_ID);
    let chunk_plan = &plan.chunks[0];
    assert_eq!(
        chunk_plan.fragments.len(),
        N,
        "RS(2,1) must produce {N} fragments"
    );

    // Write each fragment to its designated D server.
    // plan_write produces fragments as Vec<(u16, Bytes)> where u16 is the index.
    let stores_for_write: [&FsChunkStore; N] = [&d0, &d1, &d2];
    for (index, frag_bytes) in &chunk_plan.fragments {
        let i = *index as usize;
        let frag_id = FragmentId {
            chunk: CHUNK_ID,
            index: *index,
        };
        // put_fragment verifies the checksum — valid fragments from plan_write pass.
        stores_for_write[i]
            .put_fragment(frag_id, frag_bytes.clone())
            .await
            .unwrap_or_else(|e| panic!("put_fragment index {i} failed: {e}"));
    }
    eprintln!("tier1: wrote {N} fragments — frag 0→d0, frag 1→d1(dm), frag 2→d2");

    // Flush all pending writes to disk.
    must_run("sync", &[]);

    // ── 6. Commit inode metadata ───────────────────────────────────────────────
    let meta = MemMeta::default();
    let inode = InodeRecord {
        size: data.len() as u64,
        chunk_map: vec![ChunkRef {
            id: CHUNK_ID,
            scheme: EcScheme::ReedSolomon {
                k: K as u8,
                m: M as u8,
            },
            len: data.len() as u64,
            placement: vec![0, 1, 2], // fragment i → D-server i
        }],
        state: InodeState::Committed,
        version: 1,
    };
    let outcome = metadata::create(&meta, ROOT, "tier1-obj", INODE_ID, &inode)
        .await
        .expect("metadata::create");
    assert_eq!(outcome, CommitOutcome::Committed, "metadata committed");

    // ── 7. Inject bit rot: flip a payload byte in d1's on-disk fragment ────────
    // Flip one byte in the fragment's payload region (past the core header).
    // The dm device is still linear (healthy), so the write succeeds. The corruption
    // is on-disk; the scrub leg will read the corrupt bytes and detect the checksum
    // mismatch (IntegrityFault). FsChunkStore.put_fragment verifies on write, so we
    // must flip the bytes directly in the on-disk file, bypassing the store API.
    let frag1_id = FragmentId {
        chunk: CHUNK_ID,
        index: 1,
    };
    let frag1_path = fragment_path(d1_root.as_path(), frag1_id);
    let mut frag1_bytes = std::fs::read(&frag1_path).expect("read fragment 1 for bit-flip");
    let flip_offset = CORE_HEADER_LEN as usize; // first payload byte (past the core header)
    assert!(
        flip_offset < frag1_bytes.len(),
        "fragment 1 too short for bit-flip: len={} core_header={}",
        frag1_bytes.len(),
        CORE_HEADER_LEN
    );
    frag1_bytes[flip_offset] ^= 0xff;
    std::fs::write(&frag1_path, &frag1_bytes).expect("write corrupt fragment 1");
    must_run("sync", &[]); // flush corrupt bytes to disk
    eprintln!("tier1: injected bit rot at fragment 1 offset {flip_offset}");

    // ── 8. Drop page caches (mandatory for scrub leg) ─────────────────────────
    // Scrub must READ d1's fragment FROM DISK (through dm-linear → the corrupt bytes)
    // to detect the corruption. If the page cache still holds the pre-flip clean bytes
    // (from put_fragment), FsChunkStore returns Ok(clean_bytes) and scrub does NOT
    // detect the bit rot. Cache eviction is MANDATORY.
    // Privileged write — must go through `sudo` on the unprivileged CI runner, so
    // the redirection runs in the root shell rather than `std::fs::write` (which
    // would hit EACCES as the `runner` user).
    must_run("sh", &["-c", "echo 3 > /proc/sys/vm/drop_caches"]);
    eprintln!("tier1: page caches dropped before scrub");

    // Pre-condition: d1's fragment must return IntegrityFault after the byte-flip.
    let preflight_scrub = d1.get_fragment(frag1_id).await;
    assert!(
        preflight_scrub.is_err(),
        "pre-scrub preflight: d1's fragment must return an error after byte-flip + cache drop"
    );
    assert!(
        wyrd_traits::is_integrity_fault(preflight_scrub.unwrap_err().as_ref()),
        "pre-scrub preflight: error must be IntegrityFault (not EIO) — dm is still linear"
    );
    eprintln!("tier1: pre-scrub preflight PASS — d1 returns IntegrityFault");

    // ── 9. Elect custodian ─────────────────────────────────────────────────────
    let coord = MemCoordination::new();
    let leader = Custodian::elect(&coord, "zone-tier1")
        .await
        .expect("elect custodian");
    let mut zone = FencedZone::new();
    zone.install(leader.leadership());

    // ── 10. SCRUB LEG ─────────────────────────────────────────────────────────
    // scrub::reconcile walks each store, calls get_fragment on d1's fragment,
    // receives IntegrityFault, and enqueues a repair obligation on the shared queue.
    let scrub_fleet: [(DServerId, &dyn ChunkStore); 3] = [(0, &d0), (1, &d1), (2, &d2)];
    let scrub_ctx = ScrubContext {
        meta: &meta,
        fleet: &scrub_fleet,
    };
    let scrub_outcome = reconcile_step(&zone, &leader, None, Some(&scrub_ctx), None, None, 0)
        .await
        .expect(
            "scrub reconcile_step must succeed: d1's IntegrityFault is a permanent fault that \
             scrub enqueues and continues past — it must NOT abort the scrub pass",
        );
    assert_eq!(
        scrub_outcome,
        Reconciled::Changed,
        "scrub must report Changed (detected bit rot in d1's fragment and enqueued repair)"
    );

    // Causal verdict: repair queue must contain CHUNK_ID.
    let queue_after_scrub = repair::queued_repairs(&meta)
        .await
        .expect("queued_repairs after scrub");
    assert!(
        !queue_after_scrub.is_empty(),
        "scrub must have enqueued at least one repair; queue is empty — scrub was causally inert"
    );
    assert!(
        queue_after_scrub.contains(&CHUNK_ID),
        "repair queue must contain CHUNK_ID {CHUNK_ID:#x} after scrub; \
         queue: {queue_after_scrub:?}"
    );
    eprintln!(
        "tier1 SCRUB LEG PASS: {} obligation(s) enqueued, queue contains CHUNK_ID",
        queue_after_scrub.len()
    );

    // ── 11. Switch dm device to ERROR ─────────────────────────────────────────
    let error_table = dm_table_error(sectors);
    must_run("dmsetup", &["suspend", DEVICE_NAME]);
    must_run("dmsetup", &["load", DEVICE_NAME, "--table", &error_table]);
    must_run("dmsetup", &["resume", DEVICE_NAME]);
    eprintln!("tier1: dm device {DEVICE_NAME} switched to error (all I/O → EIO)");

    // ── 12. Drop page caches (mandatory for reconstruction EIO path) ─────────
    // CRITICAL: if the page cache still holds d1's fragment bytes (cached during the
    // scrub pass get_fragment call that returned IntegrityFault), reconstruction's
    // get_fragment on d1 would return IntegrityFault again — not the block-layer EIO
    // from dm-error. The scenario MUST exercise the EIO → is_permanent_read_fault
    // path (issue #251), not just the already-covered IntegrityFault path.
    // Privileged write (see the scrub-leg drop above) — escalated via `sudo`.
    must_run("sh", &["-c", "echo 3 > /proc/sys/vm/drop_caches"]);
    eprintln!("tier1: page caches dropped before reconstruction");

    // Pre-condition: d1 must return block-layer EIO (NOT IntegrityFault) after
    // dm-error + cache drop. If we get IntegrityFault, the page cache is still warm.
    let preflight_recon = d1.get_fragment(frag1_id).await;
    assert!(
        preflight_recon.is_err(),
        "reconstruction preflight: d1 must return an error after dm-error + cache drop"
    );
    let recon_err = preflight_recon.unwrap_err();
    assert!(
        !wyrd_traits::is_integrity_fault(recon_err.as_ref()),
        "reconstruction preflight: error must be block-layer EIO, NOT IntegrityFault — \
         page cache still warm; ensure drop_caches evicted d1's data: {recon_err}"
    );
    // Walk the source chain for EIO (POSIX errno 5).
    const EIO: i32 = 5;
    let has_eio = {
        let mut next = Some(recon_err.as_ref() as &dyn std::error::Error);
        loop {
            let Some(e) = next else { break false };
            if let Some(io) = e.downcast_ref::<std::io::Error>() {
                if io.raw_os_error() == Some(EIO) {
                    break true;
                }
            }
            next = e.source();
        }
    };
    assert!(
        has_eio,
        "reconstruction preflight: error must carry EIO (raw_os_error={EIO}) in its source \
         chain — the dm-error device must be returning block-layer faults; got: {recon_err}"
    );
    eprintln!("tier1: reconstruction preflight PASS — d1 returns block-layer EIO");

    // ── 13. RECONSTRUCTION LEG ────────────────────────────────────────────────
    // Fleet for reconstruction: d0, d1 (dm-error → EIO), d2, d3.
    // D1 is INCLUDED so reconstruction::assess calls get_fragment on it, receives
    // EIO, and exercises is_permanent_read_fault (issue #251), reading around it.
    //
    // Topology for reconstruction: A (server 0), C (server 2), D (server 3).
    // Server 1 (domain B) is NOT registered in the topology — the selector excludes
    // it from re-placement targets. After excluding the survivor domains A and C,
    // only D (server 3 = d3, the healthy re-placement target) remains.
    let mut recon_topo = Topology::default();
    recon_topo
        .register(0, "A")
        .register(2, "C")
        .register(3, "D");

    let recon_fleet: [(DServerId, &dyn ChunkStore); 4] = [(0, &d0), (1, &d1), (2, &d2), (3, &d3)];
    let recon_ctx = ReconstructionContext {
        meta: &meta,
        fleet: &recon_fleet,
        topology: &recon_topo,
        unreachable: &[],
    };

    let recon_result =
        reconcile_step(&zone, &leader, None, None, Some(&recon_ctx), None, 1_000).await;

    // The reconcile_step must succeed: EIO on d1 must be classified as a permanent
    // read fault via is_permanent_read_fault (issue #251) and read around — NOT
    // propagated to the caller as Err.
    let recon_outcome = recon_result.unwrap_or_else(|e| {
        panic!(
            "reconstruction reconcile_step returned Err — block-layer EIO was NOT read around.\n\
             Error: {e}\n\
             Ensure is_permanent_read_fault (issue #251) is present in \
             crates/custodian/src/reconstruction.rs."
        )
    });
    assert_eq!(
        recon_outcome,
        Reconciled::Changed,
        "reconstruction must report Changed (faulted chunk rebuilt from survivors d0 + d2)"
    );

    // ── 14. Verify reconstruction verdict ─────────────────────────────────────
    // The repair obligation must be drained from the shared queue.
    let queue_after_recon = repair::queued_repairs(&meta)
        .await
        .expect("queued_repairs after reconstruction");
    assert!(
        queue_after_recon.is_empty(),
        "reconstruction must drain the repair obligation; \
         queue still contains: {queue_after_recon:?}"
    );

    // Inode version must have bumped by exactly 1 (one version-conditional commit).
    let inode_key = metadata::inode_key(INODE_ID);
    let inode_bytes = meta
        .get(&inode_key)
        .await
        .expect("get inode after reconstruction")
        .expect("inode must exist after reconstruction");
    let updated: InodeRecord = metadata::decode(&inode_bytes).expect("decode updated inode");
    assert_eq!(
        updated.version, 2,
        "exactly one version-conditional commit (inode version 1→2)"
    );

    // Fragment 1 (rebuilt) must have been re-placed on d3 (server 3, domain D).
    assert_eq!(
        updated.chunk_map[0].placement[1], 3,
        "rebuilt fragment 1 must be re-placed on d3 (server 3, domain D); \
         actual placement: {:?}",
        updated.chunk_map[0].placement
    );
    eprintln!(
        "tier1: inode at version {}, placement: {:?}",
        updated.version, updated.chunk_map[0].placement
    );

    // ── 15. Verify full redundancy ─────────────────────────────────────────────
    // All n=3 fragments must be present and intact after reconstruction.
    // Fragment 0 on d0, fragment 1 (rebuilt) on d3, fragment 2 on d2.
    // D1 is dm-error — we do NOT attempt to read from it.
    let mut intact = 0_usize;

    let frag0 = FragmentId {
        chunk: CHUNK_ID,
        index: 0,
    };
    let bytes0 = d0
        .get_fragment(frag0)
        .await
        .expect("get fragment 0 from d0")
        .expect("fragment 0 must be present on d0");
    assert!(
        repair::fragment_intact(&bytes0, CHUNK_ID),
        "fragment 0 on d0 must verify checksum"
    );
    intact += 1;

    let frag1_rebuilt = FragmentId {
        chunk: CHUNK_ID,
        index: 1,
    };
    let bytes1 = d3
        .get_fragment(frag1_rebuilt)
        .await
        .expect("get rebuilt fragment 1 from d3")
        .expect("rebuilt fragment 1 must be present on d3");
    assert!(
        repair::fragment_intact(&bytes1, CHUNK_ID),
        "rebuilt fragment 1 on d3 must verify checksum"
    );
    intact += 1;

    let frag2 = FragmentId {
        chunk: CHUNK_ID,
        index: 2,
    };
    let bytes2 = d2
        .get_fragment(frag2)
        .await
        .expect("get fragment 2 from d2")
        .expect("fragment 2 must be present on d2");
    assert!(
        repair::fragment_intact(&bytes2, CHUNK_ID),
        "fragment 2 on d2 must verify checksum"
    );
    intact += 1;

    assert_eq!(
        intact, N,
        "full redundancy: expected {N} fragments intact, got {intact}"
    );
    eprintln!(
        "tier1: CAMPAIGN PASS — faulted chunk driven to full redundancy ({intact}/{N} intact) \
         with no read errors propagated during repair."
    );
}
