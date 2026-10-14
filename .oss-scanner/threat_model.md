# Threat model: Wyrd

Scanner-facing summary for Anthropic's OSS Scanner (issue #890). The full, living threat
model is [architecture §14](../docs/design/architecture/14-threat-model.md); where this file
and §14 differ about what exists in code today, this file describes the code, and §14
describes the design it is growing into.

## What this project does

Wyrd is a distributed file store written in Rust: metadata and bulk data are kept apart,
and every write commits atomically at one point (the metadata store's version counter).
Files are split into chunks, erasure-coded (Reed-Solomon) into fragments, and stored on
"D servers". The project is pre-release (milestones M0–M3 done; see `README.md`): it is
not deployed anywhere and carries no stability promise, but the code paths below are real
and tested.

One binary, `wyrd` (`crates/server`), runs every role:

- `wyrd s3`: the S3-compatible HTTP gateway (`crates/gateway-s3`). Object PUT/GET/DELETE
  with SigV4 header auth on every request, including `aws-chunked` streaming uploads with
  per-chunk and trailer signatures and `x-amz-checksum-*` trailers. It takes one access
  key, set on the command line; bucket and key are joined into one flat object name
  (`"{bucket}/{key}"`).
- `wyrd d-server`: a storage node serving the gRPC `ChunkStore` service
  (`crates/chunkstore-grpc`, `crates/proto/proto/wyrd/v0/chunk.proto`) over a local
  filesystem store (`crates/chunkstore-fs`).
- `wyrd custodian`: the maintenance plane (`crates/custodian`): garbage collection, scrub,
  reconstruction, rebalance.
- `wyrd put` / `wyrd get`: a local client.

A second binary, `wyrd-validate` (`crates/validate`), is a blackbox test client that talks
to a gateway over S3. It is a test tool, not a server.

The client and commit protocol live in `crates/core`. Metadata backends are `redb`
(embedded, default), FoundationDB (`crates/metadata-fdb`, feature `fdb`) and TiKV
(`crates/metadata-tikv`, feature `tikv`). Coordination (leases, discovery, fencing) is
in-memory or etcd (`crates/coordination-etcd`, feature `etcd`).

## Where untrusted input enters

Treat all of the following as attacker-controlled:

1. **Everything sent to the S3 gateway**, from any network peer, before and after
   authentication: the request line, path (bucket and key), query string, headers, the
   SigV4 `Authorization` header and its parsing, `x-amz-content-sha256`, the `aws-chunked`
   framing, chunk signatures, trailers and checksums, and the body. The gateway must
   reject a bad signature before reading the body and must not buffer a whole object.
   An unauthenticated client must get nothing past the signature check. An authenticated
   client may read and write any object (there is one credential), but must still not be
   able to cause a torn or half-visible write, corrupt other objects' data or metadata, or
   exhaust gateway memory.
2. **Fragment bytes read back from storage.** `crates/chunk-format` decodes the on-disk
   fragment format. The decoder must verify the header checksum before acting on any
   header field, bounds-check every read, and reject reserved flags and unknown code points.
   Fragments come from D servers, which are the least-trusted internal tier (§14.2), so a
   fragment may be corrupt, truncated, oversized, stale, or offered under the wrong chunk id.
   The spec is in `docs/design/specs/`; the conformance vectors (valid and invalid) are in
   `docs/design/specs/conformance/vectors/`.
3. **Responses from a D server** to the client in `crates/core` and to the custodian:
   `GetFragment`, `ListFragments` and `Health` replies. A compromised D server must not be
   able to make a reader return wrong bytes as correct, make a write appear committed when
   it is not, or make the custodian delete a fragment that is still referenced.
4. **gRPC requests to a D server** (`PutFragment`, `GetFragment`, `ListFragments`,
   `DeleteFragment`, `Health`): message sizes, ids, and paths derived from them. Bugs that
   let a request read or write outside the D server's data directory, crash it, or exhaust
   its memory or disk are in scope.
5. **Values read from the metadata and coordination stores** when they could have been
   written by an older or buggy client: chunk maps, inode records, leases and fencing tokens.
   Parsing them must fail closed, never panic or act on a half-parsed record.

Trusted: the operator, the command line and config of each role, and the metadata store
and etcd cluster themselves (network-isolated to the zone, §14.2).

## Components that matter most

In order: `crates/gateway-s3` (internet-facing) · `crates/chunk-format` (the one parser of
fully untrusted bytes) · `crates/core` (commit protocol, read path, erasure coding) ·
`crates/chunkstore-grpc` + `crates/server/src/dserver.rs` + `crates/chunkstore-fs` (the
D server) · `crates/custodian` (it holds delete authority, so a bug there loses data) ·
the metadata backends.

Lower priority: `crates/validate` (test client), `crates/testkit`, the `*-conformance`
crates and `crates/dst` (test harnesses), `xtask` (build automation), `deploy/` (sample
compose files and scripts).

Out of scope: the generated protobuf code in `crates/proto`; third-party crates (report a
dependency bug only if Wyrd's use of it is what makes it exploitable).

Where bugs have actually turned up: the "Recurring defect classes" list in
[`AGENTS.md`](../AGENTS.md) (protocol input limits, parser grammar strictness, serialization
round-trips, silently skipped entries, transaction rollback, unbounded awaits) records the
kinds of defects our own reviews keep finding. Those are good places to look first.

## Known gaps: do not report these on their own

- **The internal gRPC fabric has no TLS or authentication yet.** mTLS between components
  is designed (ADR-0005, ADR-0025) but lands at milestone M5. "A D server accepts
  unauthenticated gRPC" is known. What is in scope is what an attacker who can reach that
  port can do beyond what the protocol already allows: memory corruption, path traversal,
  crashing the node, or using it to damage data on other nodes.
- **The S3 listener speaks plain HTTP.** TLS is expected to be terminated in front of it
  until a crypto-provider dependency decision is made. Missing TLS is known.
- **No encryption at rest yet** (designed in ADR-0021, milestone M6). Fragments are stored
  in plaintext.
- **No multi-tenant isolation, per-bucket permissions, quotas or rate limits yet.** The S3
  gateway has a single credential with access to everything. Generic "no rate limit" DoS
  reports are known; a specific request that makes a node use memory or disk far out of
  proportion to its size is in scope.
- **`wyrd custodian --gc-expired-pending` is unsafe while any writer is running** (#557).
  The CLI stamps leases from a logical clock, so an in-flight write's lease already reads
  as expired and its fragments can be swept mid-write. This is documented in the flag's
  usage text; the operator must only pass it when the backend is taking no writes. GC
  without the flag is in scope.
- `#![forbid(unsafe_code)]` holds in every crate except `crates/metadata-fdb`, which has
  one `unsafe` block around the FoundationDB C client.

## How to exercise it

Everything is prebuilt; nothing needs the network.

- Tests (all features): `cargo test --workspace --exclude wyrd-dst --all-features`.
  Tests that need a live FoundationDB, TiKV or etcd cluster skip when none is configured.
- Simulation tests: `RUSTFLAGS="--cfg madsim" CARGO_TARGET_DIR=target-madsim
  MADSIM_TEST_NUM=50 cargo test -p wyrd-dst`. Every failure reproduces from its seed.
- Fragment-format conformance: `cargo xtask conformance`.
- A local S3 gateway on the embedded backends:
  `target/debug/wyrd s3 --access-key test --secret-key testsecret --s3-listen 127.0.0.1:9000 --data-dir /tmp/wyrd`,
  then drive it with hand-built SigV4 requests or `target/debug/wyrd-validate` (run it with
  no arguments for its flags). An unsigned request gets 403.
- D servers: `target/debug/wyrd d-server --bind 127.0.0.1:7001 --health-bind 127.0.0.1:7101 --data-dir /tmp/wyrd-d1`
  (and 7002/7102, 7003/7103 likewise); write through them with
  `target/debug/wyrd put <file> --key k --durability "rs(2,1)" --data-dir /tmp/wyrd-meta --endpoints http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003`.
  and read back with `wyrd get k --out <file>` and the same `--data-dir` and `--endpoints`.
  The default durability, `rs(6,3)`, needs nine D servers. `wyrd` with no arguments prints
  full usage.

## How we rate severity

Correctness of stored data is the project's main promise, so data-integrity bugs rank as
high as classic memory-safety bugs.

- **Critical:** remote code execution; a forged or bypassed SigV4 check; a torn or
  half-visible write (atomic commit is the project's headline promise); silent data
  corruption or loss that a client can trigger (a read returns wrong bytes as valid, a
  committed write is lost, the custodian reclaims a fragment that is still referenced).
- **High:** path traversal on a D server; a panic or crash in
  the gateway or D server reachable by one unauthenticated request; any memory-safety bug
  in the `unsafe` FDB block.
- **Medium:** denial of service that needs an authenticated client or sustained traffic;
  memory or disk use far out of proportion to request size; a corrupt fragment that is
  rejected but takes a node down instead of being reconstructed.
- **Low:** information leaks in error messages or logs; issues only reachable by the
  trusted operator or through a misconfiguration.

A clean reproducer matters more to us than a severity label. A failing test in the style of
the existing ones (a DST seed, a conformance vector, or an integration test) is the best
form.

## How reports and patches should look

Reproduce against the code in this image. Patches should follow the existing style: keep
`#![forbid(unsafe_code)]`, add a test that fails before the fix, and pass
`cargo clippy --all-targets` with the workspace lint settings.
