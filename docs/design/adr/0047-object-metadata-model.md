---
created: 18.07.2026 16:20
type: adr
status: Accepted
tags:
  - adr
  - s3
  - metadata
  - gateway
  - object
---
# 0047. Object metadata model: ETag, Content-Type, Last-Modified on the inode record

## Context

The S3 gateway stores an object's byte size and nothing else. A `PutObject` answers an
empty `200` with no `ETag` (`crates/gateway-s3/src/lib.rs`, the PUT arm); a `GetObject`
hardcodes `content-type: application/octet-stream` and sets only `content-length`; the
`InodeRecord` carries only `size`/`chunk_map`/`state`/`version`
(`crates/core/src/metadata.rs`) and the protocol-neutral gateway seam (`ObjectRead`,
`crates/gateway-core/src/lib.rs`) returns only `size` + the body stream.

Many SDKs validate the `ETag` and round-trip `Content-Type`; their absence breaks
integrity checks and content typing. It also blocks the rest of the 0.1-Alpha S3 epic:
HeadObject (#506) has nothing to return, server-side copy (#504 step 2) has no `ETag` to
echo, and multipart needs the `ETag`-of-parts convention. This ADR decides the object
metadata **model** once, so those slices build on a settled foundation (#503).

## Decision

Extend `InodeRecord` with three **top-level, optional** fields — flat on the record,
matching its existing flat `size`/`state`/`version` shape:

- `etag: Option<String>` — the content digest, treated as an opaque change-token;
- `content_type: Option<String>` — the writing client's `Content-Type`, verbatim;
- `modified: Option<u64>` — content-publication time in epoch milliseconds.

Each field is `Option` + `#[serde(default)]` **and** `skip_serializing_if =
"Option::is_none"`. `InodeRecord` is serialized once, centrally, as JSON over the
byte-oriented `MetadataStore` KV seam (`crates/core/src/metadata.rs`, `encode`/`decode`),
so every backend (redb/tikv/fdb) stores the same JSON and one compatibility rule covers
them all: a record written before this change still deserializes (missing fields decode
as `None`), and a record missing metadata degrades on the wire to today's behaviour (no
`ETag` header, `application/octet-stream`), never to an error. The `skip_serializing_if`
half of the rule is load-bearing for the *writes*: every commit in `metadata.rs` is a
full-value CAS — `require(key, encode(prior))` compares the re-encoded prior record
byte-for-byte against the stored bytes — so decode→encode must be the **identity** on a
legacy record. Serializing the absent fields as `"etag":null,...` would never equal the
legacy JSON, turning every overwrite and every repair re-commit of a pre-upgrade object
into a permanent `Conflict`. The CAS counter `version` is **not** a schema version and is
neither bumped nor gated on for this change.

**Which commits set metadata (load-bearing).** The metadata fields are set only at
**content publication** — object create (`metadata::create` via `write::commit_create`)
and overwrite (`metadata::commit_chunk_map_superseding{,_leased}` via
`write::commit_overwrite`). They are committed **atomically with the chunk map**, in the
same CAS. The plain `metadata::commit_chunk_map` and the custodian
reconstruction/backfill/rebalance commits are **reconstruction/repair** paths that
re-commit the SAME content: they **preserve** the existing `etag`/`content_type`/`modified`
unchanged, so a placement-maintenance commit never moves `Last-Modified` or drops the
content type.

**Seam changes.** `ObjectGateway::put_object_streaming` takes the declared content type
(`Option<String>`, no HTTP types leak into `gateway-core`) and returns the committed
`ETag` instead of `()`; `ObjectRead` gains `etag`/`content_type`/`modified` alongside
`size`. All methods stay streaming; nothing buffers.

**Wire changes.** PUT: pass the request's `Content-Type` down; answer with the `ETag`
header (S3 quotes the value). GET: replace the hardcoded content type with the stored one
(falling back to `application/octet-stream`), add `ETag` and a RFC-7231 IMF-fixdate
`Last-Modified` (formatted in-tree, no new dependency).

**ETag basis.** The `ETag` is the **lowercase-hex SHA-256** of the content (quoted on the
wire) — the digest the write path already streams through `HashingSource`, so no second
read of the object. It is treated as an **opaque change-token**, **not** MD5. S3's
convention for simple PUTs is the MD5, and some tooling assumes `ETag == MD5`; but S3
itself documents the `ETag` as **not** guaranteed to be MD5 (SSE-KMS / multipart objects),
well-behaved clients treat it as opaque, and Wyrd already carries a vetted SHA-256 on this
path. Adding an MD5 dependency for a legacy equality would need the ADR-0003 dependency
audit and buys compatibility only with clients that violate the opacity rule.

`x-amz-meta-*` user metadata is deliberately **not** modelled here; the flat-record shape
leaves room to add it later without a schema decision.

## Alternatives considered

- **MD5 ETag (S3-classic):** maximal legacy compatibility, but requires a new MD5
  dependency (ADR-0003 audit + `deny.toml`) and a second digest over every streamed byte;
  rejected — SHA-256-as-opaque-token is spec-legal and already streamed.
- **A separate metadata record keyed off the inode:** avoids touching the record shape,
  but doubles the metadata round-trips and breaks the one-commit atomicity the write path
  has (`commit_chunk_map_superseding*` commits attributes + chunk map in one CAS);
  rejected.
- **Compute the ETag lazily on GET:** re-reads the whole object per request; violates the
  stream-don't-buffer economics (`0015:789`) and gives PUT no ETag to return; rejected.
- **A nested `metadata:` sub-struct on `InodeRecord`:** rejected in favour of flat
  top-level fields — the simplest serde-default compatibility story and one fewer schema
  decision downstream.

## Consequences

- **Public seam change.** `ObjectGateway` changes, so every implementer and in-tree test
  double updates in this slice (compiler-driven); `crates/server`'s `Gateway` is the
  production implementer.
- **Persisted records** are forward-compatible via `#[serde(default)]`: old records read
  fine; new records written by old code would drop the fields (acceptable pre-Alpha, no
  migration tool).
- **Wire behaviour** change is additive (new `ETag`/`Last-Modified` headers); GET's
  content type changes from always-`octet-stream` to stored-or-`octet-stream`.
- **DST determinism** holds: the publication timestamp is taken from `SystemTime::now()`
  at the commit call site, which madsim virtualises — no new clock dependency.
- **Multipart** `ETag`-of-parts is explicitly **deferred** to the multipart slice;
  conditional requests, server-side copy (#504 step 2), and HeadObject (#506) build on
  this model.
