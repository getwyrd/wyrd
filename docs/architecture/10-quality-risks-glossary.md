# 10–12. Quality, risks, and glossary

> Living document. Combines arc42 sections 10 (quality scenarios), 11 (risks and
> technical debt), and 12 (glossary), trimmed to what earns its maintenance.

## 10. Quality scenarios

Concrete, measurable scenarios that operationalize the quality goals (section
1.3). Numbers are targets to be set during implementation; the *shape* is fixed.

| # | Scenario | Measure |
|---|----------|---------|
| Q1 | A D server is lost from a zone | All affected chunks return to full redundancy within time-to-repair budget T, with no read errors during repair (reads reconstruct from survivors) |
| Q2 | Silent bit rot corrupts a fragment | Scrubber detects it within scrub-cycle period P, before the data is needed; reconstruction follows |
| Q3 | A write is interrupted at any point | No torn state observable; either the old version or the new, never a hybrid; orphaned fragments collected by GC |
| Q4 | A zone is lost entirely | Under-replicated files re-replicated from survivors within budget; bounded by cross-region bandwidth |
| Q5 | Concurrent writers to the same file | Exactly one commit wins (version conflict); the other retries or fails cleanly |
| Q6 | Aggregate write throughput vs. cluster size | Scales close to linearly with D-server count, divided by EC amplification (~1.5× for RS(6,3)) |
| Q7 | Small-file (sub-threshold) workload | Dominated by metadata-op rate, not byte rate; served by the inline path; scales with the metadata tier |
| Q8 | Rolling upgrade across a version-skewed fleet | No downtime, no data errors, neighbors interoperate across one version gap |
| Q9 | Operator drains a D server | Data evacuated while maintaining redundancy and failure-domain invariants throughout; resumable; observable progress |

These belong in CI as load/fault tests where feasible (Q1–Q3, Q5–Q7) and in the
DR runbook drills otherwise (Q4, Q8, Q9).

## 11. Risks and technical debt

| Risk | Nature | Mitigation |
|------|--------|------------|
| Correctness bugs in the client library and custodians | These are where most correctness risk concentrates (the thick client, the repair loops); bugs here are silent corruption, not crashes | DST from day one; Jepsen runs; conformance vectors; Rust's type system in exactly this code |
| Small-file performance | A Drive workload is millions of small files; without the inline path it hammers the metadata tier and never reaches data-path scaling | Inline-data path below a size threshold (parameter **[OPEN]**); metadata-tier sharding |
| Multi-language client divergence | A non-Rust client re-implementing the thick logic incorrectly breaks atomicity or readability | Spec + conformance suite; consider a Rust core with FFI bindings rather than reimplementation |
| Contributor on-ramp (Rust) | Smaller drive-by-contributor pool; steeper ramp | Excellent CONTRIBUTING; good-first-issues in non-hot-path crates; coarse-to-fine crate split so newcomers aren't lost in plumbing |
| Compile times on a large workspace | Slows iteration | Disciplined crate split (the trait boundaries are also compile-unit boundaries); start coarser, split as needed |
| Treating etcd as a database | The classic L5 failure mode; etcd is wonderful until used as a store | Architectural rule: nothing data-proportional in L5; sized in kilobytes |
| Spanner-class store operational weight | CockroachDB/TiDB are heavy for L2 | Justified because L2 is small and off the data path; revisit only if it proves a bottleneck |

### Conscious exclusions (not debt — decisions)

- Cross-provider / untrusted federation (ADR-0005) — reversible, with stated
  cost.
- Full POSIX semantics over EC storage (FUSE is second-class).
- Single-binary as a production tier (ADR-0014).

## 12. Glossary

| Term | Meaning |
|------|---------|
| Commit point | The single linearizable metadata mutation that makes a write visible. The locus of the atomicity guarantee. |
| Zone | One datacenter/region's complete L4+L5 stack. The unit of atomicity and intra-provider federation. |
| D server | A "dumb" storage server: stores and returns fragments by chunk ID, verifies checksums; no placement or metadata logic. |
| Custodian | A background maintenance service: GC, scrub, reconstruction, rebalancing (L4) or cross-zone re-replication (L3). |
| Chunk | The unit of placement and erasure coding; a file is split into chunks. |
| Fragment | One erasure-coded piece of a chunk; *n* fragments produced, any *k* reconstruct. |
| Inode / dirent | Metadata records: the inode is "what this thing is" (+ chunk map); the dirent is "what it is called and where it lives in the tree". |
| Inline data | Small-file optimization: data stored directly in the metadata record, skipping chunking and EC. |
| Home zone | The authoritative zone for a file; where its writes linearize. |
| Pending-chunk ledger | The GC table tracking leased, not-yet-committed chunks so failed writes become collectable garbage. |
| DST | Deterministic simulation testing: running the whole system in a simulated world where every bug reproduces from a seed. |
| EC | Erasure coding (Reed-Solomon). RS(k,m): *k* data + *m* parity fragments. |
| Wyrd | The project, and the name for the system's total woven state: committed + in-flight + owed, seen as one consistent whole. *The Norns weave the Wyrd.* See ADR-0017. |
| Urth | The Norn of "what has become"; names the durable committed record (metadata store + on-disk truth). |
| Verdandi | The Norn of "what is becoming"; names the write/commit path (client library + commit protocol). |
| Skuld | The Norn of "what is owed / what shall be"; names pending work and reconciliation (replication queue, repair backlog, unsatisfied policy). |
| The Norns | Collective label for the three conceptually load-bearing components (Urth, Verdandi, Skuld). Lower-level mechanical parts keep plain descriptive names. |
