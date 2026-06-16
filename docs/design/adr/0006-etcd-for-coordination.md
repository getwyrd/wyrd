---
created: 13.06.2026 11:57
type: adr
status: Proposed
tags:
  - adr
  - coordination
  - consensus
---
# 0006. etcd for coordination; openraft reserved

## Context

L5 needs service discovery, leader election, distributed locks with fencing, and zone-wide config with change notification. A Raft library (openraft) provides consensus — but that is roughly 20% of what a coordination service is. Leases, watch streams with revision history, multi-key CAS transactions, membership changes, snapshot/compaction, and backup/restore are the rest, and each is a place to get correctness subtly wrong in the one component everything else trusts blindly. etcd has a decade of production hardening and multiple Jepsen analyses; it is also what the Kubernetes-shaped target audience already runs.

The project's novelty budget is spoken for by the commit protocol and custodian state machines.

## Decision

Use **etcd** in production, behind a `Coordination` trait. Provide an **in-memory** backend for the single-binary/dev profile (a one-process system needs no distributed coordination, and Rust cannot embed etcd as a library the way Go can). Reserve **openraft** (or raft-rs) as a future embedded backend for a no-external-dependency production mode, built behind the same trait and validated by the existing DST harness once the trait semantics are pinned by two implementations.

## Consequences

- Coordination is boring in exactly the way L5 needs; correctness risk stays pointed at the differentiator.
- An operational dependency the target audience already trusts.
- etcd's own auth has had CVEs in non-Kubernetes contexts; mitigated by treating etcd auth as defense-in-depth and network-isolating it behind the mTLS fabric (section 8.5).
- The decision is reversible by construction (the trait), so it is safe to make quickly.
