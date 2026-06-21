//! Reed-Solomon throughput micro-benchmarks (M1.7, proposal 0003 PR step 7).
//!
//! The pure-CPU coding loop is the one performance number meaningful on a laptop
//! (network throughput waits for M2), and the first real data point for the
//! deferred chunk/stripe-size question (proposal 0002). Three operations across a
//! scheme × size matrix, reported as logical-byte throughput:
//!
//! - **encode** — split into k data shards + compute m parity shards.
//! - **decode_no_loss** — reconstruct with all k data shards present (the fast
//!   concatenation path; no RS decode runs).
//! - **reconstruct_loss** — reconstruct with m data shards missing, substituting
//!   the m parity shards: forces `reed_solomon_simd::decode` (worst-case recovery).
//!
//! Run via `cargo xtask bench`. CI tracks the numbers (regression visibility),
//! it does not gate on them — wall-clock is noisy (issue #99).

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use wyrd_core::erasure;

/// (k, m) schemes under test — the architecture's running example plus a narrower
/// and a wider scheme.
const SCHEMES: &[(usize, usize)] = &[(4, 2), (6, 3), (10, 4)];

/// Logical payload sizes. 1 MiB is the default chunk size; the smaller sizes
/// probe shard-size sensitivity for the stripe-size question.
const SIZES: &[usize] = &[16 * 1024, 256 * 1024, 1024 * 1024];

/// Deterministic payload (no RNG → reproducible inputs; criterion still measures
/// wall-clock).
fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect()
}

/// The k shards `reconstruct` needs with all data shards present (indices 0..k).
fn available_no_loss(shards: &[Vec<u8>], k: usize) -> Vec<(usize, Vec<u8>)> {
    (0..k).map(|i| (i, shards[i].clone())).collect()
}

/// The k shards `reconstruct` needs with the first m data shards missing,
/// substituting the m parity shards (indices m..k data + k..k+m parity) — forces
/// the RS decode path.
fn available_with_loss(shards: &[Vec<u8>], k: usize, m: usize) -> Vec<(usize, Vec<u8>)> {
    (m..k)
        .chain(k..k + m)
        .map(|i| (i, shards[i].clone()))
        .collect()
}

fn bench_erasure(c: &mut Criterion) {
    for &(k, m) in SCHEMES {
        for &size in SIZES {
            let data = payload(size);
            let shards = erasure::encode(k, m, &data).expect("encode");
            let no_loss = available_no_loss(&shards, k);
            let with_loss = available_with_loss(&shards, k, m);
            let label = format!("rs({k},{m})/{}KiB", size / 1024);

            let mut group = c.benchmark_group("encode");
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(&label), &data, |b, data| {
                b.iter(|| erasure::encode(k, m, black_box(data)).expect("encode"));
            });
            group.finish();

            let mut group = c.benchmark_group("decode_no_loss");
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(&label), &no_loss, |b, avail| {
                b.iter(|| erasure::reconstruct(k, m, size, black_box(avail)).expect("reconstruct"));
            });
            group.finish();

            let mut group = c.benchmark_group("reconstruct_loss");
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(&label),
                &with_loss,
                |b, avail| {
                    b.iter(|| {
                        erasure::reconstruct(k, m, size, black_box(avail)).expect("reconstruct")
                    });
                },
            );
            group.finish();
        }
    }
}

// CI-bounded sampling: the full 3×3×3 matrix runs in ~1–1.5 min. The numbers are
// tracked for regression visibility, not gated, so a small sample is enough.
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = bench_erasure
}
criterion_main!(benches);
