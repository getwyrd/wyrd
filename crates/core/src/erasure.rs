//! Reed-Solomon erasure coding (the compute core of Milestone 1) over
//! [`reed-solomon-simd`]. A chunk's bytes are split into *k* equal-size data
//! shards (zero-padded to a uniform, aligned size), and *m* parity shards are
//! computed, giving *n* = k + m shards. The original is reconstructible from
//! **any *k*** of the *n* shards.
//!
//! Pure and deterministic — no I/O, no clock, no randomness — so it runs
//! directly under the DST harness (ADR-0009). This module only turns bytes ↔
//! shards; wrapping shards in v1 fragment headers and wiring EC into the
//! write/read path are later Milestone-1 steps.

use std::fmt;

/// reed-solomon-simd requires every shard to share a length that is a multiple
/// of 64 bytes; data is zero-padded up to this alignment.
const ALIGN: usize = 64;

/// An erasure-coding failure.
#[derive(Debug)]
pub enum ErasureError {
    /// Fewer than `k` shards were supplied, so the chunk cannot be reconstructed.
    TooFewShards {
        /// How many shards were supplied.
        have: usize,
        /// How many are required (`k`).
        need: usize,
    },
    /// The supplied shards are not all the same length.
    InconsistentShardSize,
    /// The underlying coder rejected the operation.
    Coder(reed_solomon_simd::Error),
}

impl fmt::Display for ErasureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErasureError::TooFewShards { have, need } => {
                write!(f, "too few shards to reconstruct: have {have}, need {need}")
            }
            ErasureError::InconsistentShardSize => write!(f, "shards are not all the same length"),
            ErasureError::Coder(e) => write!(f, "reed-solomon coder error: {e}"),
        }
    }
}

impl std::error::Error for ErasureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ErasureError::Coder(e) => Some(e),
            _ => None,
        }
    }
}

/// The aligned shard size for a chunk of `data_len` bytes under `k` data shards.
fn shard_size(data_len: usize, k: usize) -> usize {
    let per_shard = data_len.div_ceil(k.max(1));
    per_shard.div_ceil(ALIGN).max(1) * ALIGN
}

/// Encode `data` into `n = k + m` equal-size shards: indices `0..k` are the data
/// shards (the chunk's bytes, zero-padded to an aligned shard size), `k..n` are
/// the parity shards. The original `data.len()` is **not** stored here — the
/// caller records the chunk's logical length and passes it back to
/// [`reconstruct`].
pub fn encode(k: usize, m: usize, data: &[u8]) -> Result<Vec<Vec<u8>>, ErasureError> {
    let size = shard_size(data.len(), k);
    let mut shards: Vec<Vec<u8>> = (0..k)
        .map(|i| {
            let mut shard = vec![0u8; size];
            let start = i * size;
            if start < data.len() {
                let end = (start + size).min(data.len());
                shard[..end - start].copy_from_slice(&data[start..end]);
            }
            shard
        })
        .collect();

    let parity = reed_solomon_simd::encode(k, m, &shards).map_err(ErasureError::Coder)?;
    shards.extend(parity);
    Ok(shards)
}

/// Reconstruct the original `logical_len` bytes from `available` — any `>= k` of
/// the `n` shards, each as `(global_index, bytes)` (indices `0..k` data, `k..n`
/// parity). Missing data shards are recovered, the `k` data shards concatenated,
/// and the result truncated to `logical_len`.
pub fn reconstruct(
    k: usize,
    m: usize,
    logical_len: usize,
    available: &[(usize, Vec<u8>)],
) -> Result<Vec<u8>, ErasureError> {
    if available.len() < k {
        return Err(ErasureError::TooFewShards {
            have: available.len(),
            need: k,
        });
    }
    let size = available[0].1.len();
    if available.iter().any(|(_, s)| s.len() != size) {
        return Err(ErasureError::InconsistentShardSize);
    }

    let mut data: Vec<Option<Vec<u8>>> = vec![None; k];
    let mut original: Vec<(usize, &[u8])> = Vec::new();
    let mut recovery: Vec<(usize, &[u8])> = Vec::new();
    for (idx, bytes) in available {
        if *idx < k {
            data[*idx].get_or_insert_with(|| bytes.clone());
            original.push((*idx, bytes.as_slice()));
        } else if *idx < k + m {
            recovery.push((*idx - k, bytes.as_slice()));
        }
    }

    if data.iter().filter(|d| d.is_some()).count() < k {
        let restored =
            reed_solomon_simd::decode(k, m, original, recovery).map_err(ErasureError::Coder)?;
        for (index, bytes) in restored {
            data[index] = Some(bytes);
        }
    }

    let mut out = Vec::with_capacity(k * size);
    for shard in data {
        out.extend_from_slice(&shard.ok_or(ErasureError::TooFewShards { have: 0, need: k })?);
    }
    out.truncate(logical_len);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wyrd_testkit::Sim;

    /// A deterministic byte pattern of length `len`.
    fn data_of(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8)
            .collect()
    }

    /// `(index, shard)` pairs for the shards at `indices`.
    fn pick(shards: &[Vec<u8>], indices: &[usize]) -> Vec<(usize, Vec<u8>)> {
        indices.iter().map(|&i| (i, shards[i].clone())).collect()
    }

    #[test]
    fn round_trip_matrix() {
        for (k, m) in [(2usize, 1usize), (3, 2), (6, 3), (4, 4)] {
            let n = k + m;
            for &len in &[0usize, 1, k, 100, 1 << 16] {
                let data = data_of(len);
                let shards = encode(k, m, &data).unwrap();
                assert_eq!(shards.len(), n, "k={k} m={m}: n shards");

                // From the k data shards.
                let from_data = pick(&shards, &(0..k).collect::<Vec<_>>());
                assert_eq!(
                    reconstruct(k, m, len, &from_data).unwrap(),
                    data,
                    "k={k} m={m} len={len}: from data shards"
                );

                // From a parity-inclusive subset: drop data shard 0, add parity 0.
                let mut subset: Vec<usize> = (1..k).collect();
                subset.push(k);
                assert_eq!(
                    reconstruct(k, m, len, &pick(&shards, &subset)).unwrap(),
                    data,
                    "k={k} m={m} len={len}: parity-inclusive subset"
                );
            }
        }
    }

    #[test]
    fn reconstructs_from_every_six_of_nine_for_rs_6_3() {
        let (k, m) = (6usize, 3usize);
        let n = k + m; // 9
        let data = data_of(1000);
        let shards = encode(k, m, &data).unwrap();

        let mut subsets = 0;
        for mask in 0u32..(1 << n) {
            if mask.count_ones() as usize != k {
                continue;
            }
            let indices: Vec<usize> = (0..n).filter(|i| mask & (1 << i) != 0).collect();
            assert_eq!(
                reconstruct(k, m, data.len(), &pick(&shards, &indices)).unwrap(),
                data,
                "subset {indices:?}"
            );
            subsets += 1;
        }
        assert_eq!(
            subsets, 84,
            "every 6-of-9 subset (C(9,6) = 84) reconstructs"
        );
    }

    #[test]
    fn seeded_random_data_and_subsets_round_trip() {
        let (k, m) = (4usize, 3usize);
        let n = k + m;
        for seed in 0..200u64 {
            let mut sim = Sim::new(seed);
            let len = (sim.gen::<u16>() % 5000) as usize;
            let data: Vec<u8> = (0..len).map(|_| sim.gen::<u8>()).collect();
            let shards = encode(k, m, &data).unwrap();

            // A random k-subset of the n shards (Fisher–Yates over the seeded RNG).
            let mut indices: Vec<usize> = (0..n).collect();
            for i in (1..n).rev() {
                let j = (sim.gen::<u32>() as usize) % (i + 1);
                indices.swap(i, j);
            }
            assert_eq!(
                reconstruct(k, m, len, &pick(&shards, &indices[..k])).unwrap(),
                data,
                "seed {seed}"
            );
        }
    }

    #[test]
    fn fewer_than_k_shards_is_an_error() {
        let data = data_of(500);
        let shards = encode(6, 3, &data).unwrap();
        let err = reconstruct(6, 3, data.len(), &pick(&shards, &[0, 1, 2, 3, 4])).unwrap_err();
        assert!(matches!(
            err,
            ErasureError::TooFewShards { have: 5, need: 6 }
        ));
    }
}
