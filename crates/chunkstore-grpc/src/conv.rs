//! Translations between the `traits` domain types and the `wyrd.v0` wire types.
//!
//! These are deliberately small and total. Two impedance mismatches matter:
//! protobuf has no 128-bit integer, so a [`ChunkId`] crosses the wire as two
//! 64-bit halves; and protobuf has no `u16`, so a fragment index travels as a
//! `uint32` (values stay in `0..n`). The to-wire direction is infallible; the
//! from-wire direction validates presence and range, returning a tonic
//! [`Status`] the service surfaces directly.

use tonic::Status;
use wyrd_proto::v0::{ChunkId as WireChunkId, FragmentId as WireFragmentId, Health as WireHealth};
use wyrd_traits::{ChunkId, FragmentId, Health};

/// `u128` chunk id → its high/low 64-bit wire halves.
fn to_wire_chunk_id(chunk: ChunkId) -> WireChunkId {
    WireChunkId {
        hi: (chunk >> 64) as u64,
        lo: chunk as u64,
    }
}

/// Reassemble a `u128` chunk id from its wire halves.
fn from_wire_chunk_id(wire: WireChunkId) -> ChunkId {
    (u128::from(wire.hi) << 64) | u128::from(wire.lo)
}

/// `traits::FragmentId` → the wire message (infallible).
pub(crate) fn to_wire_fragment_id(id: FragmentId) -> WireFragmentId {
    WireFragmentId {
        chunk: Some(to_wire_chunk_id(id.chunk)),
        index: u32::from(id.index),
    }
}

/// Wire fragment id → `traits::FragmentId`, validating that the chunk is present
/// and the index fits the `u16` the trait uses. A malformed request is a client
/// error, mapped to `INVALID_ARGUMENT`.
pub(crate) fn from_wire_fragment_id(wire: Option<WireFragmentId>) -> Result<FragmentId, Status> {
    let wire = wire.ok_or_else(|| Status::invalid_argument("missing fragment id"))?;
    let chunk = wire
        .chunk
        .ok_or_else(|| Status::invalid_argument("missing chunk id"))?;
    let index = u16::try_from(wire.index)
        .map_err(|_| Status::invalid_argument("fragment index out of range"))?;
    Ok(FragmentId {
        chunk: from_wire_chunk_id(chunk),
        index,
    })
}

/// `traits::Health` → the wire enum.
pub(crate) fn to_wire_health(health: Health) -> WireHealth {
    match health {
        Health::Healthy => WireHealth::Healthy,
        Health::Degraded => WireHealth::Degraded,
        Health::Unhealthy => WireHealth::Unhealthy,
    }
}

/// The wire enum (carried as its `i32` discriminant) → `traits::Health`. An
/// unrecognised discriminant is a contract violation by the peer.
pub(crate) fn from_wire_health(status: i32) -> Result<Health, Status> {
    let wire = WireHealth::try_from(status)
        .map_err(|_| Status::invalid_argument(format!("unknown health status {status}")))?;
    Ok(match wire {
        WireHealth::Healthy => Health::Healthy,
        WireHealth::Degraded => Health::Degraded,
        WireHealth::Unhealthy => Health::Unhealthy,
    })
}
