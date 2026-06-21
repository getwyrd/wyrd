//! **Single active custodian, fenced** (proposal 0005 §"Single active custodian,
//! fenced", `0005:358-383`).
//!
//! The custodian runs as **one active leader per zone**, elected via the *existing*
//! [`Coordination::elect_leader`] seam (`traits/src/lib.rs:286-288`), which returns a
//! [`Leadership`] carrying a monotonic **fencing token** that rises on every grant
//! (`traits/src/lib.rs:326-328`; `coordination-mem/src/lib.rs:184-192`). The
//! single-active safety invariant (`0005:56-59`): **at most one custodian acts per
//! zone**, and a superseded leader's actions are **rejected by the fencing token**.
//!
//! This module is the leadership half. The decisive second guard for a *location*
//! mutation — the version-conditional `commit` (`0005:200-203`) — belongs to the
//! reconstruction slice (M3.6, out of scope here); what M3.3 makes load-bearing is
//! the fence itself: a deposed leader's coordination action is refused.

use wyrd_traits::{Coordination, FencingToken, Leadership, Result};

/// A leader-elected custodian for a zone, holding the fenced [`Leadership`] term it
/// won. The term's fencing token stamps every coordination action so a superseded
/// leader is detectable.
#[derive(Debug, Clone)]
pub struct Custodian {
    zone: String,
    leadership: Leadership,
}

impl Custodian {
    /// Campaign to become the single active custodian for `zone`, via the existing
    /// `Coordination::elect_leader`. The returned custodian holds the term's fencing
    /// token.
    pub async fn elect(coord: &impl Coordination, zone: &str) -> Result<Self> {
        let leadership = coord.elect_leader(zone).await?;
        Ok(Self {
            zone: zone.to_owned(),
            leadership,
        })
    }

    /// The zone this custodian campaigned for.
    pub fn zone(&self) -> &str {
        &self.zone
    }

    /// The fencing token of this custodian's leadership term — rises on every new
    /// grant, so a later term's token strictly exceeds an earlier one's.
    pub fn term(&self) -> FencingToken {
        self.leadership.token
    }

    /// This custodian's leadership grant.
    pub fn leadership(&self) -> Leadership {
        self.leadership
    }
}

/// The single-active authority for one zone: it remembers the **current
/// (highest)** leadership term and **rejects** any action stamped with an older
/// token — the fencing guard that keeps a deposed-but-still-running custodian safe
/// (`0005:362-367`). A real backend enforces this at the coordination service and
/// at the version-conditional metadata `commit`; this is the in-process model of
/// that monotonic-token rejection, shared by the custodian's coordination actions.
#[derive(Debug, Default, Clone)]
pub struct FencedZone {
    current_term: FencingToken,
}

impl FencedZone {
    /// A zone with no leader yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a newly-granted leadership term as the current one. Monotonic: a
    /// stale grant never lowers the fence.
    pub fn install(&mut self, leadership: Leadership) {
        self.current_term = self.current_term.max(leadership.token);
    }

    /// The current (highest installed) fencing term.
    pub fn current_term(&self) -> FencingToken {
        self.current_term
    }

    /// Authorize an action stamped with `token`. A superseded leader — whose term
    /// is below the current one — is **rejected** ([`FenceError::Fenced`]); the
    /// active leader's action is admitted.
    pub fn authorize(&self, token: FencingToken) -> std::result::Result<(), FenceError> {
        if token < self.current_term {
            Err(FenceError::Fenced {
                token,
                current: self.current_term,
            })
        } else {
            Ok(())
        }
    }
}

/// A coordination action was refused because the actor's fencing token is stale —
/// it has been superseded by a newer leadership term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceError {
    /// The actor's token is below the zone's current term.
    Fenced {
        /// The stale token the deposed actor presented.
        token: FencingToken,
        /// The zone's current (winning) term.
        current: FencingToken,
    },
}

impl std::fmt::Display for FenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FenceError::Fenced { token, current } => write!(
                f,
                "fenced: action with stale leadership token {token} rejected \
                 (current term is {current})"
            ),
        }
    }
}

impl std::error::Error for FenceError {}
