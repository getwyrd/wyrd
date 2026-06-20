//! Deterministic-simulation testing (DST) seams and harness skeleton.
//!
//! DST is the spine of the project's correctness story from Milestone 0
//! (ADR-0009): production logic is written against the abstract time and disk
//! seams in this crate, then driven by a single-threaded, seed-reproducible
//! simulator in which every bug reproduces from its seed. **madsim** is the
//! intended production runtime (it simulates time, scheduling, network, and
//! randomness); these seams are shaped to be driven by it, and a madsim-backed
//! [`Sim`] runner is wired in as the first async protocol code lands.
//!
//! This crate is a real dependency, not a helper, so the determinism story
//! cannot rot as the system grows. At M0 it provides the trait seams, a
//! seed-derived deterministic RNG, fault-injection hook points, and a runner
//! skeleton.

#![forbid(unsafe_code)]

use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Abstract logical time. Production code reads time through this seam instead
/// of the wall clock, so the simulator controls time and a run is reproducible.
pub trait Clock {
    /// The current logical time, in milliseconds since the simulation epoch.
    fn now_millis(&self) -> u64;
}

/// The production [`Clock`]: real wall-clock time, in milliseconds since the Unix
/// epoch. Used by single-process backends outside a simulation.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A manually-advanced [`Clock`] for deterministic tests: cheap to clone and
/// share (the handle and the code under test see the same time), and advanced
/// explicitly so expiry and timeout logic is exercised without real waiting.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    millis: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl ManualClock {
    /// A clock started at `start_millis`.
    pub fn new(start_millis: u64) -> Self {
        Self {
            millis: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(start_millis)),
        }
    }

    /// Advance the clock by `millis`.
    pub fn advance(&self, millis: u64) {
        self.millis
            .fetch_add(millis, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the clock to an absolute `millis`.
    pub fn set(&self, millis: u64) {
        self.millis
            .store(millis, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.millis.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Abstract durable storage. Production code performs disk I/O through this seam
/// so the simulator can model latency, reordering, and faults deterministically.
pub trait Disk {
    /// Read the bytes previously written under `key`, if any.
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, DiskError>;

    /// Write `bytes` under `key`. Not durable until [`Disk::sync`] succeeds.
    fn write(&mut self, key: &str, bytes: &[u8]) -> Result<(), DiskError>;

    /// Flush previously written bytes to durable storage.
    fn sync(&mut self) -> Result<(), DiskError>;
}

/// A disk fault surfaced by the simulator (or, later, a real backend).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskError {
    /// The operation failed due to an injected or real I/O fault.
    Io(String),
}

impl std::fmt::Display for DiskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskError::Io(msg) => write!(f, "disk i/o fault: {msg}"),
        }
    }
}

impl std::error::Error for DiskError {}

/// The operations at which a fault may be injected. Extended as more seams gain
/// fault coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// A disk read.
    DiskRead,
    /// A disk write.
    DiskWrite,
    /// A durability sync.
    DiskSync,
}

/// Decides whether to inject a fault at a given point. The default
/// implementation injects nothing; a campaign supplies one that fails
/// operations according to the seed.
pub trait FaultInjector {
    /// Return `true` to inject a fault at `point`.
    fn should_fail(&mut self, point: FaultPoint) -> bool;
}

/// A fault injector that never injects a fault — the baseline for a run that
/// exercises only the happy path.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoFaults;

impl FaultInjector for NoFaults {
    fn should_fail(&mut self, _point: FaultPoint) -> bool {
        false
    }
}

/// A seed-reproducible simulation context.
///
/// Everything non-deterministic a component needs — randomness, time, fault
/// decisions — is drawn from here, so a whole run is a pure function of its
/// seed. The runner is single-threaded by construction.
pub struct Sim {
    seed: u64,
    rng: ChaCha8Rng,
    clock_millis: u64,
}

impl Sim {
    /// Create a simulation from a seed. The same seed always produces the same
    /// run.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            rng: ChaCha8Rng::seed_from_u64(seed),
            clock_millis: 0,
        }
    }

    /// The seed this simulation was created from — record it to reproduce a run.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The deterministic RNG. All randomness in a run must be drawn from here.
    pub fn rng(&mut self) -> &mut impl RngCore {
        &mut self.rng
    }

    /// Draw a uniformly random value of type `T` from the deterministic RNG.
    pub fn gen<T>(&mut self) -> T
    where
        rand::distributions::Standard: rand::distributions::Distribution<T>,
    {
        self.rng.gen()
    }

    /// Advance logical time by `millis`.
    pub fn advance(&mut self, millis: u64) {
        self.clock_millis += millis;
    }
}

impl Clock for Sim {
    fn now_millis(&self) -> u64 {
        self.clock_millis
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_reproduces_the_same_run() {
        // The core DST promise: a run is a pure function of its seed.
        let mut a = Sim::new(42);
        let mut b = Sim::new(42);
        let xs: Vec<u64> = (0..16).map(|_| a.gen()).collect();
        let ys: Vec<u64> = (0..16).map(|_| b.gen()).collect();
        assert_eq!(xs, ys);
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Sim::new(1);
        let mut b = Sim::new(2);
        let xs: Vec<u64> = (0..16).map(|_| a.gen()).collect();
        let ys: Vec<u64> = (0..16).map(|_| b.gen()).collect();
        assert_ne!(xs, ys);
    }

    #[test]
    fn clock_advances_monotonically() {
        let mut sim = Sim::new(0);
        assert_eq!(sim.now_millis(), 0);
        sim.advance(10);
        sim.advance(5);
        assert_eq!(sim.now_millis(), 15);
    }

    #[test]
    fn no_faults_injects_nothing() {
        let mut faults = NoFaults;
        assert!(!faults.should_fail(FaultPoint::DiskWrite));
    }
}
