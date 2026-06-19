//! Seed-reproducible round-trip property test (ADR-0009): for many seeds,
//! generate a random fragment, `encode` then `decode`, and assert the header and
//! payload survive exactly. A seed that ever fails is committable here as a
//! permanent regression.

use wyrd_chunk_format::{decode, encode, EcSchemeType, FragmentHeader};
use wyrd_testkit::Sim;

fn ec_scheme(byte: u8) -> EcSchemeType {
    match byte % 3 {
        0 => EcSchemeType::None,
        1 => EcSchemeType::Replication,
        _ => EcSchemeType::ReedSolomon,
    }
}

#[test]
fn encode_decode_round_trips_across_seeds() {
    for seed in 0..256u64 {
        let mut sim = Sim::new(seed);

        let chunk_id: u128 = sim.gen();
        let payload_len = (sim.gen::<u16>() % 1024) as usize;
        let payload: Vec<u8> = (0..payload_len).map(|_| sim.gen::<u8>()).collect();

        let mut header = FragmentHeader::new_v1(chunk_id, payload.len() as u64);
        header.ec_scheme_type = ec_scheme(sim.gen::<u8>());
        header.ec_k = sim.gen::<u8>();
        header.ec_m = sim.gen::<u8>();
        header.ec_fragment_index = sim.gen::<u16>();

        let bytes = encode(&header, &payload);
        let decoded = decode(&bytes).unwrap_or_else(|e| panic!("seed {seed}: decode failed: {e}"));

        assert_eq!(decoded.header, header, "seed {seed}: header mismatch");
        assert_eq!(decoded.payload, payload, "seed {seed}: payload mismatch");
    }
}
