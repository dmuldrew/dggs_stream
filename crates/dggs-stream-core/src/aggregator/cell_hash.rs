use std::hash::{BuildHasher, Hasher};

/// SplitMix64 finalizer over a raw 64-bit cell ID.
///
/// H3 fills unused child digits with ones, so at resolution R the low
/// `3 * (15 - R)` bits of every index are identical. A multiplicative hash
/// such as FxHash leaves those low bits nearly constant, and hashbrown picks
/// buckets from the low bits, so tables degrade toward linear probing. This
/// finalizer folds the varying base-cell and child digits into every output
/// bit. Arithmetic stays in u64 on both 32- and 64-bit hosts.
#[inline(always)]
pub fn mix_cell_id(cell_u64: u64) -> u64 {
    let mut h = cell_u64;
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

/// `BuildHasher` for maps and sets keyed by `u64` cell IDs.
#[derive(Clone, Copy, Debug, Default)]
pub struct CellIdBuildHasher;

impl BuildHasher for CellIdBuildHasher {
    type Hasher = CellIdHasher;

    #[inline(always)]
    fn build_hasher(&self) -> CellIdHasher {
        CellIdHasher(0)
    }
}

/// Hasher produced by [`CellIdBuildHasher`]. A single `u64` key hashes to
/// `mix_cell_id(key)`; other writes are folded through the same finalizer.
#[derive(Clone, Copy, Debug, Default)]
pub struct CellIdHasher(u64);

impl Hasher for CellIdHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline(always)]
    fn write_u64(&mut self, n: u64) {
        self.0 = mix_cell_id(self.0.rotate_left(5) ^ n);
    }

    /// Fallback for non-`u64` keys: fold 8-byte little-endian words, with
    /// the length mixed in so trailing zero bytes still change the hash.
    fn write(&mut self, bytes: &[u8]) {
        let (chunks, rest) = bytes.as_chunks::<8>();
        for &chunk in chunks {
            self.write_u64(u64::from_le_bytes(chunk));
        }
        let mut tail = [0u8; 8];
        tail[..rest.len()].copy_from_slice(rest);
        self.write_u64(u64::from_le_bytes(tail) ^ ((bytes.len() as u64) << 56));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// All 823,543 res-10 children of one res-3 cell, in index order. With
    /// FxBuildHasher, inserting and removing ~600k such cells took seconds.
    fn res10_cells() -> Vec<u64> {
        h3o::CellIndex::try_from(0x8a2a1072b59ffffu64)
            .unwrap()
            .parent(h3o::Resolution::Three)
            .unwrap()
            .children(h3o::Resolution::Ten)
            .map(u64::from)
            .collect()
    }

    #[test]
    fn cell_id_hashes_match_known_outputs() {
        // Resolution 8 at (-122.4194, 37.7749), with hashes from the original mixer.
        let cases: [(u64, u64); 4] = [
            (0x0882_8308_281f_ffff, 0xfe6f_d72f_0460_5740), // H3
            (0x1ae2_9800_0000_0000, 0x0b0d_4369_cad6_ad86), // A5
            (0x8085_9000_0000_0000, 0xa2db_1ec2_8201_f228), // S2, high bit set
            (0x0000_0000_0006_6343, 0xf311_b9ac_8c57_a7ed), // HEALPix
        ];
        for (cell_id, expected) in cases {
            assert_eq!(mix_cell_id(cell_id), expected);
            assert_eq!(CellIdBuildHasher.hash_one(cell_id), expected);
        }
    }

    #[test]
    fn res10_insert_remove_roundtrip() {
        let cells = res10_cells();
        assert!(cells.len() > 500_000, "{}", cells.len());
        let mut map: HashMap<u64, u64, CellIdBuildHasher> = HashMap::default();
        for &c in &cells {
            map.insert(c, c);
        }
        for &c in &cells {
            assert!(map.remove(&c).is_some());
        }
        assert!(map.is_empty());
    }
}
