//! SIMD-accelerated sum, benchmarked against a scalar baseline in
//! `benches/simd_vs_scalar.rs`. Deliberately scoped to `sum` only — a
//! correct SIMD `min`/`max` needs a lane-wise compare plus a horizontal
//! reduction, and that's a second, separately-verifiable piece of work
//! not included here (see `windowed.rs`, which computes min/max
//! scalar and says so).

use wide::f64x4;

pub fn sum_scalar(data: &[f64]) -> f64 {
    data.iter().sum()
}

pub fn sum_simd(data: &[f64]) -> f64 {
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    let mut acc = f64x4::splat(0.0);
    for chunk in chunks {
        acc += f64x4::from([chunk[0], chunk[1], chunk[2], chunk[3]]);
    }

    acc.reduce_add() + remainder.iter().sum::<f64>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simd_sum_matches_scalar_sum_across_boundary_sizes() {
        // sizes chosen to straddle the SIMD lane width (4): 0 and 1 are
        // degenerate, 3 is one short of a full lane, 4 is exact, 5 is
        // one over — this is where an off-by-one in the remainder
        // handling would actually show up.
        for size in [0usize, 1, 3, 4, 5, 16, 17, 1000] {
            let data: Vec<f64> = (0..size).map(|i| i as f64 * 0.5).collect();
            let scalar = sum_scalar(&data);
            let simd = sum_simd(&data);
            assert!(
                (scalar - simd).abs() < 1e-9,
                "mismatch at size {size}: scalar={scalar} simd={simd}"
            );
        }
    }
}