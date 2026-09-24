//! Proof, not just a claim: this is what actually gets checked into
//! the README as the "SIMD path is faster" evidence, rather than a
//! sentence asking to be taken on faith.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use engine::simd_agg::{sum_scalar, sum_simd};

fn bench_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("windowed_sum");
    for size in [64usize, 1_024, 65_536] {
        let data: Vec<f64> = (0..size).map(|i| i as f64).collect();
        group.bench_with_input(BenchmarkId::new("scalar", size), &data, |b, d| {
            b.iter(|| sum_scalar(std::hint::black_box(d)))
        });
        group.bench_with_input(BenchmarkId::new("simd", size), &data, |b, d| {
            b.iter(|| sum_simd(std::hint::black_box(d)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_sum);
criterion_main!(benches);