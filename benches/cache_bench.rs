use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn cache_benchmark(c: &mut Criterion) {
    c.bench_function("noop", |b| b.iter(|| black_box(1 + 1)));
}

criterion_group!(benches, cache_benchmark);
criterion_main!(benches);
