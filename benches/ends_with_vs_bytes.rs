use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn ends_with(s: &str, c: char) -> bool {
    s.ends_with(c)
}

fn last_eq(s: &str, c: u8) -> bool {
    s.as_bytes().last().unwrap() == &c
}

fn criterion_benchmark(c: &mut Criterion) {
    let s = "hello🚀a".repeat(100000);
    c.bench_function("ends_with", |b| {
        b.iter(|| ends_with(black_box(&s), black_box('\n')))
    });
    c.bench_function("last_eq", |b| {
        b.iter(|| last_eq(black_box(&s), black_box(b'\n')))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
