use criterion::criterion_main;

mod benchmarks;

#[cfg(not(tarpaulin_include))]
criterion_main! {
    benchmarks::cache_hits::benches,
}