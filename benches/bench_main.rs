use criterion::criterion_main;

mod benchmarks;

#[cfg(not(tarpaulin_include))]
criterion_main! {
    benchmarks::delete::benches,
}