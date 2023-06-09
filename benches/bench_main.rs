use criterion::criterion_main;

mod benchmarks;

#[cfg(not(tarpaulin_include))]
criterion_main! {
    benchmarks::read_buffer::benches,
    benchmarks::frequency_counter::benches,
    benchmarks::put::benches,
    benchmarks::async_await_put::benches,
    benchmarks::get::benches,
    benchmarks::get_ref::benches,
    benchmarks::put_get::benches,
    benchmarks::delete::benches,
    benchmarks::put_or_update::benches,
    benchmarks::cache_hits::benches,
}