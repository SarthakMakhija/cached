use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::read_buffer::benches,
    benchmarks::frequency_counter::benches,
    benchmarks::put::benches
}