/// The module proxy exists only to allow writing benchmarks for non-public APIs.
/// `Cached` uses `criterion` as a dev-dependency to write benchmarks and
/// `criterion` supports writing benchmarks only for public APIs.
///  At this stage, we have benchmarks for non-public APIs including:
    /// `pool`              : Pool represents a ring-buffer that is used to buffer the gets for various keys
    /// `frequency_counter` : FrequencyCounter counts the access frequency of keys using count-min sketch
/// This module exposes proxy objects for these abstractions to allow benchmarks to be written.
/// Effectively, these proxy objects are thin in terms of behaviors and they end up delegating to the target objects.
pub mod pool;
pub mod frequency_counter;
pub mod admission_policy;