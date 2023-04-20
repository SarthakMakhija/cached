### Development
1. Integration test for Cached
   1. Check assertion on weight used
2. Support for `update` API
3. Add doorkeeper in `TinyLFU`
4. `Clear` and `Close` methods
5. Clear TODOs
6. Add benchmarks
7. Validation on user inputs like `counters`, `pool_size` etc
8. Callbacks when a key is deleted or evicted
9. Add `loom` based tests
10. Logging
11. Validate TinyLFU logic of resetting counters
12. `Clippy` in pipeline
13. See if there is alternative to `receiver.recv()` because this means thread is blocked
14. Measure the amount of time it takes for `TTLTicker` to clean up N keys (N >= 50000)
15. Contention free pool, remove `RWLock` if possible

### Release
1. Documentation
2. Release
   1. Release build instead of debug
3. README
4. Take a look at `Cargo.toml` of any production project