### Development

- [ ] Integration test for Cached
   - [ ] Check assertion on weight used
- [ ] Support for `update` API
- [ ] Add doorkeeper in `TinyLFU`
- [ ] `Clear` and `Close` methods
- [ ] Clear TODOs
- [ ] Add benchmarks
- [ ] Validation on user inputs like `counters`, `pool_size` etc
- [ ] Callbacks when a key is deleted or evicted
- [ ] Add `loom` based tests
- [ ] Logging
- [ ] Validate TinyLFU logic of resetting counters
- [ ] `Clippy`
  - [X] Add `Cargo Clippy` in the pipeline
  - [ ] Fail pipeline on `Clippy` warnings
- [ ] See if there is alternative to `receiver.recv()` because this means thread is blocked
- [ ] Measure the amount of time it takes for `TTLTicker` to clean up N keys (N >= 50000)
- [ ] Contention free pool, remove `RWLock` if possible

### Release
- [ ] Documentation
-  [ ] Release
   - [ ] Release build instead of debug
-  [ ] README
-  [ ] Take a look at `Cargo.toml` of any production project