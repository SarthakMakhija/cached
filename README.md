<p align="center">
  <img src="https://user-images.githubusercontent.com/21108320/230467879-7e2fa76a-627a-4074-8ab7-6d878b68b432.png"/>
</p>

[![Build](https://github.com/SarthakMakhija/cached/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/SarthakMakhija/cached/actions/workflows/build.yml)
[![Coverage](https://codecov.io/gh/SarthakMakhija/cached/branch/main/graph/badge.svg?token=ED4FKSYPCU)](https://codecov.io/gh/SarthakMakhija/cached)

LFU-based in-memory cache in Rust.

### Features
- **High Cache-hit ratio**: Provides high-cache ratio, the numbers are available [here](https://github.com/SarthakMakhija/cached/blob/main/benches/results/cache_hits.json)
- **Simple API**: Provides simple APIs for `put`, `get`, `multi_get`, `map_get`, `delete` and `upsert` 
- **TTL and Access frequency based eviction**: Eviction is based either on `time_to_live`, if provided, or access frequency of the keys. A key with a higher access frequency can evict others 
- **Fully concurrent**: Provides support for concurrent puts, gets, deletes and upserts
- **Metrics**: Provides various metrics like: `CacheHits`, `CacheMisses`, `KeysAdded`, `KeysDeleted` etc., and exposes to the clients as `StatsSummary`
- **Configurable**: Provides configurable parameters to allows the clients to choose what works the best for them 

###Examples
```rust

const COUNTERS: TotalCounters = 100;
const CAPACITY: TotalCapacity = 10;
const CACHE_WEIGHT: Weight    = 100;

#[tokio::test]
async fn put_a_key_value() {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, CACHE_WEIGHT).build());
    let acknowledgement =
            cached.put("topic", "LFU cache").unwrap();
     acknowledgement.handle().await;
     
     let value = cached.get(&"topic");
     assert_eq!(Some("LFU cache"), value);
}

#[tokio::test]
async fn get_value_for_an_existing_key_and_map_it() {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, CACHE_WEIGHT).build());

    let acknowledgement =
        cached.put_with_weight("topic", "LFU cache", 20).unwrap();
    acknowledgement.handle().await;

    let value = cached.map_get(&"topic", |value| value.to_uppercase());
    assert_eq!("LFU CACHE", value.unwrap());
}

#[tokio::test]
async fn update_the_weight_of_an_existing_key() {
    let cached = CacheD::new(ConfigBuilder::new(COUNTERS, CAPACITY, CACHE_WEIGHT).build());

    let acknowledgement =
        cached.put("topic", "LFU cache").unwrap();
    acknowledgement.handle().await;

    let acknowledgement =
        cached.upsert(UpsertRequestBuilder::new("topic").weight(29).build()).unwrap();
    acknowledgement.handle().await;

    let value = cached.get_ref(&"topic");
    let value_ref = value.unwrap();
    let stored_value = value_ref.value();
    let key_id = stored_value.key_id();

    assert_eq!("LFU cache", stored_value.value());
    assert_eq!(Some(29), cached.admission_policy.weight_of(&key_id));
}
```
### Usage

### Cache-hit ratio

Cache-hit ratio is measured using [Zipf](https://en.wikipedia.org/wiki/Zipf%27s_law) distribution using [rand_distr](https://docs.rs/rand_distr/latest/rand_distr/struct.Zipf.html) crate.
Each key and value is of type `u64` and the system calculated weight of a single key/value pair is 40 bytes.

The benchmark runs with the following parameters:

```rust
//Total of 100_000 key/value pairs are loaded in the cache
const CAPACITY: usize = 100_000;

//TotalCounters for count-min sketch will be 10 times the capacity
const COUNTERS: TotalCounters = (CAPACITY * 10) as TotalCounters;

//Each key/value pair takes 40 bytes of memory, so the total cache weight is 40 times the total capacity. 
//This parameter will be changed to understand the impact of cache-hit ratio
const WEIGHT: Weight = (CAPACITY * 40) as Weight;

//Total of 100_000 * 16 items form a sample in Zipf distribution
const ITEMS: usize = CAPACITY * 16;
```

| Total Cache Weight       	 | Zipf exponent 	 | Cache-hit ratio 	 | Comments                                                                                        	                          |
|----------------------------|-----------------|-------------------|----------------------------------------------------------------------------------------------------------------------------|
| 100_000 * 40 	             | 1.001         	 | 98%             	 | Total cache weight is `100_000 * 40`, it allows all the incoming keys to be accepted.                                    	 |
| 100_000 * 39 	             | 1.001         	 | 71%             	 | Cache weight is less than the total incoming keys, so some of the incoming keys will be rejected 	                         |
| 100_000 * 35 	             | 1.001         	 | 64%             	 | Cache weight is less than the total incoming keys, so some of the incoming keys will be rejected 	                         |

Benchmark for Cache-hit is available [here](https://github.com/SarthakMakhija/cached/blob/main/benches/benchmarks/cache_hits.rs) and its results are available 
[here](https://github.com/SarthakMakhija/cached/blob/main/benches/results/cache_hits.json).

### FAQs

### References

- [Ristretto](https://github.com/dgraph-io/ristretto)
- [Tiny-LFU](https://dgraph.io/blog/refs/TinyLFU%20-%20A%20Highly%20Efficient%20Cache%20Admission%20Policy.pdf)
- [BP-Wrapper](https://dgraph.io/blog/refs/bp_wrapper.pdf)

*The logo is built using [logo.com](https://app.logo.com/)*
