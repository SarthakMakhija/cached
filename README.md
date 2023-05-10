<p align="center">
  <img src="https://user-images.githubusercontent.com/21108320/230467879-7e2fa76a-627a-4074-8ab7-6d878b68b432.png"/>
</p>

[![Build](https://github.com/SarthakMakhija/cached/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/SarthakMakhija/cached/actions/workflows/build.yml)
[![Coverage](https://codecov.io/gh/SarthakMakhija/cached/branch/main/graph/badge.svg?token=ED4FKSYPCU)](https://codecov.io/gh/SarthakMakhija/cached)

LFU-based in-memory cache in Rust. (**WIP**)

**Features**
- Weight-Based Eviction - any large new item deemed valuable can evict multiple smaller items based on weight
- Fully Concurrent - you can use as many threads/tokio tasks as you want with little throughput degradation.
- Metrics - optional performance metrics for throughput, hit ratios, and other stats.
- Simple API - just figure out your ideal Config values and you're off and running.

**Usage**
```rust

#[tokio::test]
async fn put_a_key_value() {
	  let cached = CacheD::new(ConfigBuilder::default());
    let acknowledgement =
            cached.put("topic", "microservices").unwrap();
     acknowledgement.handle().await;
     
     let value = cached.get(&"topic");
     assert_eq!(Some("microservices"), value);
}

#[tokio::test]
async fn get_value_for_an_existing_key_and_map_it() {
    let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

    let acknowledgement =
        cached.put("topic", "microservices").unwrap();
    acknowledgement.handle().await;

    let value = cached.map_get(&"topic", |value| value.to_uppercase());
    assert_eq!("MICROSERVICES", value.unwrap());
}
```

### References

- [Tiny-LFU](https://dgraph.io/blog/refs/TinyLFU%20-%20A%20Highly%20Efficient%20Cache%20Admission%20Policy.pdf)
- [BP-Wrapper](https://dgraph.io/blog/refs/bp_wrapper.pdf)
- [Ristretto](https://github.com/dgraph-io/ristretto)

*The logo is built using [logo.com](https://app.logo.com/)*
