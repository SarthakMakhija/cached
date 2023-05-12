use std::sync::Arc;

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;

#[tokio::test]
async fn get_values_for_an_existing_keys() {
    let cached = CacheD::new(ConfigBuilder::new().total_cache_weight(1000).counters(10).build());
    let cached = Arc::new(cached);

    let mut handles = Vec::new();
    let readonly_cache = cached.clone();

    for task_id in 1..=5 {
        let cached = cached.clone();
        let future = async move {
            let mut start_index = task_id * 10;
            let end_index = start_index + 10;

            while start_index < end_index {
                cached.put_with_weight(start_index, start_index, 20).unwrap().handle().await;
                start_index += 1;
            }
        };
        handles.push(tokio::spawn(future));
    };
    for handle in handles {
        handle.await.unwrap();
    }

    for index in 10..60 {
        let value = readonly_cache.get(&index);
        assert_eq!(Some(index), value);
    }
    assert_eq!(1000, readonly_cache.total_weight_used());
}

#[tokio::test]
async fn put_key_values_given_cache_weight_is_reached() {
    let cached = CacheD::new(ConfigBuilder::new().total_cache_weight(980).counters(10).build());
    let cached = Arc::new(cached);

    let mut handles = Vec::new();
    let clone = cached.clone();

    for task_id in 1..=5 {
        let cached = cached.clone();
        let future = async move {
            let mut start_index = task_id * 10;
            let end_index = start_index + 10;

            while start_index < end_index {
                cached.put_with_weight(start_index, start_index, 20).unwrap().handle().await;
                start_index += 1;
            }
        };
        handles.push(tokio::spawn(future));
    };

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(980, clone.total_weight_used());
}