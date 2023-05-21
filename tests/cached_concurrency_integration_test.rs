use std::sync::Arc;

use cached::cache::cached::CacheD;
use cached::cache::config::ConfigBuilder;

#[tokio::test]
async fn get_values_for_an_existing_keys() {
    let cached = CacheD::new(ConfigBuilder::new(1000, 100, 1000).build());
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
    let cached = CacheD::new(ConfigBuilder::new(1000, 100, 980).build());
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

#[tokio::test]
async fn ensure_that_the_weight_of_the_cache_does_not_go_beyond_the_total_weight() {
    let cached = CacheD::new(ConfigBuilder::new(1000, 100, 9500).build());
    let cached = Arc::new(cached);

    let mut handles = Vec::new();
    let readonly_cache = cached.clone();

    for task_id in 1..=1000 {
        let cached = cached.clone();
        let future = async move {
            let mut start_index = task_id * 10;
            let end_index = start_index + 10;

            while start_index < end_index {
                cached.put_with_weight(start_index, start_index, 10).unwrap().handle().await;
                start_index += 1;
            }
        };
        handles.push(tokio::spawn(future));
    };
    for handle in handles {
        handle.await.unwrap();
    }
    assert_eq!(9500, readonly_cache.total_weight_used());
}

#[tokio::test]
async fn put_delete_and_get() {
    let cached = Arc::new(CacheD::new(ConfigBuilder::new(1000, 100, 2000).build()));
    let put_cached = cached.clone();

    let put_handle = tokio::spawn(async move {
        for count in 1..=100 {
            put_cached.put_with_weight(count, count, 20).unwrap().handle().await;
        }
    });
    put_handle.await.unwrap();

    let delete_cached = cached.clone();
    let delete_handle = tokio::spawn(async move {
        for count in 50..=70 {
            delete_cached.delete(count).unwrap().handle().await;
        }
    });
    delete_handle.await.unwrap();

    let deleted_range = 50..=70;
    for count in 1..=100 {
        if deleted_range.contains(&count) {
            assert_eq!(None, cached.get(&count));
        } else {
            assert_eq!(Some(count), cached.get(&count));
        }
    }
}