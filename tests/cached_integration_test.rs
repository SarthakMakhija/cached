use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cached::cache::cached::CacheD;
use cached::cache::command::acknowledgement::CommandAcknowledgement;
use cached::cache::command::CommandStatus;
use cached::cache::config::ConfigBuilder;
use cached::cache::upsert::UpsertRequestBuilder;

mod r#macro;

#[tokio::test]
async fn get_values_for_an_existing_keys() {
    let cached = CacheD::new(ConfigBuilder::new().counters(10).build());
    let key_value_pairs = hash_map!("topic" => "microservices", "cache" => "cached", "disk" => "SSD");

    let acknowledgements = put(&cached, key_value_pairs.clone());
    for acknowledgement in acknowledgements {
        acknowledgement.handle().await;
    }

    for key_value in &key_value_pairs {
        let expected_value = key_value_pairs.get(key_value.0);
        assert_eq!(expected_value, cached.get(key_value.0).as_ref());
    }
}

#[tokio::test]
async fn update_values_for_an_existing_keys() {
    let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

    let key_value_pairs = (1..10).map(|index| (index, index * 10)).collect::<HashMap<i32, i32>>();
    let acknowledgements = put(&cached, key_value_pairs);
    for acknowledgement in acknowledgements {
        acknowledgement.handle().await;
    }

    let update_key_value_pairs = (1..10).map(|index| (index, index * 100)).collect::<HashMap<i32, i32>>();
    let acknowledgements = upsert(&cached, update_key_value_pairs.clone());
    for acknowledgement in acknowledgements {
        acknowledgement.handle().await;
    }

    for key_value in &update_key_value_pairs {
        let expected_value = update_key_value_pairs.get(key_value.0);
        assert_eq!(expected_value, cached.get(key_value.0).as_ref());
    }
}

#[tokio::test]
async fn delete_values_for_some_existing_keys() {
    let cached = CacheD::new(ConfigBuilder::new().counters(10).build());

    let key_value_pairs = (1..10).map(|index| (index, index * 10)).collect::<HashMap<i32, i32>>();
    let acknowledgements = put(&cached, key_value_pairs.clone());
    for acknowledgement in acknowledgements {
        acknowledgement.handle().await;
    }
    for index in 1..10 {
        if index % 2 == 0 {
            cached.delete(index).unwrap().handle().await;
        }
    }

    for key_value in &key_value_pairs {
        let expected_value = if key_value.0 % 2 == 0 {
            None
        } else {
            key_value_pairs.get(key_value.0)
        };
        assert_eq!(expected_value, cached.get(key_value.0).as_ref());
    }
}

#[tokio::test]
async fn weight_of_the_cache_does_not_exceed_the_maximum_weight() {
    let cached = CacheD::new(ConfigBuilder::new().total_cache_weight(100).counters(10).build());

    for index in 1..=10 {
        let status = cached.put_with_weight(index, index*10, 10).unwrap().handle().await;
        assert_eq!(CommandStatus::Accepted, status);
    }
    let status = cached.put_with_weight(11, 11*10, 8).unwrap().handle().await;
    assert_eq!(CommandStatus::Accepted, status);

    assert_eq!(98, cached.total_weight_used());
}

#[tokio::test]
async fn weight_of_the_cache_does_not_exceed_the_maximum_weight_100() {
    const TOTAL_KEYS: usize = 10;
    let cached = CacheD::new(
        ConfigBuilder::new()
            .total_cache_weight(100)
            .counters(100)
            .access_pool_size(1)
            .access_buffer_size(TOTAL_KEYS)
            .build()
    );

    for index in 1..=TOTAL_KEYS {
        let status = cached.put_with_weight(index, index*10, 10).unwrap().handle().await;
        assert_eq!(CommandStatus::Accepted, status);
    }
    for index in 1..=TOTAL_KEYS {
        cached.get(&index);
        cached.get(&index);
    }

    thread::sleep(Duration::from_secs(3));

    let status = cached.put_with_weight(11, 11*10, 8).unwrap().handle().await;
    assert_eq!(CommandStatus::Rejected, status);
    assert_eq!(100, cached.total_weight_used());
}

fn put<Key, Value>(cached: &CacheD<Key, Value>, key_value_pairs: HashMap<Key, Value>) -> Vec<Arc<CommandAcknowledgement>>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static {
    let mut acknowledgements = Vec::new();
    for key_value in key_value_pairs {
        let acknowledgement =
            cached.put(key_value.0, key_value.1).unwrap();
        acknowledgements.push(acknowledgement);
    }
    acknowledgements
}

fn upsert<Key, Value>(cached: &CacheD<Key, Value>, key_value_pairs: HashMap<Key, Value>) -> Vec<Arc<CommandAcknowledgement>>
    where Key: Hash + Eq + Send + Sync + Clone + 'static,
          Value: Send + Sync + Clone + 'static {
    let mut acknowledgements = Vec::new();
    for key_value in key_value_pairs {
        let acknowledgement =
            cached.upsert(UpsertRequestBuilder::new(key_value.0).value(key_value.1).build()).unwrap();
        acknowledgements.push(acknowledgement);
    }
    acknowledgements
}