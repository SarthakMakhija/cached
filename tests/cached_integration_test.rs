mod r#macro;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use cached::cache::cached::CacheD;
use cached::cache::command::acknowledgement::CommandAcknowledgement;
use cached::cache::config::config::ConfigBuilder;

#[tokio::test]
async fn get_value_for_an_existing_keys() {
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

fn put<Key, Value>(cached: &CacheD<Key, Value>, key_value_pairs: HashMap<Key, Value>) -> Vec<Arc<CommandAcknowledgement>>
    where Key: Hash + Eq + Send + Sync + 'static,
          Value: Send + Sync + Clone + 'static {
    let mut acknowledgements = Vec::new();
    for key_value in key_value_pairs {
        let acknowledgement = cached.put(key_value.0, key_value.1);
        acknowledgements.push(acknowledgement);
    }
    return acknowledgements;
}