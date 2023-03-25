#[macro_export]
macro_rules! hash_map {
    ($($key:expr => $value:expr),+) => {{
        use std::collections::HashMap;
        let mut key_value_pairs = HashMap::new();
        $(key_value_pairs.insert($key, $value);)+
        key_value_pairs
    }};
}