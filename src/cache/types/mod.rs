use std::time::SystemTime;

/// Defines the type for the hash of a key
pub type KeyHash = u64;

/// Defines the type for total counter used in `crate::cache::lfu::frequency_counter::FrequencyCounter`,
/// for counting the access frequencies of keys
pub type TotalCounters = u64;

/// Defines type for the capacity of the cache in terms of the number of items the cache may store
pub type TotalCapacity = usize;

/// Defines a flag to denote if the client has specified the `time_to_live`
pub type IsTimeToLiveSpecified = bool;

/// Defines the type for the total number of shards to be used in the data structure used for storing the key/value mapping
pub type TotalShards = usize;

/// Defines the type for the weight of the cache, which is the total space reserved for the cache
pub type Weight = i64;

/// Defines the type for the access frequency of keys in the cache
pub type FrequencyEstimate = u8;

/// Defines the type for the id of each key
pub(crate) type KeyId = u64;

/// Defines the type expiry of a key
pub(crate) type ExpireAfter = SystemTime;

/// Defines the type for the capacity for DoorKeeper which is an implementation of BloomFilter
pub(crate) type DoorKeeperCapacity = usize;

/// Defines the type for the false positive rate for DoorKeeper which is an implementation of BloomFilter
pub(crate) type DoorKeeperFalsePositiveRate = f64;
