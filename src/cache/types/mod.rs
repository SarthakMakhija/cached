use std::time::SystemTime;

pub type KeyHash = u64;
pub type TotalCounters = u64;
pub type TotalCapacity = usize;
pub type IsTimeToLiveSpecified = bool;
pub type TotalShards = usize;
pub type Weight = i64;
pub type FrequencyEstimate = u8;

pub(crate) type KeyId = u64;
pub(crate) type ExpireAfter = SystemTime;
pub(crate) type DoorKeeperCapacity = usize;
pub(crate) type DoorKeeperFalsePositiveRate = f64;
