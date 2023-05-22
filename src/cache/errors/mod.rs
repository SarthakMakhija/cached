use std::fmt::{Display, Formatter};

const ERROR_MESSAGE_TOTAL_COUNTERS_GT_ZERO: &str = "Total number of counters must be greater than zero";
const ERROR_MESSAGE_TOTAL_CACHE_WEIGHT_GT_ZERO: &str = "Total cache weight must be greater than zero";
const ERROR_MESSAGE_TOTAL_CAPACITY_GT_ZERO: &str = "Total capacity must be greater than zero";
const ERROR_MESSAGE_TOTAL_SHARDS_GT_ONE: &str = "Total number of shards must be greater than one";
const ERROR_MESSAGE_TOTAL_SHARDS_POWER_OF_2: &str = "Total number of shards must be a power of 2";
const ERROR_MESSAGE_POOL_SIZE_GT_ZERO: &str = "Pool size must be greater than zero";
const ERROR_MESSAGE_BUFFER_SIZE_GT_ZERO: &str = "Buffer size must be greater than zero";
const ERROR_MESSAGE_COMMAND_BUFFER_SIZE_GT_ZERO: &str = "Command buffer size must be greater than zero";
const ERROR_MESSAGE_KEY_WEIGHT_GT_ZERO: &str = "Weight of the input key/value must be greater than zero";
const ERROR_MESSAGE_WEIGHT_CALCULATION_GT_ZERO: &str = "Weight of the input key/value calculated by the weight calculation function must be greater than zero";
const ERROR_MESSAGE_UPSERT_VALUE_MISSING: &str = "Upsert has resulted in a put request, value must be specified";
const ERROR_MESSAGE_INVALID_UPSERT: &str = "Upsert request is invalid, either 'value', 'weight', 'time_to_live' or 'remove_time_to_live' must be specified";
const ERROR_MESSAGE_INVALID_UPSERT_EITHER_TIME_TO_LIVE_OR_REMOVE_TIME_TO_LIVE: &str = "Upsert request is invalid, only one of 'time_to_live' or 'remove_time_to_live' must be specified";

/// Errors enum define various application errors.
/// Currently, errors are categorized either as OperationError or ConfigError under [`ErrorType`] enum.
#[derive(Eq, PartialEq, Debug)]
pub(crate) enum Errors {
    TotalCountersGtZero,
    TotalCacheWeightGtZero,
    TotalCapacityGtZero,
    TotalShardsGtOne,
    TotalShardsPowerOf2,
    PoolSizeGtZero,
    BufferSizeGtZero,
    CommandBufferSizeGtZero,
    KeyWeightGtZero(&'static str),
    WeightCalculationGtZero,
    UpsertValueMissing,
    InvalidUpsert,
    InvalidUpsertEitherTimeToLiveOrRemoveTimeToLive,
}

pub(crate) enum ErrorType {
    ConfigError,
    OperationError(&'static str),
}

impl Display for ErrorType {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorType::ConfigError =>
                write!(formatter, "Config error"),
            ErrorType::OperationError(operation) =>
                write!(formatter, "Operation {}", operation),
        }
    }
}

impl Display for Errors {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Errors::TotalCountersGtZero =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_COUNTERS_GT_ZERO),
            Errors::TotalCacheWeightGtZero =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_CACHE_WEIGHT_GT_ZERO),
            Errors::TotalCapacityGtZero =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_CAPACITY_GT_ZERO),
            Errors::TotalShardsGtOne =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_SHARDS_GT_ONE),
            Errors::TotalShardsPowerOf2 =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_SHARDS_POWER_OF_2),
            Errors::PoolSizeGtZero =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_POOL_SIZE_GT_ZERO),
            Errors::BufferSizeGtZero =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_BUFFER_SIZE_GT_ZERO),
            Errors::CommandBufferSizeGtZero =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_COMMAND_BUFFER_SIZE_GT_ZERO),
            Errors::WeightCalculationGtZero =>
                write!(formatter, "[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_WEIGHT_CALCULATION_GT_ZERO),
            Errors::KeyWeightGtZero(operation) =>
                write!(formatter, "[{}]: {}", ErrorType::OperationError(operation), ERROR_MESSAGE_KEY_WEIGHT_GT_ZERO),
            Errors::UpsertValueMissing =>
                write!(formatter, "[{}]: {}", ErrorType::OperationError("upsert"), ERROR_MESSAGE_UPSERT_VALUE_MISSING),
            Errors::InvalidUpsert =>
                write!(formatter, "[{}]: {}", ErrorType::OperationError("upsert request builder"), ERROR_MESSAGE_INVALID_UPSERT),
            Errors::InvalidUpsertEitherTimeToLiveOrRemoveTimeToLive =>
                write!(formatter, "[{}]: {}", ErrorType::OperationError("upsert request builder"), ERROR_MESSAGE_INVALID_UPSERT_EITHER_TIME_TO_LIVE_OR_REMOVE_TIME_TO_LIVE),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::errors::{ERROR_MESSAGE_BUFFER_SIZE_GT_ZERO, ERROR_MESSAGE_TOTAL_CAPACITY_GT_ZERO, ERROR_MESSAGE_TOTAL_SHARDS_POWER_OF_2};
    use crate::cache::errors::ERROR_MESSAGE_COMMAND_BUFFER_SIZE_GT_ZERO;
    use crate::cache::errors::ERROR_MESSAGE_INVALID_UPSERT;
    use crate::cache::errors::ERROR_MESSAGE_INVALID_UPSERT_EITHER_TIME_TO_LIVE_OR_REMOVE_TIME_TO_LIVE;
    use crate::cache::errors::ERROR_MESSAGE_KEY_WEIGHT_GT_ZERO;
    use crate::cache::errors::ERROR_MESSAGE_POOL_SIZE_GT_ZERO;
    use crate::cache::errors::ERROR_MESSAGE_TOTAL_CACHE_WEIGHT_GT_ZERO;
    use crate::cache::errors::ERROR_MESSAGE_TOTAL_COUNTERS_GT_ZERO;
    use crate::cache::errors::ERROR_MESSAGE_TOTAL_SHARDS_GT_ONE;
    use crate::cache::errors::ERROR_MESSAGE_UPSERT_VALUE_MISSING;
    use crate::cache::errors::ERROR_MESSAGE_WEIGHT_CALCULATION_GT_ZERO;
    use crate::cache::errors::Errors;
    use crate::cache::errors::ErrorType;

    #[test]
    fn error_total_counters() {
        let error = Errors::TotalCountersGtZero;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_COUNTERS_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_total_cache_weight() {
        let error = Errors::TotalCacheWeightGtZero;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_CACHE_WEIGHT_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_total_capacity() {
        let error = Errors::TotalCapacityGtZero;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_CAPACITY_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_total_shards() {
        let error = Errors::TotalShardsGtOne;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_SHARDS_GT_ONE), error.to_string());
    }

    #[test]
    fn error_total_shards_power_of_2() {
        let error = Errors::TotalShardsPowerOf2;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_TOTAL_SHARDS_POWER_OF_2), error.to_string());
    }

    #[test]
    fn error_pool_size() {
        let error = Errors::PoolSizeGtZero;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_POOL_SIZE_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_buffer_size() {
        let error = Errors::BufferSizeGtZero;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_BUFFER_SIZE_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_command_buffer_size() {
        let error = Errors::CommandBufferSizeGtZero;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_COMMAND_BUFFER_SIZE_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_key_weight_calculation() {
        let error = Errors::WeightCalculationGtZero;
        assert_eq!(format!("[{}]: {}", ErrorType::ConfigError, ERROR_MESSAGE_WEIGHT_CALCULATION_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_key_weight() {
        let error = Errors::KeyWeightGtZero("put_with_weight");
        assert_eq!(format!("[{}]: {}", ErrorType::OperationError("put_with_weight"), ERROR_MESSAGE_KEY_WEIGHT_GT_ZERO), error.to_string());
    }

    #[test]
    fn error_upsert_value_missing() {
        let error = Errors::UpsertValueMissing;
        assert_eq!(format!("[{}]: {}", ErrorType::OperationError("upsert"), ERROR_MESSAGE_UPSERT_VALUE_MISSING), error.to_string());
    }

    #[test]
    fn error_upsert_invalid() {
        let error = Errors::InvalidUpsert;
        assert_eq!(format!("[{}]: {}", ErrorType::OperationError("upsert request builder"), ERROR_MESSAGE_INVALID_UPSERT), error.to_string());
    }

    #[test]
    fn error_upsert_invalid_time_to_live() {
        let error = Errors::InvalidUpsertEitherTimeToLiveOrRemoveTimeToLive;
        assert_eq!(format!("[{}]: {}", ErrorType::OperationError("upsert request builder"), ERROR_MESSAGE_INVALID_UPSERT_EITHER_TIME_TO_LIVE_OR_REMOVE_TIME_TO_LIVE), error.to_string());
    }
}