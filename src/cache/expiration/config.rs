use std::time::Duration;

pub(crate) struct TTLConfig {
    shards: usize,
    tick_duration: Duration,
}

impl TTLConfig {
    pub(crate) fn new(shards: usize, tick_duration: Duration) -> Self {
        TTLConfig {
            shards,
            tick_duration,
        }
    }

    pub(crate) fn shards(&self) -> usize { self.shards }

    pub(crate) fn tick_duration(&self) -> Duration { self.tick_duration }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::cache::expiration::config::TTLConfig;

    #[test]
    fn ttl_shards() {
        let ttl_config = TTLConfig::new(4, Duration::from_millis(10));
        assert_eq!(4, ttl_config.shards());
    }

    #[test]
    fn ttl_tick_duration() {
        let ttl_config = TTLConfig::new(4, Duration::from_millis(20));
        assert_eq!(Duration::from_millis(20), ttl_config.tick_duration());
    }
}