use std::time::SystemTime;

pub type ClockType = Box<dyn Clock + Send + Sync>;

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;

    fn has_passed(&self, time: &SystemTime) -> bool {
        return self.now().gt(time);
    }
}

pub(crate) struct SystemClock {}

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        return SystemTime::now();
    }
}

impl SystemClock {
    pub(crate) fn new() -> Self {
        return SystemClock {};
    }

    pub(crate) fn boxed() -> ClockType {
        return Box::new(SystemClock::new());
    }
}