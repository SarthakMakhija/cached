use std::time::SystemTime;

pub type ClockType = Box<dyn Clock + Send + Sync>;

#[derive(Clone)]
pub struct SystemClock {}

pub trait BoxedClockClone {
    fn clone_box(&self) -> ClockType;
}

pub trait Clock: Send + Sync + BoxedClockClone {
    fn now(&self) -> SystemTime;

    fn has_passed(&self, time: &SystemTime) -> bool {
        self.now().gt(time)
    }
}

impl<T> BoxedClockClone for T
    where
        T: 'static + Clock + Clone {
    fn clone_box(&self) -> ClockType {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Clock> {
    fn clone(&self) -> Box<dyn Clock> {
        self.clone_box()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

impl SystemClock {
    pub fn new() -> SystemClock {
        SystemClock {}
    }

    pub fn boxed() -> ClockType {
        Box::new(SystemClock::new())
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        SystemClock::new()
    }
}