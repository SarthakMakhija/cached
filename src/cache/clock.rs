use std::time::SystemTime;

/// Defines a boxed pointer to [`Clock`]
pub type ClockType = Box<dyn Clock + Send + Sync>;

/// The default implementation of [`Clock`] trait. `SystemClock` is cloneable
#[derive(Clone)]
pub struct SystemClock {}

/// BoxedClockClone represents a trait get an instance of [`ClockType`]
pub trait BoxedClockClone {
    fn clone_box(&self) -> ClockType;
}

/// Clock represents a trait to get the current [`std::time::SystemTime`]
///
/// The default implementation of Clock is SystemClock.
///
/// Clients can provide their implementation of Clock using [`crate::cache::config::Config`] and
/// since Clock is considered to be a lightweight object, Clock is of type [`BoxedClockClone`]
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