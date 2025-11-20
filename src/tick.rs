use std::fmt;
use std::ops::{Add, Sub};

/// Absolute tick in modular 32-bit time.
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct Tick(pub u32);

/// Signed linear delta between two ticks.
/// Range: -(2^31) ..= +(2^31 - 1)
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct TickDelta(pub i32);

impl Tick {
    /// Create a tick.
    pub fn new(value: u32) -> Self {
        Tick(value)
    }

    /// Return the raw value.
    pub fn value(self) -> u32 {
        self.0
    }

    /// Compute a wrap-aware signed difference: self - other.
    /// Positive means self is after other.
    pub fn diff(self, other: Tick) -> TickDelta {
        TickDelta(self.0.wrapping_sub(other.0) as i32)
    }

    /// Returns true if `self` happens after `other` in tick-time.
    pub fn is_after(self, other: Tick) -> bool {
        self.diff(other).0 > 0
    }

    /// Returns true if `self` happens before `other`.
    pub fn is_before(self, other: Tick) -> bool {
        self.diff(other).0 < 0
    }

    /// Add a tick delta with wrapping.
    pub fn add(self, delta: TickDelta) -> Tick {
        Tick(self.0.wrapping_add(delta.0 as u32))
    }

    /// Subtract a tick delta with wrapping.
    pub fn sub(self, delta: TickDelta) -> Tick {
        Tick(self.0.wrapping_sub(delta.0 as u32))
    }
}

impl TickDelta {
    pub fn new(v: i32) -> Self {
        TickDelta(v)
    }

    pub fn value(self) -> i32 {
        self.0
    }
}

/// Allow `tick + delta`
impl Add<TickDelta> for Tick {
    type Output = Tick;

    fn add(self, delta: TickDelta) -> Tick {
        self.add(delta)
    }
}

/// Allow `tick - delta`
impl Sub<TickDelta> for Tick {
    type Output = Tick;

    fn sub(self, delta: TickDelta) -> Tick {
        self.sub(delta)
    }
}

/// Allow `new_tick - old_tick = delta`
impl Sub<Tick> for Tick {
    type Output = TickDelta;

    fn sub(self, other: Tick) -> TickDelta {
        self.diff(other)
    }
}

impl fmt::Debug for Tick {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Tick({})", self.0)
    }
}

impl fmt::Debug for TickDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TickDelta({})", self.0)
    }
}