use crate::tick::{Tick, TickDelta};

#[test]
fn test_tick_new_and_value() {
    let tick = Tick::new(42);
    assert_eq!(tick.value(), 42);
}

#[test]
fn test_tick_delta_new_and_value() {
    let delta = TickDelta::new(-10);
    assert_eq!(delta.value(), -10);
}

#[test]
fn test_tick_diff_positive() {
    let t1 = Tick::new(100);
    let t2 = Tick::new(50);
    let diff = t1.diff(t2);
    assert_eq!(diff.value(), 50);
}

#[test]
fn test_tick_diff_negative() {
    let t1 = Tick::new(50);
    let t2 = Tick::new(100);
    let diff = t1.diff(t2);
    assert_eq!(diff.value(), -50);
}

#[test]
fn test_tick_diff_zero() {
    let t1 = Tick::new(100);
    let t2 = Tick::new(100);
    let diff = t1.diff(t2);
    assert_eq!(diff.value(), 0);
}

#[test]
fn test_tick_diff_wrapping() {
    // Test wrapping behavior: u32::MAX - 0 should wrap to -1 as i32
    let t1 = Tick::new(u32::MAX);
    let t2 = Tick::new(0);
    let diff = t1.diff(t2);
    assert_eq!(diff.value(), -1);

    // Test wrapping in the other direction: 0 - u32::MAX should wrap to 1
    let t1 = Tick::new(0);
    let t2 = Tick::new(u32::MAX);
    let diff = t1.diff(t2);
    assert_eq!(diff.value(), 1);
}

#[test]
fn test_tick_is_after() {
    let t1 = Tick::new(100);
    let t2 = Tick::new(50);
    assert!(t1.is_after(t2));
    assert!(!t2.is_after(t1));
    assert!(!t1.is_after(t1));
}

#[test]
fn test_tick_is_before() {
    let t1 = Tick::new(50);
    let t2 = Tick::new(100);
    assert!(t1.is_before(t2));
    assert!(!t2.is_before(t1));
    assert!(!t1.is_before(t1));
}

#[test]
fn test_tick_add() {
    let tick = Tick::new(100);
    let delta = TickDelta::new(50);
    let result = tick.add(delta);
    assert_eq!(result.value(), 150);
}

#[test]
fn test_tick_add_negative() {
    let tick = Tick::new(100);
    let delta = TickDelta::new(-50);
    let result = tick.add(delta);
    assert_eq!(result.value(), 50);
}

#[test]
fn test_tick_add_wrapping() {
    let tick = Tick::new(u32::MAX);
    let delta = TickDelta::new(1);
    let result = tick.add(delta);
    assert_eq!(result.value(), 0);
}

#[test]
fn test_tick_sub() {
    let tick = Tick::new(100);
    let delta = TickDelta::new(50);
    let result = tick.sub(delta);
    assert_eq!(result.value(), 50);
}

#[test]
fn test_tick_sub_negative() {
    let tick = Tick::new(100);
    let delta = TickDelta::new(-50);
    let result = tick.sub(delta);
    assert_eq!(result.value(), 150);
}

#[test]
fn test_tick_sub_wrapping() {
    let tick = Tick::new(0);
    let delta = TickDelta::new(1);
    let result = tick.sub(delta);
    assert_eq!(result.value(), u32::MAX);
}

#[test]
fn test_tick_add_operator() {
    let tick = Tick::new(100);
    let delta = TickDelta::new(50);
    let result = tick + delta;
    assert_eq!(result.value(), 150);
}

#[test]
fn test_tick_sub_operator_delta() {
    let tick = Tick::new(100);
    let delta = TickDelta::new(50);
    let result = tick - delta;
    assert_eq!(result.value(), 50);
}

#[test]
fn test_tick_sub_operator_tick() {
    let t1 = Tick::new(100);
    let t2 = Tick::new(50);
    let result = t1 - t2;
    assert_eq!(result.value(), 50);
}

#[test]
fn test_tick_equality() {
    let t1 = Tick::new(100);
    let t2 = Tick::new(100);
    let t3 = Tick::new(200);
    assert_eq!(t1, t2);
    assert_ne!(t1, t3);
}

#[test]
fn test_tick_ordering() {
    let t1 = Tick::new(50);
    let t2 = Tick::new(100);
    let t3 = Tick::new(100);

    assert!(t1 < t2);
    assert!(t2 > t1);
    assert!(t2 <= t3);
    assert!(t2 >= t3);
}

#[test]
fn test_tick_delta_equality() {
    let d1 = TickDelta::new(100);
    let d2 = TickDelta::new(100);
    let d3 = TickDelta::new(200);
    assert_eq!(d1, d2);
    assert_ne!(d1, d3);
}

#[test]
fn test_tick_debug() {
    let tick = Tick::new(42);
    let debug_str = format!("{:?}", tick);
    assert_eq!(debug_str, "Tick(42)");
}

#[test]
fn test_tick_delta_debug() {
    let delta = TickDelta::new(-10);
    let debug_str = format!("{:?}", delta);
    assert_eq!(debug_str, "TickDelta(-10)");
}

#[test]
fn test_tick_clone() {
    let t1 = Tick::new(100);
    let t2 = t1;
    assert_eq!(t1, t2);
}

#[test]
fn test_tick_delta_clone() {
    let d1 = TickDelta::new(50);
    let d2 = d1;
    assert_eq!(d1, d2);
}

#[test]
fn test_tick_hash() {
    use std::collections::HashMap;
    let mut map = HashMap::new();
    map.insert(Tick::new(100), "value1");
    map.insert(Tick::new(200), "value2");

    assert_eq!(map.get(&Tick::new(100)), Some(&"value1"));
    assert_eq!(map.get(&Tick::new(200)), Some(&"value2"));
    assert_eq!(map.get(&Tick::new(300)), None);
}

#[test]
fn test_tick_delta_hash() {
    use std::collections::HashMap;
    let mut map = HashMap::new();
    map.insert(TickDelta::new(100), "value1");
    map.insert(TickDelta::new(-50), "value2");

    assert_eq!(map.get(&TickDelta::new(100)), Some(&"value1"));
    assert_eq!(map.get(&TickDelta::new(-50)), Some(&"value2"));
    assert_eq!(map.get(&TickDelta::new(200)), None);
}

#[test]
fn test_tick_wrapping_edge_cases() {
    // Test various wrapping scenarios
    let max_tick = Tick::new(u32::MAX);
    let zero_tick = Tick::new(0);
    // MAX + 1 should wrap to 0
    assert_eq!(max_tick.add(TickDelta::new(1)), zero_tick);

    // 0 - 1 should wrap to MAX
    assert_eq!(zero_tick.sub(TickDelta::new(1)), max_tick);

    // MAX - 0 should give -1 delta (wrapping)
    assert_eq!(max_tick.diff(zero_tick).value(), -1);

    // 0 - MAX should give 1 delta (wrapping)
    assert_eq!(zero_tick.diff(max_tick).value(), 1);

    // MAX is before 0 (wrapping semantics: -1 means before)
    assert!(max_tick.is_before(zero_tick));

    // 0 is after MAX (wrapping semantics: +1 means after)
    assert!(zero_tick.is_after(max_tick));
}

#[test]
fn test_tick_delta_negative_values() {
    let tick = Tick::new(100);
    let large_negative = TickDelta::new(i32::MIN);
    let result = tick.add(large_negative);
    // Should wrap correctly
    assert_eq!(result.value(), 100u32.wrapping_add(i32::MIN as u32));
}

#[test]
fn test_tick_delta_positive_values() {
    let tick = Tick::new(100);
    let large_positive = TickDelta::new(i32::MAX);
    let result = tick.add(large_positive);
    // Should wrap correctly
    assert_eq!(result.value(), 100u32.wrapping_add(i32::MAX as u32));
}
