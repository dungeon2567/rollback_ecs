use crate::component::Component;
use crate::storage::Storage;
use crate::tick::Tick;
use std::any::Any;
use std::cell::UnsafeCell;
use std::rc::Rc;

pub trait Rollback {
    fn rollback(&self, target_tick: Tick);
}

pub trait SetTick {
    fn set_tick(&self, tick: Tick);
}

/// A trait that combines Any, Rollback, and SetTick for storage types.
/// This allows World to use a single array instead of separate arrays for each trait.
pub trait StorageLike: Any + Rollback + SetTick {
    /// Downcast to Any for type erasure
    fn as_any(&self) -> &dyn Any;
}

impl<T: Component + Clone> Rollback for Rc<UnsafeCell<Storage<T>>> {
    fn rollback(&self, target_tick: Tick) {
        unsafe {
            (*self.get()).rollback(target_tick);
        }
    }
}

impl<T: Component> SetTick for Rc<UnsafeCell<Storage<T>>> {
    fn set_tick(&self, tick: Tick) {
        unsafe {
            (*self.get()).set_tick(tick);
        }
    }
}

impl<T: Component> StorageLike for Rc<UnsafeCell<Storage<T>>> {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}
