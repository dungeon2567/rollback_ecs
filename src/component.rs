use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn next_id() -> usize {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Resource: Any + Clone + Default
where
    Self: Sized,
{
    fn type_index() -> usize;
}

pub trait Component: Resource {
    /// Returns true if this component type is temporary and should not be tracked by rollback.
    /// Defaults to false. Temporary components should override this to return true.
    const IS_TEMPORARY: bool = false;

    /// Returns the cleanup system for this component type as a boxed PipelineStage.
    /// The world will automatically schedule it when the component storage is first accessed.
    ///
    /// # Example
    /// ```ignore
    /// let cleanup_system = TestComponent::cleanup_system(&mut world);
    /// world.add_system_instance(cleanup_system);
    /// world.build_scheduler();
    /// world.run();
    /// ```
    fn cleanup_system(
        _world: &mut crate::world::World,
    ) -> Box<dyn crate::scheduler::PipelineStage> {
        // This will be implemented by the Component derive macro
        // The macro generates a cleanup system and returns it as a boxed trait object
        panic!("cleanup_system must be implemented by the Component derive macro");
    }
}

pub trait Tag: Any
where
    Self: Sized,
{
}

#[derive(Default, Clone)]
pub struct Destroyed {}

impl crate::component::Resource for Destroyed {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}

impl crate::component::Component for Destroyed {
    const IS_TEMPORARY: bool = true;

    fn cleanup_system(
        world: &mut crate::world::World,
    ) -> Box<dyn crate::scheduler::PipelineStage> {
        use crate::scheduler::PipelineStage;
        Box::new(crate::system::DestroySystem::create(world))
    }
}

pub use rollback_macros::Component;
pub use rollback_macros::Tag;

#[cfg(test)]
#[path = "component.tests.rs"]
mod tests;
