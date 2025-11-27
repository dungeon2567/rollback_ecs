use crate::component::{Component, Destroyed};
use crate::entity::Entity;
use crate::rollback::StorageLike;
use crate::scheduler::{PipelineStage, Scheduler};
use crate::storage::Storage;
use crate::tick::Tick;
use std::any::Any;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::rc::Rc;

pub struct World {
    pub storages: [MaybeUninit<Box<dyn StorageLike>>; 128],
    pub mask: u128,
    scheduler: Option<Scheduler>,
    pending_systems: Vec<Box<dyn PipelineStage>>,
    current_tick: Tick,
}

impl World {
    /// Creates a new World with systems created from the provided closure.
    /// The scheduler is automatically built from the created systems.
    ///
    /// # Example
    /// ```ignore
    /// let mut world = World::new_with_systems(|world| {
    ///     vec![
    ///         Box::new(SystemA::create(world)) as Box<dyn PipelineStage>,
    ///         Box::new(SystemB::create(world)) as Box<dyn PipelineStage>,
    ///     ]
    /// });
    /// world.run();
    /// ```
    pub fn new_with_systems<F>(systems_fn: F) -> Self
    where
        F: FnOnce(&mut World) -> Vec<Box<dyn PipelineStage>>,
    {
        let mut world = World {
            storages: std::array::from_fn(|_| MaybeUninit::uninit()),
            mask: 0,
            scheduler: None,
            pending_systems: Vec::new(),
            current_tick: Tick::new(0),
        };

        // Create systems using the provided closure
        let systems = systems_fn(&mut world);

        // Build scheduler from created systems
        world.scheduler = Some(Scheduler::new(systems));

        world
    }

    /// Creates a new empty World without systems.
    /// This is mainly for testing or when you need to add systems later.
    /// For production use, prefer `new_with_systems()`.
    ///
    /// Note: DestroySystem is automatically added when Entity storage is first accessed
    /// (via Entity component's cleanup_system).
    pub fn new() -> Self {
        World {
            storages: std::array::from_fn(|_| MaybeUninit::uninit()),
            mask: 0,
            scheduler: None,
            pending_systems: Vec::new(),
            current_tick: Tick::new(0),
        }
    }

    pub fn get_storage<T: Component>(&mut self) -> Rc<UnsafeCell<Storage<T>>> {
        let id = T::type_index();

        if id >= 128 {
            panic!("invalid component type index")
        }

        let bit = 1u128 << id;

        if (self.mask & bit) == 0 {
            let rc = Rc::new(UnsafeCell::new(Storage::<T>::new()));
            self.storages[id] = MaybeUninit::new(Box::new(rc.clone()) as Box<dyn StorageLike>);
            self.mask |= bit;

            if !T::IS_TEMPORARY {
                let cleanup_system = T::cleanup_system(self);
                self.add_system_instance(cleanup_system);
            }

            return rc;
        }

        let storage_like = unsafe { self.storages[id].assume_init_ref() };

        unsafe {
            let raw = storage_like.as_any() as *const dyn Any as *const Rc<UnsafeCell<Storage<T>>>;

            (*raw).clone()
        }
    }

    pub fn run_system<T: PipelineStage>(&mut self) {
        T::create(self).run();
    }

    pub fn schedule<T: PipelineStage>(&mut self) {
        T::create(self).run();
    }

    /// Adds a system to the pending systems list.
    /// The system will be included when `build_scheduler()` is called.
    ///
    /// # Example
    /// ```ignore
    /// world.add_system::<MySystem>();
    /// world.add_system::<AnotherSystem>();
    /// world.build_scheduler();
    /// world.run();
    /// ```
    pub fn add_system<T: PipelineStage>(&mut self) {
        let system = T::create(self);
        self.pending_systems.push(Box::new(system));
    }

    /// Adds a system instance to the pending systems list.
    /// This allows adding systems that have already been created.
    ///
    /// # Example
    /// ```ignore
    /// let system = MySystem::create(&mut world);
    /// world.add_system_instance(Box::new(system));
    /// ```
    pub fn add_system_instance(&mut self, system: Box<dyn PipelineStage>) {
        self.pending_systems.push(system);
    }

    /// Builds the scheduler from all pending systems.
    /// This will panic if there are circular dependencies or non-deterministic ordering.
    /// After building, the scheduler is ready to run and pending systems are cleared.
    /// The scheduler is built once and can be reused every tick by calling `run()`.
    /// DestroySystem is always scheduled to run last, regardless of dependencies.
    ///
    /// # Panics
    /// Panics if there's a circular dependency or any source of non-determinism.
    ///
    /// # Example
    /// ```ignore
    /// let mut world = World::new();
    /// world.add_system::<SystemA>();
    /// world.add_system::<SystemB>();
    /// world.build_scheduler();
    /// // Every tick:
    /// world.run();
    /// ```
    ///
    /// Note: If you're creating a World with systems, prefer `new_with_systems()` which
    /// automatically builds the scheduler.
    pub fn build_scheduler(&mut self) {
        let systems = std::mem::take(&mut self.pending_systems);
        self.scheduler = Some(Scheduler::new(systems));
    }

    /// Runs the scheduler and increments the world tick.
    /// This increments the current tick and updates all storages with the new tick.
    ///
    /// # Panics
    /// Panics if the scheduler has not been built yet. Call `build_scheduler()` first.
    ///
    /// # Example
    /// ```ignore
    /// world.add_system::<MySystem>();
    /// world.build_scheduler();
    /// world.run(); // Tick 0 -> 1
    /// world.run(); // Tick 1 -> 2
    /// ```
    pub fn run(&mut self) {
        if let Some(ref scheduler) = self.scheduler {
            scheduler.run();
        } else {
            panic!("Scheduler has not been built. Call build_scheduler() first.");
        }

        // Increment tick
        self.current_tick = Tick::new(self.current_tick.value().wrapping_add(1));

        // Update all storages with the new tick
        let mut mask = self.mask;
        while mask != 0 {
            let start = mask.trailing_zeros();
            let run = (mask >> start).trailing_ones();

            for i in 0..run {
                let idx = (start + i) as usize;
                unsafe {
                    let storage = self.storages[idx].assume_init_ref();
                    storage.set_tick(self.current_tick);
                }
            }

            let range_mask = if run == 128 {
                u128::MAX
            } else {
                ((1u128 << run) - 1) << start
            };
            mask &= !range_mask;
        }
    }

    /// Runs the scheduler sequentially and increments the world tick.
    /// This is the sequential version that executes systems one by one.
    /// Use this when you need deterministic sequential execution or when systems
    /// are not thread-safe.
    ///
    /// This increments the current tick and updates all storages with the new tick.
    ///
    /// # Panics
    /// Panics if the scheduler has not been built yet. Call `build_scheduler()` first.
    ///
    /// # Example
    /// ```ignore
    /// world.add_system::<MySystem>();
    /// world.build_scheduler();
    /// world.run_sequential(); // Tick 0 -> 1 (sequential execution)
    /// world.run_sequential(); // Tick 1 -> 2 (sequential execution)
    /// ```
    pub fn run_sequential(&mut self) {
        if let Some(ref scheduler) = self.scheduler {
            scheduler.run_sequential();
        } else {
            panic!("Scheduler has not been built. Call build_scheduler() first.");
        }

        // Increment tick
        self.current_tick = Tick::new(self.current_tick.value().wrapping_add(1));

        // Update all storages with the new tick
        let mut mask = self.mask;
        while mask != 0 {
            let start = mask.trailing_zeros();
            let run = (mask >> start).trailing_ones();

            for i in 0..run {
                let idx = (start + i) as usize;
                unsafe {
                    let storage = self.storages[idx].assume_init_ref();
                    storage.set_tick(self.current_tick);
                }
            }

            let range_mask = if run == 128 {
                u128::MAX
            } else {
                ((1u128 << run) - 1) << start
            };
            mask &= !range_mask;
        }
    }

    /// Returns the current world tick.
    pub fn current_tick(&self) -> Tick {
        self.current_tick
    }

    /// Returns a reference to the scheduler if it has been built.
    pub fn scheduler(&self) -> Option<&Scheduler> {
        self.scheduler.as_ref()
    }

    /// Schedules the cleanup system for a component type on the world's scheduler.
    /// This is a convenience method that calls `Component::cleanup_system` and adds it to pending systems.
    /// If the storage for this component has already been accessed, the cleanup system is already
    /// auto-scheduled, so this is a no-op.
    ///
    /// # Example
    /// ```ignore
    /// world.schedule_cleanup::<TestComponent>();
    /// world.build_scheduler();
    /// world.run();
    /// ```
    pub fn schedule_cleanup<T: Component>(&mut self) {
        let id = T::type_index();
        let bit = 1u128 << id;

        // If storage already exists, cleanup was auto-scheduled when storage was first accessed
        if (self.mask & bit) != 0 {
            return;
        }

        // Storage doesn't exist yet, so create and schedule cleanup
        let cleanup_system = T::cleanup_system(self);
        self.add_system_instance(cleanup_system);
    }

    pub fn set<T: Component>(&mut self, entity: Entity, component: &T)
    where
        T: Clone,
    {
        let ents = self.get_storage::<Entity>();
        let current = unsafe { (*ents.get()).get(entity.index()) };

        if let Some(current_entity) = current {
            if current_entity.generation() != entity.generation() {
                panic!(
                    "Attempted to set component on entity that does not match the current entity at index {} (expected generation {}, got {})",
                    entity.index(),
                    current_entity.generation(),
                    entity.generation()
                );
            }

            let storage = self.get_storage::<T>();
            unsafe {
                (*storage.get()).set(entity.index(), component);
            }
        } else {
            panic!(
                "Attempted to set component on entity {} which does not exist",
                entity.index()
            );
        }
    }

    pub fn destroy(&mut self, entity: Entity) {
        let ents = self.get_storage::<Entity>();
        let current = unsafe { (*ents.get()).get(entity.index()) };

        if let Some(current_entity) = current {
            if current_entity.generation() != entity.generation() {
                panic!(
                    "Attempted to destroy an entity that does not match the current entity at index {} (expected generation {}, got {})",
                    entity.index(),
                    current_entity.generation(),
                    entity.generation()
                );
            }
            let destroyed = self.get_storage::<Destroyed>();
            unsafe {
                (*destroyed.get()).set(entity.index(), &Destroyed {});
            }
        } else {
            panic!(
                "Attempted to destroy entity {} which does not exist",
                entity.index()
            );
        }
    }

    pub fn spawn(&mut self) -> Entity {
        unsafe { (*self.get_storage::<Entity>().get()).spawn() }
    }

    pub fn rollback(&mut self, target_tick: Tick) {
        let mut mask = self.mask;

        while mask != 0 {
            let start = mask.trailing_zeros();
            let run = (mask >> start).trailing_ones();

            for i in 0..run {
                let idx = (start + i) as usize;
                unsafe {
                    let storage = self.storages[idx].assume_init_ref();
                    storage.rollback(target_tick);
                }
            }

            let range_mask = if run == 128 {
                u128::MAX
            } else {
                ((1u128 << run) - 1) << start
            };
            mask &= !range_mask;
        }

        // Update world's current tick to match the target tick after rollback
        self.current_tick = target_tick;
    }

}

impl Drop for World {
    fn drop(&mut self) {
        let mut mask = self.mask;

        unsafe {
            let ptr = self.storages.as_mut_ptr();

            while mask != 0 {
                let start = mask.trailing_zeros();
                let run = (mask >> start).trailing_ones();

                for i in 0..run {
                    let idx = (start + i) as usize;
                    ptr.add(idx).read().assume_init_drop();
                }

                let range_mask = if run == 128 {
                    u128::MAX
                } else {
                    ((1u128 << run) - 1) << start
                };
                mask &= !range_mask;
            }
        }
    }
}

#[cfg(test)]
#[path = "world.tests.rs"]
mod tests;
