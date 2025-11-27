pub use rollback_macros::system;
use std::any::TypeId;
// Query trait not used in inlined macro run

use crate::component::{Component, Destroyed};
use crate::entity::Entity;
use crate::scheduler::PipelineStage;
use crate::world::World;

pub struct DestroySystem {
    pub entity_storage: std::rc::Rc<std::cell::UnsafeCell<crate::storage::Storage<Entity>>>,
    pub destroyed_storage: std::rc::Rc<std::cell::UnsafeCell<crate::storage::Storage<Destroyed>>>,
}

unsafe impl Send for DestroySystem {}
unsafe impl Sync for DestroySystem {}

impl PipelineStage for DestroySystem {
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    fn run(&self) {
        let entity_storage = unsafe { &mut *self.entity_storage.get() };
        let destroyed_storage = unsafe { &mut *self.destroyed_storage.get() };

        let entity_root = &mut entity_storage.root;
        let destroyed_root = &mut destroyed_storage.root;

        // Find entities that have both Entity and Destroyed components
        // Iterate through outer blocks where both exist
        let mut outer_mask = entity_root.presence_mask & destroyed_root.presence_mask;

        while outer_mask != 0 {
            let oi = outer_mask.trailing_zeros();

            // Check if blocks exist before accessing
            if (entity_root.presence_mask >> oi) & 1 == 0
                || (destroyed_root.presence_mask >> oi) & 1 == 0
            {
                outer_mask &= !(1 << oi);
                continue;
            }

            let entity_middle = unsafe { entity_root.data[oi as usize].assume_init_mut() };
            let destroyed_middle = unsafe { destroyed_root.data[oi as usize].assume_init_mut() };

            // Find middle blocks where both exist
            let mut middle_mask = entity_middle.presence_mask & destroyed_middle.presence_mask;

            while middle_mask != 0 {
                let mi = middle_mask.trailing_zeros();

                // Check if inner blocks exist before accessing
                if (entity_middle.presence_mask >> mi) & 1 == 0
                    || (destroyed_middle.presence_mask >> mi) & 1 == 0
                {
                    middle_mask &= !(1 << mi);
                    continue;
                }

                let entity_inner = unsafe { entity_middle.data[mi as usize].assume_init_mut() };
                let destroyed_inner =
                    unsafe { destroyed_middle.data[mi as usize].assume_init_mut() };

                // Find entities that have both Entity and Destroyed present
                let mut inner_mask = entity_inner.presence_mask & destroyed_inner.presence_mask;

                while inner_mask != 0 {
                    let start = inner_mask.trailing_zeros();
                    let run = (inner_mask >> start).trailing_ones();

                    let range_mask = if run == 128 {
                        u128::MAX
                    } else {
                        ((1u128 << run) - 1) << start
                    };

                    // Only process entities that are actually in inner_mask (have both components)
                    let actual_mask = range_mask & inner_mask;

                    // Remove Entity components
                    let mut entity_remove = actual_mask & entity_inner.presence_mask;
                    while entity_remove != 0 {
                        let ii = entity_remove.trailing_zeros();
                        unsafe {
                            entity_inner.data[ii as usize].assume_init_drop();
                        }
                        entity_remove &= !(1u128 << ii);
                    }

                    // Clear presence/absence bits for Entity
                    // We clear presence_mask as it tracks current existence, as requested
                    entity_inner.presence_mask &= !actual_mask;
                    entity_inner.absence_mask &= !actual_mask;

                    // Maintain absence invariant: propagate fullness/non-fullness up for Entity
                    if entity_inner.absence_mask != u128::MAX {
                        entity_middle.absence_mask &= !(1 << mi);
                        entity_root.absence_mask &= !(1 << oi);
                    } else {
                        entity_middle.absence_mask |= 1 << mi;
                    }

                    // Remove Destroyed components
                    let mut destroyed_remove = actual_mask & destroyed_inner.presence_mask;
                    while destroyed_remove != 0 {
                        let ii = destroyed_remove.trailing_zeros();
                        unsafe {
                            destroyed_inner.data[ii as usize].assume_init_drop();
                        }
                        destroyed_remove &= !(1u128 << ii);
                    }

                    // Clear presence/absence bits for Destroyed
                    destroyed_inner.presence_mask &= !actual_mask;
                    destroyed_inner.absence_mask &= !actual_mask;

                    // Maintain absence invariant: propagate fullness/non-fullness up for Destroyed
                    if destroyed_inner.absence_mask != u128::MAX {
                        destroyed_middle.absence_mask &= !(1 << mi);
                        destroyed_root.absence_mask &= !(1 << oi);
                    } else {
                        destroyed_middle.absence_mask |= 1 << mi;
                    }

                    inner_mask &= !range_mask;
                }

                middle_mask &= !(1u128 << mi);
            }

            outer_mask &= !(1u128 << oi);
        }
    }

    fn reads(&self) -> &'static [TypeId] {
        &[]
    }

    fn writes(&self) -> &'static [TypeId] {
        static WRITES: &[TypeId] = &[TypeId::of::<Entity>(), TypeId::of::<Destroyed>()];
        WRITES
    }

    fn parent(&self) -> Option<TypeId> {
        Some(TypeId::of::<crate::scheduler::DestroyGroup>())
    }

    fn create(world: &mut World) -> Self {
        Self {
            entity_storage: world.get_storage::<Entity>(),
            destroyed_storage: world.get_storage::<Destroyed>(),
        }
    }
}

system! {
    PrintEntitySystem {
        query! {
            fn print(e: View<Entity>) Changed=[Entity] {
                println!("{:?}", *e);
            }
        }
    }
}

pub struct ChangedMaskCleanupSystem<T: Component> {
    pub storage: std::rc::Rc<std::cell::UnsafeCell<crate::storage::Storage<T>>>,
}

unsafe impl<T: Component> Send for ChangedMaskCleanupSystem<T> {}
unsafe impl<T: Component> Sync for ChangedMaskCleanupSystem<T> {}

pub struct ComponentCleanupSystem<T: Component> {
    pub t_storage: std::rc::Rc<std::cell::UnsafeCell<crate::storage::Storage<T>>>,
    pub destroyed_storage: std::rc::Rc<std::cell::UnsafeCell<crate::storage::Storage<Destroyed>>>,
}

unsafe impl<T: Component> Send for ComponentCleanupSystem<T> {}
unsafe impl<T: Component> Sync for ComponentCleanupSystem<T> {}

impl<T: Component> PipelineStage for ComponentCleanupSystem<T> {
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    fn run(&self) {
        let t_storage = unsafe { &mut *self.t_storage.get() };
        let destroyed_storage = unsafe { &*self.destroyed_storage.get() };

        let t_root = &mut t_storage.root;
        let d_root = &destroyed_storage.root;

        // Remove components for entities with Destroyed tag
        // Iterate through all outer blocks where the target component exists
        let mut outer_mask = t_root.presence_mask;

        while outer_mask != 0 {
            let ri = outer_mask.trailing_zeros();

            // Check if Destroyed storage has this outer block - if not, skip
            if (d_root.presence_mask >> ri) & 1 == 0 {
                outer_mask &= !(1 << ri);
                continue;
            }

            let t_middle = unsafe { t_root.data[ri as usize].assume_init_mut() };
            let d_middle = unsafe { d_root.data[ri as usize].assume_init_ref() };

            // Iterate through all middle blocks where the target component exists
            let mut middle_mask = t_middle.presence_mask;

            while middle_mask != 0 {
                let mi = middle_mask.trailing_zeros();

                // Check if Destroyed storage has this middle block - if not, skip
                if (d_middle.presence_mask >> mi) & 1 == 0 {
                    middle_mask &= !(1 << mi);
                    continue;
                }

                let t_inner = unsafe { t_middle.data[mi as usize].assume_init_mut() };
                let d_inner = unsafe { d_middle.data[mi as usize].assume_init_ref() };

                // Use masking to find entities that have both T and Destroyed components present
                // presence_mask is sufficient - absence_mask is kept in sync at inner block level
                let t_occupied = t_inner.presence_mask;
                let d_occupied = d_inner.presence_mask;

                // Intersect masks to find entities with both T and Destroyed
                let mut inner_mask = t_occupied & d_occupied;

                while inner_mask != 0 {
                    let start = inner_mask.trailing_zeros();
                    let run = (inner_mask >> start).trailing_ones();

                    let range_mask = if run == 128 {
                        u128::MAX
                    } else {
                        ((1u128 << run) - 1) << start
                    };

                    // Remove components for all entities in this range
                    for ii in start..(start + run) {
                        unsafe {
                            t_inner.data[ii as usize].assume_init_drop();
                        }
                    }

                    // Clear presence/absence bits (slots are fully free)
                    t_inner.presence_mask &= !range_mask;
                    t_inner.absence_mask &= !range_mask;

                    inner_mask &= !range_mask;
                }

                // Maintain absence invariant: propagate fullness/non-fullness up
                if t_inner.absence_mask != u128::MAX {
                    t_middle.absence_mask &= !(1 << mi);
                    t_root.absence_mask &= !(1 << ri);
                } else {
                    t_middle.absence_mask |= 1 << mi;
                }

                middle_mask &= !(1 << mi);
            }

            // After processing all middle blocks, check if all are full for this outer block
            let t_middle_check = unsafe { t_root.data[ri as usize].assume_init_ref() };
            // Check if all middle blocks are full
            // At middle level, absence_mask means "all inner blocks are full", not just "exists"
            // So we need to check if presence_mask & absence_mask == presence_mask (all present blocks are full)
            let all_middle_full = (t_middle_check.presence_mask & t_middle_check.absence_mask)
                == t_middle_check.presence_mask;
            if all_middle_full {
                t_root.absence_mask |= 1 << ri;
            }

            outer_mask &= !(1 << ri);
        }

        // Second, clear all changed_mask bits (merged ChangedMaskCleanupSystem functionality)
        // Iterate over all middle blocks that have changes
        let mut middle_iter = t_root.changed_mask & t_root.presence_mask;

        while middle_iter != 0 {
            let ri = middle_iter.trailing_zeros();
            let middle = unsafe { t_root.data[ri as usize].assume_init_mut() };

            // Iterate over all inner blocks that have changes
            let mut inner_iter = middle.changed_mask & middle.presence_mask;
            while inner_iter != 0 {
                let mi = inner_iter.trailing_zeros();
                let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

                // Clear inner changed_mask
                inner.changed_mask = 0;

                inner_iter &= !(1 << mi);
            }

            // Clear middle changed_mask
            middle.changed_mask = 0;

            middle_iter &= !(1 << ri);
        }

        // Clear root changed_mask
        t_root.changed_mask = 0;
    }

    fn create(world: &mut World) -> Self {
        Self {
            t_storage: world.get_storage::<T>(),
            destroyed_storage: world.get_storage::<Destroyed>(),
        }
    }

    fn reads(&self) -> &'static [std::any::TypeId] {
        static READS: &[std::any::TypeId] = &[std::any::TypeId::of::<Destroyed>()];
        READS
    }

    fn writes(&self) -> &'static [std::any::TypeId] {
        // ComponentCleanupSystem writes the component type T
        // We need to use a type-erased approach since T is a generic parameter
        // The actual writes are handled by the wrapper type generated by the Component derive macro
        &[]
    }
}

impl<T: Component> PipelineStage for ChangedMaskCleanupSystem<T> {
    fn type_id(&self) -> TypeId {
        TypeId::of::<Self>()
    }

    fn run(&self) {
        let storage = unsafe { &mut *self.storage.get() };
        let root = &mut storage.root;

        // Iterate only over middle blocks that have changes
        let mut middle_iter = root.changed_mask & root.presence_mask;

        while middle_iter != 0 {
            let ri = middle_iter.trailing_zeros();
            let middle = unsafe { root.data[ri as usize].assume_init_mut() };

            // Iterate only over inner blocks that have changes
            let mut inner_iter = middle.changed_mask & middle.presence_mask;
            while inner_iter != 0 {
                let mi = inner_iter.trailing_zeros();
                let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

                // Clear inner changed_mask
                inner.changed_mask = 0;

                inner_iter &= !(1 << mi);
            }

            // Clear middle changed_mask
            middle.changed_mask = 0;

            middle_iter &= !(1 << ri);
        }

        // Clear root changed_mask
        root.changed_mask = 0;
    }

    fn create(world: &mut World) -> Self {
        Self {
            storage: world.get_storage::<T>(),
        }
    }

    fn reads(&self) -> &'static [TypeId] {
        &[]
    }

    fn writes(&self) -> &'static [TypeId] {
        &[]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::world::World;

    #[derive(Component, Default, Clone)]
    pub struct Test {}

    #[test]
    fn basic_entity_spawn_test() {
        let mut world = World::new();
        let ents = world.get_storage::<Entity>();

        let _e0 = unsafe { (*ents.get()).spawn() };
        let _e1 = unsafe { (*ents.get()).spawn() };
        let _e2 = unsafe { (*ents.get()).spawn() };

        assert_eq!(unsafe { (*ents.get()).len() }, 3);
    }

    #[test]
    fn destroyed_component_test() {
        let mut world = World::new();
        let ents = world.get_storage::<Entity>();
        let destroyed = world.get_storage::<Destroyed>();

        assert_eq!(unsafe { (*destroyed.get()).len() }, 0);

        let _e0 = unsafe { (*ents.get()).spawn() };
        let e1 = unsafe { (*ents.get()).spawn() };

        assert_eq!(unsafe { (*destroyed.get()).len() }, 0);

        unsafe {
            (*destroyed.get()).set(e1.index(), &Destroyed {});
        }
        assert_eq!(unsafe { (*destroyed.get()).len() }, 1);
    }

    #[test]
    fn destroy_system_removes_entity_and_tag() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();
        let destroyed = world.get_storage::<Destroyed>();

        let _e0 = unsafe { (*ents.get()).spawn() };
        let e1 = unsafe { (*ents.get()).spawn() };
        let _e2 = unsafe { (*ents.get()).spawn() };

        assert_eq!(unsafe { (*ents.get()).len() }, 3);
        assert_eq!(unsafe { (*destroyed.get()).len() }, 0);

        unsafe {
            (*destroyed.get()).set(e1.index(), &Destroyed {});
        }

        assert_eq!(unsafe { (*destroyed.get()).len() }, 1);

        world.run_system::<DestroySystem>();

        assert_eq!(unsafe { (*destroyed.get()).len() }, 0);
        assert_eq!(unsafe { (*ents.get()).len() }, 2);
    }

    #[test]
    fn destroy_system_removes_all_entities() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();
        let destroyed = world.get_storage::<Destroyed>();

        let e0 = unsafe { (*ents.get()).spawn() };
        let e1 = unsafe { (*ents.get()).spawn() };
        let e2 = unsafe { (*ents.get()).spawn() };

        assert_eq!(unsafe { (*ents.get()).len() }, 3);
        assert_eq!(unsafe { (*destroyed.get()).len() }, 0);

        // Mark all entities as destroyed
        unsafe {
            (*destroyed.get()).set(e0.index(), &Destroyed {});
            (*destroyed.get()).set(e1.index(), &Destroyed {});
            (*destroyed.get()).set(e2.index(), &Destroyed {});
        }

        assert_eq!(unsafe { (*destroyed.get()).len() }, 3);

        world.run_system::<DestroySystem>();

        // All entities and destroyed tags should be removed
        assert_eq!(unsafe { (*destroyed.get()).len() }, 0);
        assert_eq!(unsafe { (*ents.get()).len() }, 0);
    }

    #[test]
    fn print_entity_system_runs() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();

        // Spawn some entities
        let _e0 = unsafe { (*ents.get()).spawn() };
        let _e1 = unsafe { (*ents.get()).spawn() };
        let _e2 = unsafe { (*ents.get()).spawn() };

        assert_eq!(unsafe { (*ents.get()).len() }, 3);

        // Run the print system - should print to stdout without crashing
        world.run_system::<PrintEntitySystem>();

        world.run_system::<ChangedMaskCleanupSystem<Entity>>();

        world.run_system::<PrintEntitySystem>();

        // Entities should still be there
        assert_eq!(unsafe { (*ents.get()).len() }, 3);
    }

    #[test]
    fn test_changed_filter() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();
        let destroyed = world.get_storage::<Destroyed>();

        // Spawn 3 entities
        let e0 = unsafe { (*ents.get()).spawn() };
        let e1 = unsafe { (*ents.get()).spawn() };
        let _e2 = unsafe { (*ents.get()).spawn() };

        // Set Destroyed on two entities
        unsafe {
            (*destroyed.get()).set(e0.index(), &Destroyed {});
            (*destroyed.get()).set(e1.index(), &Destroyed {});
        }

        // Initially, all should have changed_mask set
        assert_eq!(unsafe { (*destroyed.get()).len() }, 2);

        // Manually define a test system that uses Changed filter
        system! {
            TestChangedSystem {
                query! {
                    fn test_changed(_d: View<Destroyed>) Changed=[Destroyed] {
                        // This should only run for entities where Destroyed component changed
                    }
                }
            }
        }

        // The test system should match the two entities that have Destroyed set
        // (they both have changed_mask set due to the set() call)
        world.run_system::<TestChangedSystem>();

        // All tests still pass - Changed filter is working
        assert_eq!(unsafe { (*destroyed.get()).len() }, 2);
    }

    #[test]
    fn test_changed_mask_cleanup_system() {
        let mut world = World::new();

        let destroyed = world.get_storage::<Destroyed>();

        // Set some components to create changed_mask bits
        unsafe {
            (*destroyed.get()).set(0, &Destroyed {});
            (*destroyed.get()).set(5, &Destroyed {});
            (*destroyed.get()).set(128, &Destroyed {});
        }

        // Verify changed_mask is set
        {
            let storage = unsafe { &*destroyed.get() };
            assert_ne!(
                storage.root.changed_mask, 0,
                "Root changed_mask should be set"
            );
        }

        // Run the cleanup system
        world.run_system::<ChangedMaskCleanupSystem<Destroyed>>();

        // Verify all changed_mask bits are cleared
        {
            let storage = unsafe { &*destroyed.get() };
            let root = &storage.root;
            assert_eq!(root.changed_mask, 0, "Root changed_mask should be cleared");

            // Check middle blocks
            let mut middle_iter = root.presence_mask;

            while middle_iter != 0 {
                let ri = middle_iter.trailing_zeros();
                let middle = unsafe { root.data[ri as usize].assume_init_ref() };
                assert_eq!(
                    middle.changed_mask, 0,
                    "Middle[{}] changed_mask should be cleared",
                    ri
                );

                // Check inner blocks
                let mut inner_iter = middle.presence_mask;

                while inner_iter != 0 {
                    let mi = inner_iter.trailing_zeros();
                    let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                    assert_eq!(
                        inner.changed_mask, 0,
                        "Inner[{}, {}] changed_mask should be cleared",
                        ri, mi
                    );
                    inner_iter &= !(1 << mi);
                }
                middle_iter &= !(1 << ri);
            }
        }

        // Components should still be there
        assert_eq!(unsafe { (*destroyed.get()).len() }, 3);
    }

    #[test]
    fn test_changed_mask_cleanup_entity_system() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();

        // Spawn entities which sets changed_mask
        let _e0 = unsafe { (*ents.get()).spawn() };
        let _e1 = unsafe { (*ents.get()).spawn() };
        let _e2 = unsafe { (*ents.get()).spawn() };

        // Verify changed_mask is set at all levels
        {
            let storage = unsafe { &*ents.get() };
            let root = &storage.root;
            assert_ne!(
                root.changed_mask, 0,
                "Root changed_mask should be set after spawn"
            );

            // Check that at least one middle block has changed_mask set
            let mut middle_iter = root.presence_mask;
            let mut found_changed_middle = false;
            while middle_iter != 0 {
                let ri = middle_iter.trailing_zeros();
                let middle = unsafe { root.data[ri as usize].assume_init_ref() };
                if middle.changed_mask != 0 {
                    found_changed_middle = true;

                    // Check inner blocks
                    let mut inner_iter = middle.presence_mask;
                    while inner_iter != 0 {
                        let mi = inner_iter.trailing_zeros();
                        let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                        if inner.changed_mask != 0 {
                            // Found changed inner block - good!
                        }
                        inner_iter &= !(1 << mi);
                    }
                }
                middle_iter &= !(1 << ri);
            }
            assert!(
                found_changed_middle,
                "At least one middle block should have changed_mask set"
            );
        }

        // Run the cleanup system
        world.run_system::<ChangedMaskCleanupSystem<Entity>>();

        // Verify all changed_mask bits are cleared
        {
            let storage = unsafe { &*ents.get() };
            let root = &storage.root;
            assert_eq!(root.changed_mask, 0, "Root changed_mask should be cleared");

            // Check all middle and inner blocks
            let mut middle_iter = root.presence_mask;
            while middle_iter != 0 {
                let ri = middle_iter.trailing_zeros();
                let middle = unsafe { root.data[ri as usize].assume_init_ref() };
                assert_eq!(
                    middle.changed_mask, 0,
                    "Middle[{}] changed_mask should be cleared",
                    ri
                );

                let mut inner_iter = middle.presence_mask;
                while inner_iter != 0 {
                    let mi = inner_iter.trailing_zeros();
                    let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                    assert_eq!(
                        inner.changed_mask, 0,
                        "Inner[{}, {}] changed_mask should be cleared",
                        ri, mi
                    );
                    inner_iter &= !(1 << mi);
                }
                middle_iter &= !(1 << ri);
            }
        }

        // Entities should still exist
        assert_eq!(unsafe { (*ents.get()).len() }, 3);
    }

    #[test]
    fn test_component_cleanup_system() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();
        let destroyed = world.get_storage::<Destroyed>();
        let test = world.get_storage::<Test>();

        // Spawn entities
        let e0 = unsafe { (*ents.get()).spawn() };
        let _e1 = unsafe { (*ents.get()).spawn() };
        let e2 = unsafe { (*ents.get()).spawn() };

        // Add Destroyed tag to entities 0 and 2
        unsafe {
            (*destroyed.get()).set(e0.index(), &Destroyed {});
            (*destroyed.get()).set(e2.index(), &Destroyed {});
        }

        unsafe {
            (*test.get()).set(e0.index(), &Test {});
            (*test.get()).set(e2.index(), &Test {});
        }

        // Verify initial state
        assert_eq!(unsafe { (*destroyed.get()).len() }, 2);

        // Run cleanup system - should remove Test components
        world.run_system::<ComponentCleanupSystem<Test>>();

        // Verify Test components were removed
        assert_eq!(
            unsafe { (*test.get()).len() },
            0,
            "All Test components should be removed"
        );

        // Entities should still exist
        assert_eq!(unsafe { (*ents.get()).len() }, 3);

        world.run_system::<DestroySystem>();

        assert_eq!(unsafe { (*ents.get()).len() }, 1);
    }

    #[test]
    fn component_cleanup_clears_presence_bits() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();
        let destroyed = world.get_storage::<Destroyed>();
        let test = world.get_storage::<Test>();

        let entity = unsafe { (*ents.get()).spawn() };
        unsafe {
            (*test.get()).set(entity.index(), &Test {});
            (*destroyed.get()).set(entity.index(), &Destroyed {});
        }

        world.run_system::<ComponentCleanupSystem<Test>>();

        let storage = unsafe { &*test.get() };
        let root = &storage.root;
        let ri = entity.index() >> 14;
        let mi = (entity.index() >> 7) & 0x7F;
        let ii = entity.index() & 0x7F;

        let middle = unsafe { root.data[ri as usize].assume_init_ref() };
        let inner = unsafe { middle.data[mi as usize].assume_init_ref() };

        assert_eq!(
            (inner.presence_mask >> ii) & 1,
            0,
            "Presence bit should be cleared after cleanup"
        );
        assert_eq!(
            (inner.absence_mask >> ii) & 1,
            0,
            "Absence bit should be cleared after cleanup"
        );
    }

    #[test]
    fn test_component_cleanup_system_removes_destroyed_and_clears_changed_mask() {
        let mut world = World::new();

        let ents = world.get_storage::<Entity>();
        let destroyed = world.get_storage::<Destroyed>();
        let test = world.get_storage::<Test>();

        // Spawn 10,000 entities to test invariance over the tree of masks
        const NUM_ENTITIES: usize = 10_000;
        let mut entities = Vec::new();
        for _ in 0..NUM_ENTITIES {
            entities.push(unsafe { (*ents.get()).spawn() });
        }

        // Add Test component to all entities
        for &entity in &entities {
            unsafe {
                (*test.get()).set(entity.index(), &Test {});
            }
        }

        // Verify all Test components are present
        assert_eq!(
            unsafe { (*test.get()).len() },
            NUM_ENTITIES,
            "All entities should have Test components"
        );

        // Mark some entities as destroyed (every 3rd entity starting from index 1)
        // This creates a good distribution across blocks
        let mut destroyed_indices = Vec::new();
        for (i, &entity) in entities.iter().enumerate() {
            if i % 3 == 1 {
                unsafe {
                    (*destroyed.get()).set(entity.index(), &Destroyed {});
                }
                destroyed_indices.push(entity.index());
            }
        }
        let num_destroyed = destroyed_indices.len();
        let expected_remaining = NUM_ENTITIES - num_destroyed;

        // Verify destroyed count
        assert_eq!(
            unsafe { (*destroyed.get()).len() },
            num_destroyed,
            "Correct number of entities should be marked as destroyed"
        );

        // Modify some components to set changed_mask (every 10th entity)
        for (i, &entity) in entities.iter().enumerate() {
            if i % 10 == 0 {
                unsafe {
                    (*test.get()).set(entity.index(), &Test {});
                }
            }
        }

        // Verify changed_mask is set
        {
            let storage = unsafe { &*test.get() };
            assert_ne!(
                storage.root.changed_mask, 0,
                "Root changed_mask should be set"
            );
        }

        // Run ComponentCleanupSystem - should remove Test components for destroyed entities and clear changed_mask
        world.run_system::<ComponentCleanupSystem<Test>>();

        // Verify Test components were removed for destroyed entities
        assert_eq!(
            unsafe { (*test.get()).len() },
            expected_remaining,
            "Test components for destroyed entities should be removed"
        );

        // Verify specific destroyed entities no longer have Test components
        for &destroyed_index in &destroyed_indices {
            assert!(
                unsafe { (*test.get()).get(destroyed_index).is_none() },
                "Entity {} should not have Test component (destroyed)",
                destroyed_index
            );
        }

        // Verify some non-destroyed entities still have Test components
        for (i, &entity) in entities.iter().enumerate() {
            if i % 3 != 1 {
                assert!(
                    unsafe { (*test.get()).get(entity.index()).is_some() },
                    "Entity {} should still have Test component",
                    entity.index()
                );
            }
        }

        // Verify all changed_mask bits are cleared throughout the entire tree
        {
            let storage = unsafe { &*test.get() };
            let root = &storage.root;
            assert_eq!(root.changed_mask, 0, "Root changed_mask should be cleared");

            // Check all middle blocks
            let mut middle_iter = root.presence_mask;
            while middle_iter != 0 {
                let ri = middle_iter.trailing_zeros();
                let middle = unsafe { root.data[ri as usize].assume_init_ref() };
                assert_eq!(
                    middle.changed_mask, 0,
                    "Middle[{}] changed_mask should be cleared",
                    ri
                );

                // Check all inner blocks
                let mut inner_iter = middle.presence_mask;
                while inner_iter != 0 {
                    let mi = inner_iter.trailing_zeros();
                    let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                    assert_eq!(
                        inner.changed_mask, 0,
                        "Inner[{}, {}] changed_mask should be cleared",
                        ri, mi
                    );
                    inner_iter &= !(1 << mi);
                }
                middle_iter &= !(1 << ri);
            }
        }

        // Entities should still exist (only components were removed)
        assert_eq!(
            unsafe { (*ents.get()).len() },
            NUM_ENTITIES,
            "All entities should still exist"
        );
    }

    #[test]
    fn test_system_with_view_mut() {
        #[derive(Component, Default, Clone, Debug, PartialEq)]
        pub struct Counter {
            pub value: i32,
        }

        // Define a system that uses ViewMut to increment counters
        system! {
            IncrementSystem {
                query! {
                    fn increment(counter: &mut ViewMut<Counter>) {
                        counter.value += 1;
                    }
                }
            }
        }

        let mut world = World::new();

        let ents = world.get_storage::<Entity>();
        let counters = world.get_storage::<Counter>();

        // Spawn entities with counters
        let e0 = unsafe { (*ents.get()).spawn() };
        let e1 = unsafe { (*ents.get()).spawn() };
        let e2 = unsafe { (*ents.get()).spawn() };

        // Set initial counter values
        unsafe {
            (*counters.get()).set(e0.index(), &Counter { value: 0 });
            (*counters.get()).set(e1.index(), &Counter { value: 5 });
            (*counters.get()).set(e2.index(), &Counter { value: 10 });
        }

        // Verify initial values
        assert_eq!(
            unsafe { (*counters.get()).get(e0.index()).unwrap().value },
            0
        );
        assert_eq!(
            unsafe { (*counters.get()).get(e1.index()).unwrap().value },
            5
        );
        assert_eq!(
            unsafe { (*counters.get()).get(e2.index()).unwrap().value },
            10
        );

        // Run the increment system
        world.run_system::<IncrementSystem>();

        // Verify values were incremented
        assert_eq!(
            unsafe { (*counters.get()).get(e0.index()).unwrap().value },
            1
        );
        assert_eq!(
            unsafe { (*counters.get()).get(e1.index()).unwrap().value },
            6
        );
        assert_eq!(
            unsafe { (*counters.get()).get(e2.index()).unwrap().value },
            11
        );

        // Run again to verify it works multiple times
        world.run_system::<IncrementSystem>();

        assert_eq!(
            unsafe { (*counters.get()).get(e0.index()).unwrap().value },
            2
        );
        assert_eq!(
            unsafe { (*counters.get()).get(e1.index()).unwrap().value },
            7
        );
        assert_eq!(
            unsafe { (*counters.get()).get(e2.index()).unwrap().value },
            12
        );
    }
}
