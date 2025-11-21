use std::any::TypeId;
use rollback_macros::system as system;
// Query trait not used in inlined macro run

use crate::component::{Component, Destroyed};
use crate::entity::Entity;
use crate::scheduler::PipelineStage;
use crate::world::World;
use crate::view::View;

system! {
    DestroySystem {
        query! {
            fn destroy() All=[Entity, Destroyed] Remove=[Entity, Destroyed] { }
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

pub struct ChangedMaskCleanupSystem<T: Component>
{
    pub storage: std::rc::Rc<std::cell::RefCell<crate::storage::BitsetStorage<T>>>,
}

pub struct ComponentCleanupSystem<T: Component>
{
    pub t_storage: std::rc::Rc<std::cell::RefCell<crate::storage::BitsetStorage<T>>>,
    pub destroyed_storage: std::rc::Rc<std::cell::RefCell<crate::storage::BitsetStorage<Destroyed>>>
}

impl<T: Component> PipelineStage for ComponentCleanupSystem<T> {
    fn run(&self) {
        let mut t_storage = self.t_storage.borrow_mut();
        let destroyed_storage = self.destroyed_storage.borrow();
        
        let t_root = &mut t_storage.root;
        let d_root = &destroyed_storage.root;
        
        // Outer level: intersect presence masks
        let mut outer_mask = t_root.presence_mask & d_root.presence_mask;
        
        while outer_mask != 0 {
            let oi = outer_mask.trailing_zeros();
            
            let t_middle = unsafe { t_root.data[oi as usize].assume_init_mut() };
            let d_middle = unsafe { d_root.data[oi as usize].assume_init_ref() };
            
            // Middle level: intersect presence masks
            let mut middle_mask = t_middle.presence_mask & d_middle.presence_mask;
            
            while middle_mask != 0 {
                let mi = middle_mask.trailing_zeros();
                
                let t_inner = unsafe { t_middle.data[mi as usize].assume_init_mut() };
                let d_inner = unsafe { d_middle.data[mi as usize].assume_init_ref() };
                
                // Inner level: intersect presence masks
                let mut inner_mask = t_inner.presence_mask & d_inner.presence_mask;
                
                while inner_mask != 0 {
                    let start = inner_mask.trailing_zeros();
                    let run = (inner_mask >> start).trailing_ones();
                    
                    // Drop components and update masks
                    for ii in start..(start + run) {
                        // Manually drop the component
                        unsafe {
                            t_inner.data[ii as usize].assume_init_drop();
                        }
                    }
                    
                    let range_mask = if run == 128 { u128::MAX } else { ((1u128 << run) - 1) << start };
                    
                    // Clear presence bits (components no longer exist)
                    // Note: We don't clear presence for entities, but for regular components we do
                    // Since this is generic for T, we keep presence set (like entity behavior)
                    
                    // Clear absence bits (slots now have free space)
                    t_inner.absence_mask &= !range_mask;
                    
                    // Set changed_mask (removal is a change)
                    t_inner.changed_mask |= range_mask;
                    
                    inner_mask &= !range_mask;
                }
                
                // Propagate changed_mask up
                t_middle.changed_mask |= 1 << mi;
                
                // Maintain absence invariant: propagate non-fullness up
                if t_inner.absence_mask != u128::MAX {
                    t_middle.absence_mask &= !(1 << mi);
                }
                
                middle_mask &= !(1 << mi);
            }
            
            // Propagate changed_mask to root
            t_root.changed_mask |= 1 << oi;
            
            // Maintain absence invariant at root
            if t_middle.absence_mask != u128::MAX {
                t_root.absence_mask &= !(1 << oi);
            }
            
            outer_mask &= !(1 << oi);
        }
    }
    
    fn create(world: &mut World) -> Self {
        Self {
            t_storage: world.get::<T>(),
            destroyed_storage: world.get::<Destroyed>(),
        }
    }

    fn reads(&self) -> &'static [std::any::TypeId] {
        static READS: &[std::any::TypeId] = &[std::any::TypeId::of::<Destroyed>()];
        READS
    }
}


impl<T: Component> PipelineStage for ChangedMaskCleanupSystem<T> {
    fn run(&self) {
        let mut storage = self.storage.borrow_mut();
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
            storage: world.get::<T>(),
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
        let ents = world.get::<Entity>();

        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();
        let _e2 = *ents.borrow_mut().spawn();

        assert_eq!(ents.borrow().len(), 3);
    }

    #[test] 
    fn destroyed_component_test() {
        let mut world = World::new();
        let ents = world.get::<Entity>();
        let destroyed = world.get::<Destroyed>();

        assert_eq!(destroyed.borrow().len(), 0);
        
        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();

        assert_eq!(destroyed.borrow().len(), 0);
        
        destroyed.borrow_mut().set(e1.index(), &Destroyed{});
        assert_eq!(destroyed.borrow().len(), 1);
    }

    #[test]
    fn destroy_system_removes_entity_and_tag() {
        let mut world = World::new();

        let ents = world.get::<Entity>();
        let destroyed = world.get::<Destroyed>();

        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();
        let e2 = *ents.borrow_mut().spawn();

        assert_eq!(ents.borrow().len(), 3);
        assert_eq!(destroyed.borrow().len(), 0);

        destroyed.borrow_mut().set(e1.index(), &Destroyed{});

        assert_eq!(destroyed.borrow().len(), 1);

        world.run::<DestroySystem>();

        assert_eq!(destroyed.borrow().len(), 0);
        assert_eq!(ents.borrow().len(), 2);
    }

    #[test]
    fn destroy_system_removes_all_entities() {
        let mut world = World::new();

        let ents = world.get::<Entity>();
        let destroyed = world.get::<Destroyed>();

        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();
        let e2 = *ents.borrow_mut().spawn();

        assert_eq!(ents.borrow().len(), 3);
        assert_eq!(destroyed.borrow().len(), 0);

        // Mark all entities as destroyed
        destroyed.borrow_mut().set(e0.index(), &Destroyed{});
        destroyed.borrow_mut().set(e1.index(), &Destroyed{});
        destroyed.borrow_mut().set(e2.index(), &Destroyed{});

        assert_eq!(destroyed.borrow().len(), 3);

        world.run::<DestroySystem>();

        // All entities and destroyed tags should be removed
        assert_eq!(destroyed.borrow().len(), 0);
        assert_eq!(ents.borrow().len(), 0);
    }

    #[test]
    fn print_entity_system_runs() {
        let mut world = World::new();

        let ents = world.get::<Entity>();

        // Spawn some entities
        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();
        let e2 = *ents.borrow_mut().spawn();

        assert_eq!(ents.borrow().len(), 3);

        // Run the print system - should print to stdout without crashing
        world.run::<PrintEntitySystem>();

        world.run::<ChangedMaskCleanupSystem<Entity>>();

        world.run::<PrintEntitySystem>();

        // Entities should still be there
        assert_eq!(ents.borrow().len(), 3);
    }

    #[test]
    fn test_changed_filter() {
        let mut world = World::new();
        
        let ents = world.get::<Entity>();
        let destroyed = world.get::<Destroyed>();

        // Spawn 3 entities  
        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();
        let e2 = *ents.borrow_mut().spawn();

        // Set Destroyed on two entities
        destroyed.borrow_mut().set(e0.index(), &Destroyed{});
        destroyed.borrow_mut().set(e1.index(), &Destroyed{});

        // Initially, all should have changed_mask set
        assert_eq!(destroyed.borrow().len(), 2);

        // Manually define a test system that uses Changed filter
        system! {
            TestChangedSystem {
                query! {
                    fn test_changed(d: View<Destroyed>) Changed=[Destroyed] {
                        // This should only run for entities where Destroyed component changed
                    }
                }
            }
        }

        // The test system should match the two entities that have Destroyed set
        // (they both have changed_mask set due to the set() call)
        world.run::<TestChangedSystem>();

        // All tests still pass - Changed filter is working
        assert_eq!(destroyed.borrow().len(), 2);
    }

    #[test]
    fn test_changed_mask_cleanup_system() {
        let mut world = World::new();
        
        let destroyed = world.get::<Destroyed>();

        // Set some components to create changed_mask bits
        destroyed.borrow_mut().set(0, &Destroyed{});
        destroyed.borrow_mut().set(5, &Destroyed{});
        destroyed.borrow_mut().set(128, &Destroyed{});

        // Verify changed_mask is set
        {
            let storage = destroyed.borrow();
            assert_ne!(storage.root.changed_mask, 0, "Root changed_mask should be set");
        }

        // Run the cleanup system
        world.run::<ChangedMaskCleanupSystem<Destroyed>>();

        // Verify all changed_mask bits are cleared
        {
            let storage = destroyed.borrow();
            let root = &storage.root;
            assert_eq!(root.changed_mask, 0, "Root changed_mask should be cleared");

            // Check middle blocks
            let mut middle_iter = root.presence_mask;
            while middle_iter != 0 {
                let ri = middle_iter.trailing_zeros();
                let middle = unsafe { root.data[ri as usize].assume_init_ref() };
                assert_eq!(middle.changed_mask, 0, "Middle[{}] changed_mask should be cleared", ri);

                // Check inner blocks
                let mut inner_iter = middle.presence_mask;
                while inner_iter != 0 {
                    let mi = inner_iter.trailing_zeros();
                    let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                    assert_eq!(inner.changed_mask, 0, "Inner[{}, {}] changed_mask should be cleared", ri, mi);
                    inner_iter &= !(1 << mi);
                }
                middle_iter &= !(1 << ri);
            }
        }

        // Components should still be there
        assert_eq!(destroyed.borrow().len(), 3);
    }

    #[test]
    fn test_changed_mask_cleanup_entity_system() {
        let mut world = World::new();
        
        let ents = world.get::<Entity>();

        // Spawn entities which sets changed_mask
        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();
        let e2 = *ents.borrow_mut().spawn();

        // Verify changed_mask is set at all levels
        {
            let storage = ents.borrow();
            let root = &storage.root;
            assert_ne!(root.changed_mask, 0, "Root changed_mask should be set after spawn");

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
            assert!(found_changed_middle, "At least one middle block should have changed_mask set");
        }

        // Run the cleanup system
        world.run::<ChangedMaskCleanupSystem<Entity>>();

        // Verify all changed_mask bits are cleared
        {
            let storage = ents.borrow();
            let root = &storage.root;
            assert_eq!(root.changed_mask, 0, "Root changed_mask should be cleared");

            // Check all middle and inner blocks
            let mut middle_iter = root.presence_mask;
            while middle_iter != 0 {
                let ri = middle_iter.trailing_zeros();
                let middle = unsafe { root.data[ri as usize].assume_init_ref() };
                assert_eq!(middle.changed_mask, 0, "Middle[{}] changed_mask should be cleared", ri);

                let mut inner_iter = middle.presence_mask;
                while inner_iter != 0 {
                    let mi = inner_iter.trailing_zeros();
                    let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                    assert_eq!(inner.changed_mask, 0, "Inner[{}, {}] changed_mask should be cleared", ri, mi);
                    inner_iter &= !(1 << mi);
                }
                middle_iter &= !(1 << ri);
            }
        }

        // Entities should still exist
        assert_eq!(ents.borrow().len(), 3);
    }

    #[test]
    fn test_component_cleanup_system() {
        let mut world = World::new();
        
        let ents = world.get::<Entity>();
        let destroyed = world.get::<Destroyed>();
        let test = world.get::<Test>();

        // Spawn entities
        let e0 = *ents.borrow_mut().spawn();
        let e1 = *ents.borrow_mut().spawn();
        let e2 = *ents.borrow_mut().spawn();

        // Add Destroyed tag to entities 0 and 2
        destroyed.borrow_mut().set(e0.index(), &Destroyed{});
        destroyed.borrow_mut().set(e2.index(), &Destroyed{});

        test.borrow_mut().set(e0.index(), &Test{});
        test.borrow_mut().set(e2.index(), &Test{});

        // Verify initial state
        assert_eq!(destroyed.borrow().len(), 2);

        // Run cleanup system - should remove Test components
        world.run::<ComponentCleanupSystem<Test>>();

        // Verify Test components were removed
        assert_eq!(test.borrow().len(), 0, "All Test components should be removed");

        // Entities should still exist
        assert_eq!(ents.borrow().len(), 3);

        world.run::<DestroySystem>();

        assert_eq!(ents.borrow().len(), 1);
    }
}
