use std::any::Any;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::cell::RefCell;
use crate::component::{Component, Destroyed};
use crate::scheduler::PipelineStage;
use crate::entity::Entity;
use crate::storage::BitsetStorage;

pub struct World {
    pub storages: [MaybeUninit<Box<dyn Any>>; 128],
    pub mask: u128
}

impl World {
    pub fn new() -> Self {
        World {
            storages: std::array::from_fn(|_| MaybeUninit::uninit()),
            mask: 0,
        }
    }
    
    pub fn get<T: Component>(&mut self) -> Rc<RefCell<BitsetStorage<T>>> {
        let id = T::type_index();

        if id >= 128 { panic!("invalid component type index") }

        let bit = 1u128 << id;

        if (self.mask & bit) == 0 {
            let rc = Rc::new(RefCell::new(BitsetStorage::<T>::new()));
            self.storages[id] = MaybeUninit::new(Box::new(rc.clone()) as Box<dyn Any>);
            self.mask |= bit;
            return rc;
        }

        let any = unsafe { self.storages[id].assume_init_ref() };

        unsafe {
            let raw = any.as_ref() as *const dyn Any as *const Rc<RefCell<BitsetStorage<T>>>;

            (*raw).clone()
        }
    }

    pub fn run<T: PipelineStage>(&mut self) {
        T::create(self).run();
    }

    pub fn schedule<T: PipelineStage>(&mut self) {
        T::create(self).run();
    }

    pub fn destroy(&mut self, entity: Entity) {
        let ents = self.get::<Entity>();
        let current = ents.borrow().get(entity.index()).cloned();
        
        if let Some(current_entity) = current {
            assert_eq!(current_entity, entity, "Attempted to destroy an entity that does not match the current entity at index {}", entity.index());
            self.get::<Destroyed>().borrow_mut().set(entity.index(), &Destroyed {});
        } else {
            panic!("Attempted to destroy entity {} which does not exist", entity.index());
        }
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
                    ptr.add((start + i) as usize).read().assume_init_drop();
                }
                
                let range_mask = if run == 128 { u128::MAX } else { ((1u128 << run) - 1) << start };
                mask &= !range_mask;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::DestroySystem;

    #[test]
    fn test_world_destroy() {
        let mut world = World::new();
        let ents = world.get::<Entity>();

        // Spawn an entity
        let e0 = *ents.borrow_mut().spawn();
        
        // Verify it exists
        assert_eq!(ents.borrow().len(), 1);
        assert!(ents.borrow().get(e0.index()).is_some());

        // Destroy it
        world.destroy(e0);

        // Verify it's marked as destroyed (Destroyed component added)
        let destroyed = world.get::<Destroyed>();
        assert!(destroyed.borrow().get(e0.index()).is_some());

        // Run DestroySystem
        world.run::<DestroySystem>();

        // Verify it's gone
        assert_eq!(ents.borrow().len(), 0);
        assert!(ents.borrow().get(e0.index()).is_none());
    }
}
