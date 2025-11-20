use crate::storage::block::Block;
use crate::storage::view::ViewMut;

pub struct Storage<T>
{
    pub root: Block<Box<Block<Box<Block<T>>>>>
}

impl<T> Storage<T> {
    pub fn new() -> Self {
        Storage {
            root: Block::new()
        }
    }

    pub fn len(&self) -> usize {
        let root = &self.root;
        let mut count = 0;

        // 1. Count fully occupied middle blocks
        count += root.absence_mask.count_ones() as usize * 16384;

        // 2. Iterate partially occupied middle blocks
        let mut partial_root = root.presence_mask & !root.absence_mask;
        while partial_root != 0 {
            let ri = partial_root.trailing_zeros();
            let middle = unsafe { root.data[ri as usize].assume_init_ref() };

            // 2a. Count fully occupied inner blocks
            count += middle.absence_mask.count_ones() as usize * 128;

            // 2b. Iterate partially occupied inner blocks
            let mut partial_middle = middle.presence_mask & !middle.absence_mask;
            
            while partial_middle != 0 {
                let mi = partial_middle.trailing_zeros();
                let inner = unsafe { middle.data[mi as usize].assume_init_ref() };

                // 2c. Count items in inner block
                count += inner.absence_mask.count_ones() as usize;

                partial_middle &= !(1 << mi);
            }

            partial_root &= !(1 << ri);
        }

        count
    }

    pub fn set(&mut self, index: u32, value: &T) where T: Clone
    {
        // Decode global index to ri, mi, ii
        // ri (0..128) * 16384 + mi (0..128) * 128 + ii (0..128)
        let ri = index / 16384;
        let mi = (index % 16384) / 128;
        let ii = index % 128;

        // Validate index is in bounds
        if ri >= 128 {
            panic!("Index out of bounds: {}", index);
        }

        let root = &mut self.root;

        // Ensure middle block exists
        root.ensure_child_exists(ri);

        let middle = unsafe { root.data[ri as usize].assume_init_mut() };

        // Ensure inner block exists
        if (middle.presence_mask >> mi) & 1 == 0 {
            // Create new inner block
            let new_inner = Block::new();

            middle.data[mi as usize].write(Box::new(new_inner));

            middle.presence_mask |= 1 << mi;
        }

        let inner = unsafe {
            middle.data[mi as usize].assume_init_mut()
        };

        // Set the value
        let slot = unsafe {
            inner.data[ii as usize].assume_init_mut()
        };

        *slot = value.clone();
        
        // Update presence and absence masks
        inner.presence_mask |= 1 << ii;
        inner.absence_mask |= 1 << ii;
        
        // Mark as changed
        inner.changed_mask |= 1 << ii;
        
        // Propagate changed_mask up the hierarchy
        middle.changed_mask |= 1 << mi;
        root.changed_mask |= 1 << ri;
    }

    pub fn remove(&mut self, index: u32) {
        // Decode global index to ri, mi, ii
        let ri = index / 16384;
        let mi = (index % 16384) / 128;
        let ii = index % 128;

        let root = &mut self.root;
        if (root.presence_mask >> ri) & 1 == 0 {
            return; // Middle block doesn't exist
        }

        let middle = unsafe { root.data[ri as usize].assume_init_mut() };
        if (middle.presence_mask >> mi) & 1 == 0 {
            return; // Inner block doesn't exist
        }

        let inner = unsafe { middle.data[mi as usize].assume_init_mut() };
        
        // Check if component actually exists before removing
        if (inner.presence_mask >> ii) & 1 == 0 {
            return; // Component doesn't exist, nothing to remove
        }
        
        // NOTE: We do NOT clear presence_mask here
        // For entities: presence_mask tracks initialization, not current existence
        // This allows entity generation to persist across delete/respawn cycles
        // For regular components: keeping presence_mask set is harmless and
        // allows us to track that the slot has been used
        
        // Clear the absence bit (slot now has free space)
        inner.absence_mask &= !(1 << ii);
        
        // Mark as changed (removal is a change)
        inner.changed_mask |= 1 << ii;
        
        // Maintain invariant: propagate non-fullness up the hierarchy
        if inner.absence_mask != u128::MAX {
            middle.absence_mask &= !(1 << mi);
        }
        
        if middle.absence_mask != u128::MAX {
            root.absence_mask &= !(1 << ri);
        }
        
        // Propagate changed_mask up the hierarchy
        middle.changed_mask |= 1 << mi;
        root.changed_mask |= 1 << ri;
    }

    pub fn get(&self, index: u32) -> Option<&T> {
        let ri = index / 16384;
        let mi = (index % 16384) / 128;
        let ii = index % 128;

        let root = &self.root;
        if (root.presence_mask >> ri) & 1 == 0 {
            return None;
        }

        let middle = unsafe { root.data[ri as usize].assume_init_ref() };
        if (middle.presence_mask >> mi) & 1 == 0 {
            return None;
        }

        let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
        
        // Check if component is currently occupied (alive)
        if (inner.absence_mask >> ii) & 1 == 0 {
            return None;
        }

        unsafe { Some(inner.data[ii as usize].assume_init_ref()) }
    }
}

use crate::entity::Entity;

impl Storage<Entity> {
    pub fn spawn(&mut self) -> &Entity {
        let root = &mut self.root;
        
        // 1. Find free slot in root
        let free_root = !root.absence_mask;
        if free_root == 0 {
            panic!("Storage is full");
        }
        let ri = free_root.trailing_zeros();
        
        root.ensure_child_exists(ri);
        
        let mi;
        let ii;

        {
            let middle = unsafe { root.data[ri as usize].assume_init_mut() };

            // 2. Find free slot in middle
            let free_middle = !middle.absence_mask;
            if free_middle == 0 {
                 panic!("Storage inconsistency: Root said free, Middle is full");
            }
            mi = free_middle.trailing_zeros();

            // Ensure inner block exists
            if (middle.presence_mask >> mi) & 1 == 0 {
                let new_inner = Block::new();
                middle.data[mi as usize].write(Box::new(new_inner));
                middle.presence_mask |= 1 << mi;
                // Ensure the new inner block is not marked as full (it's empty)
                middle.absence_mask &= !(1 << mi);
            }
            
            {
                let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

                // 3. Find free slot in inner
                // For entities, we need to find a slot that is not occupied
                // We can reuse slots that were previously occupied but are now free
                let free_inner = !inner.absence_mask;
                if free_inner == 0 {
                     panic!("Storage inconsistency: Middle said free, Inner is full");
                }
                ii = free_inner.trailing_zeros();

                // Initialize or update the entity
                let global_index = ri * 16384 + mi * 128 + ii;
                if (inner.presence_mask >> ii) & 1 == 0 {
                    // First time initializing this slot
                    inner.data[ii as usize].write(Entity::new(global_index, 0));
                    inner.presence_mask |= 1 << ii;
                }
                
                // Increment generation for the allocated entity
                let entity = unsafe { inner.data[ii as usize].assume_init_mut() };
                entity.increment_generation();
                
                // Mark as occupied
                inner.absence_mask |= 1 << ii;
                
                // Mark as changed - spawning/respawning is a change
                inner.changed_mask |= 1 << ii;
                
                // Maintain invariant: propagate fullness up the hierarchy
                if inner.absence_mask == u128::MAX {
                    middle.absence_mask |= 1 << mi;
                }
            }
            
            // Maintain invariant: propagate fullness to root
            if middle.absence_mask == u128::MAX {
                root.absence_mask |= 1 << ri;
            }
            
            // Propagate changed_mask up the hierarchy
            middle.changed_mask |= 1 << mi;
        }
        
        root.changed_mask |= 1 << ri;

        // Re-traverse to return the reference.
        unsafe {
            let middle = root.data[ri as usize].assume_init_mut();
            let inner = middle.data[mi as usize].assume_init_mut();
            
            inner.data[ii as usize].assume_init_ref()
        }
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::Component;
    use crate::storage::block::Block;

    /// Verify tree invariants for the storage hierarchy
    fn verify_tree_invariants<T>(storage: &Storage<T>) {
        let root = &storage.root;
        
        // Invariant 1: absence_mask must be subset of presence_mask
        assert_eq!(
            root.absence_mask & !root.presence_mask,
            0,
            "Root: absence_mask has bits set where presence_mask is not set"
        );
        
        // Iterate over all present middle blocks
        let mut middle_iter = root.presence_mask;

        while middle_iter != 0 {
            let ri = middle_iter.trailing_zeros();
            let middle = unsafe { root.data[ri as usize].assume_init_ref() };
            
            // Invariant 2: middle absence_mask must be subset of presence_mask
            assert_eq!(
                middle.absence_mask & !middle.presence_mask,
                0,
                "Middle[{}]: absence_mask has bits set where presence_mask is not set",
                ri
            );
            
            // Invariant 3: If middle is full, root's absence_mask should reflect this
            let middle_is_full = middle.absence_mask == u128::MAX;
            let root_thinks_full = (root.absence_mask >> ri) & 1 == 1;

            assert_eq!(
                middle_is_full,
                root_thinks_full,
                "Middle[{}]: fullness mismatch (middle_full={}, root_thinks_full={})",
                ri,
                middle_is_full,
                root_thinks_full
            );
            
            // Iterate over all present inner blocks
            let mut inner_iter = middle.presence_mask;

            while inner_iter != 0 {
                let mi = inner_iter.trailing_zeros();
                let inner = unsafe { middle.data[mi as usize].assume_init_ref() };
                
                // Invariant 4: inner absence_mask must be subset of presence_mask
                assert_eq!(
                    inner.absence_mask & !inner.presence_mask,
                    0,
                    "Inner[{}, {}]: absence_mask has bits set where presence_mask is not set",
                    ri,
                    mi
                );
                
                // Invariant 5: If inner is full, middle's absence_mask should reflect this
                let inner_is_full = inner.absence_mask == u128::MAX;
                let middle_thinks_full = (middle.absence_mask >> mi) & 1 == 1;
                assert_eq!(
                    inner_is_full,
                    middle_thinks_full,
                    "Inner[{}, {}]: fullness mismatch (inner_full={}, middle_thinks_full={})",
                    ri,
                    mi,
                    inner_is_full,
                    middle_thinks_full
                );
                
                inner_iter &= !(1 << mi);
            }
            
            middle_iter &= !(1 << ri);
        }
    }

    #[test]
    fn test_create() {
        let mut storage = Storage::<Entity>::new();

        // Create 128 items (fill one inner block)
        for i in 0..128 {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
            assert_eq!(e.generation(), 1);
        }

        {
            // Verify root masks
            let root = &storage.root;
            // First bit of presence should be 1 (first middle block exists)
            assert_eq!(root.presence_mask, 1);
            // Absence mask should be 0 (first middle block is not full yet)
            assert_eq!(root.absence_mask, 0);

            // Verify middle block
            let middle = unsafe { root.data[0].assume_init_ref() };
            // First bit of presence should be 1 (first inner block exists)
            assert_eq!(middle.presence_mask, 1);
            // First bit of absence should be 1 (first inner block is full)
            assert_eq!(middle.absence_mask, 1);

            // Verify inner block
            let inner = unsafe { middle.data[0].assume_init_ref() };
            assert_eq!(inner.presence_mask, u128::MAX);
            assert_eq!(inner.absence_mask, u128::MAX);
        }
        
        verify_tree_invariants(&storage);

        // Fill the rest of the first middle block (128 * 128 = 16384 items total)
        // We already inserted 128 items (0..128).
        // We need to insert 127 more inner blocks.
        for i in 128..16384 {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
            assert_eq!(e.generation(), 1);
        }

        {
            let root = &storage.root;
            // Verify root masks again
            // First bit of presence should be 1
            assert_eq!(root.presence_mask, 1);
            // First bit of absence should be 1 (first middle block is now full)
            assert_eq!(root.absence_mask, 1);

            // Verify middle block
            let middle = unsafe { root.data[0].assume_init_ref() };
            assert_eq!(middle.presence_mask, u128::MAX); // All inner blocks present
            assert_eq!(middle.absence_mask, u128::MAX); // All inner blocks full
        }
        
        verify_tree_invariants(&storage);
    }

    #[test]
    fn test_len() {
        let mut storage = Storage::<Entity>::new();

        assert_eq!(storage.len(), 0);

        for _ in 0..10 {
            storage.spawn();
        }
        assert_eq!(storage.len(), 10);

        // Fill one inner block (128 items)
        for _ in 10..128 {
            storage.spawn();
        }
        assert_eq!(storage.len(), 128);

        // Add one more to start next inner block
        storage.spawn();
        assert_eq!(storage.len(), 129);

        // Fill a whole middle block (128 * 128 = 16384 items)
        // We already have 129 items.
        for _ in 129..16384 {
            storage.spawn();
        }
        assert_eq!(storage.len(), 16384);

        // Add one more to start next middle block
        storage.spawn();
        assert_eq!(storage.len(), 16385);
        
        verify_tree_invariants(&storage);
    }

    #[test]
    fn test_entity_generation() {
        let mut storage = Storage::<Entity>::new();

        // Create first entity
        let e1 = *storage.spawn();
        assert_eq!(e1.index(), 0);
        assert_eq!(e1.generation(), 1); // Initialized to 0, incremented to 1

        // Create second entity
        let e2 = *storage.spawn();
        assert_eq!(e2.index(), 1);
        assert_eq!(e2.generation(), 1); // Initialized to 0, incremented to 1

        // "Delete" first entity by clearing absence mask bit
        // We need to manually access the inner block to do this for testing
        {
            let root = &mut storage.root;
            let middle = unsafe { root.data[0].assume_init_mut() };
            let inner = unsafe { middle.data[0].assume_init_mut() };
            inner.absence_mask &= !1; // Clear bit 0
        }

        // Create again, should reuse slot 0 and increment generation
        let e3 = *storage.spawn();
        assert_eq!(e3.index(), 0);
        assert_eq!(e3.generation(), 2); // 1 -> 2
        
        verify_tree_invariants(&storage);
    }

    // Helper function to delete an entity using the Storage::remove method
    fn delete_entity(storage: &mut Storage<Entity>, index: u32) {
        storage.remove(index);
    }

    #[test]
    fn test_spawn_delete_cycle() {
        let mut storage = Storage::<Entity>::new();
        
        // Spawn and delete the same slot multiple times
        for cycle in 1..=10 {
            let e = *storage.spawn();
            assert_eq!(e.index(), 0);
            assert_eq!(e.generation(), cycle);
            
            verify_tree_invariants(&storage);
            assert_eq!(storage.len(), 1);
            
            delete_entity(&mut storage, 0);
            
            verify_tree_invariants(&storage);
            assert_eq!(storage.len(), 0);
        }
    }

    #[test]
    fn test_batch_delete_and_respawn() {
        let mut storage = Storage::<Entity>::new();
        
        // Spawn 256 entities (2 full inner blocks)
        for i in 0..256 {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
            assert_eq!(e.generation(), 1);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 256);
        
        // Delete first 128 entities (first inner block)
        for i in 0..128 {
            delete_entity(&mut storage, i);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 128);
        
        // Spawn again, should reuse first 128 slots with incremented generation
        for i in 0..128 {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
            assert_eq!(e.generation(), 2); // Generation incremented
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 256);
    }

    #[test]
    fn test_delete_every_other() {
        let mut storage = Storage::<Entity>::new();
        
        // Spawn 100 entities
        for i in 0..100 {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 100);
        
        // Delete every other entity (even indices)
        for i in (0..100).step_by(2) {
            delete_entity(&mut storage, i);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 50);
        
        // Spawn 50 more, should fill the even slots
        for i in (0..100).step_by(2) {
            let e = storage.spawn();
            assert_eq!(e.index(), i); // Should reuse even slots
            assert_eq!(e.generation(), 2);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 100);
    }

    #[test]
    fn test_delete_across_blocks() {
        let mut storage = Storage::<Entity>::new();
        
        // Fill first inner block completely (128 entities)
        for i in 0..128 {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
            assert_eq!(e.generation(), 1);
        }
        
        verify_tree_invariants(&storage);
        
        // Spawn 64 more in second inner block
        for i in 128..192 {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
            assert_eq!(e.generation(), 1);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 192);
        
        // Delete from both blocks: last 64 from first block, first 32 from second
        for i in 64..128 {
            delete_entity(&mut storage, i);
        }
        for i in 128..160 {
            delete_entity(&mut storage, i);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 96);
        
        // Spawn again - should fill deleted slots in order
        // First fill 64..128, then 128..160
        for expected_idx in (64..128).chain(128..160) {
            let e = storage.spawn();
            assert_eq!(e.index(), expected_idx);
            assert_eq!(e.generation(), 2);
        }
        
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 192);
    }

    #[test]
    fn test_delete_loop_pattern() {
        let mut storage = Storage::<Entity>::new();
        
        // Pattern: spawn 10, delete 5, spawn 10, delete 5, etc.
        let mut expected_indices = Vec::new();
        
        // First batch: spawn 10 (indices 0..10)
        for i in 0..10 {
            storage.spawn();
            expected_indices.push(i);
        }
        verify_tree_invariants(&storage);
        
        // Delete last 5 (indices 5..10)
        for i in 5..10 {
            delete_entity(&mut storage, i);
            expected_indices.retain(|&x| x != i);
        }
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 5);
        
        // Second batch: spawn 10
        // Should reuse 5..10, then allocate 10..15
        for i in (5..10).chain(10..15) {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
            expected_indices.push(i);
        }
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 15);
        
        // Delete 10..15
        for i in 10..15 {
            delete_entity(&mut storage, i);
            expected_indices.retain(|&x| x != i);
        }
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 10);
        
        // Third batch: spawn 10
        // Should reuse 10..15, then allocate 15..20
        for i in (10..15).chain(15..20) {
            let e = storage.spawn();
            assert_eq!(e.index(), i);
        }
        verify_tree_invariants(&storage);
        assert_eq!(storage.len(), 20);
    }

    #[test]
    fn test_changed_mask_propagation() {
        use crate::component::Destroyed;
        
        let mut storage = Storage::<Destroyed>::new();

        // Initially, all changed_masks should be 0
        assert_eq!(storage.root.changed_mask, 0);

        // Set a value at index 0 (ri=0, mi=0, ii=0)
        storage.set(0, &Destroyed{});

        // Verify changed_mask is set at all levels
        let root = &storage.root;
        assert_eq!(root.changed_mask & 1, 1, "Root changed_mask bit 0 should be set");

        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.changed_mask & 1, 1, "Middle changed_mask bit 0 should be set");

        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.changed_mask & 1, 1, "Inner changed_mask bit 0 should be set");

        // Set another value in the same inner block (ri=0, mi=0, ii=5)
        storage.set(5, &Destroyed{});

        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };

        assert_eq!(inner.changed_mask & (1 << 5), 1 << 5, "Inner changed_mask bit 5 should be set");
        assert_eq!(inner.changed_mask & 1, 1, "Inner changed_mask bit 0 should still be set");

        // Set a value in a different middle block (ri=0, mi=1, ii=0) -> index 128
        storage.set(128, &Destroyed{});

        let root = &storage.root;
        assert_eq!(root.changed_mask & 1, 1, "Root changed_mask bit 0 should still be set");

        let middle1 = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle1.changed_mask & (1 << 1), 1 << 1, "Middle changed_mask bit 1 should be set");

        let inner1 = unsafe { middle1.data[1].assume_init_ref() };
        assert_eq!(inner1.changed_mask & 1, 1, "Second inner changed_mask bit 0 should be set");

        // Set a value in a different root block (ri=1, mi=0, ii=0) -> index 16384
        storage.set(16384, &Destroyed{});

        let root = &storage.root;
        assert_eq!(root.changed_mask & (1 << 1), 1 << 1, "Root changed_mask bit 1 should be set");
        assert_eq!(root.changed_mask & 1, 1, "Root changed_mask bit 0 should still be set");
    }

    #[test]
    fn test_get() {
        let mut storage = Storage::<u32>::new();
        
        // Set some values
        storage.set(0, &100);
        storage.set(5, &105);
        storage.set(128, &228);
        
        // Verify get returns correct values
        assert_eq!(storage.get(0), Some(&100));
        assert_eq!(storage.get(5), Some(&105));
        assert_eq!(storage.get(128), Some(&228));
        
        // Verify get returns None for missing values
        assert_eq!(storage.get(1), None);
        assert_eq!(storage.get(129), None);
        assert_eq!(storage.get(1000), None);
        
        // Remove a value and verify get returns None
        storage.remove(5);
        assert_eq!(storage.get(5), None);
        
        // Set it again and verify get returns Some
        storage.set(5, &999);
        assert_eq!(storage.get(5), Some(&999));
    }
}
