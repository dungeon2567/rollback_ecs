use crate::component::{Component, Resource};
use crate::entity::Entity;
use crate::safety::verify_storage_invariants;
use crate::storage::Storage;
use crate::tick::Tick;

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

    verify_storage_invariants(&storage).unwrap();

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

    verify_storage_invariants(&storage).unwrap();
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

    verify_storage_invariants(&storage).unwrap();
}

#[test]
fn test_entity_generation() {
    let mut storage = Storage::<Entity>::new();

    // Create first entity
    let e1 = storage.spawn();
    assert_eq!(e1.index(), 0);
    assert_eq!(e1.generation(), 1); // Initialized to 0, incremented to 1

    // Create second entity
    let e2 = storage.spawn();
    assert_eq!(e2.index(), 1);
    assert_eq!(e2.generation(), 1); // Initialized to 0, incremented to 1

    // "Delete" first entity by clearing absence mask bit
    // We need to manually access the inner block to do this for testing
    {
        let root = &mut storage.root;
        let middle = unsafe { root.data[0].assume_init_mut() };
        let inner = unsafe { middle.data[0].assume_init_mut() };
        inner.absence_mask &= !1; // Clear bit 0
        inner.presence_mask &= !1; // Clear presence bit too
    }

    // Create again, should reuse slot 0 and reset generation (presence mask cleared)
    let e3 = storage.spawn();
    assert_eq!(e3.index(), 0);
    assert_eq!(e3.generation(), 1); // 1 -> 1 (reset)

    verify_storage_invariants(&storage).unwrap();
}

// Helper function to delete an entity using the Storage::remove method
fn delete_entity(storage: &mut Storage<Entity>, index: u32) {
    storage.remove(index);
}

#[test]
fn test_spawn_delete_cycle() {
    let mut storage = Storage::<Entity>::new();

    // Spawn and delete the same slot multiple times
    for _cycle in 1..=10 {
        let e = storage.spawn();
        assert_eq!(e.index(), 0);
        assert_eq!(e.generation(), 1); // Always 1 because removal clears presence

        verify_storage_invariants(&storage).unwrap();
        assert_eq!(storage.len(), 1);

        delete_entity(&mut storage, 0);

        verify_storage_invariants(&storage).unwrap();
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

    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 256);

    // Delete first 128 entities (first inner block)
    for i in 0..128 {
        delete_entity(&mut storage, i);
    }

    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 128);

    // Spawn again, should reuse first 128 slots with reset generation
    for i in 0..128 {
        let e = storage.spawn();
        assert_eq!(e.index(), i);
        assert_eq!(e.generation(), 1); // Generation reset
    }

    verify_storage_invariants(&storage).unwrap();
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

    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 100);

    // Delete every other entity (even indices)
    for i in (0..100).step_by(2) {
        delete_entity(&mut storage, i);
    }

    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 50);

    // Spawn 50 more, should fill the even slots
    for i in (0..100).step_by(2) {
        let e = storage.spawn();
        assert_eq!(e.index(), i); // Should reuse even slots
        assert_eq!(e.generation(), 1); // Reset
    }

    verify_storage_invariants(&storage).unwrap();
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

    verify_storage_invariants(&storage).unwrap();

    // Spawn 64 more in second inner block
    for i in 128..192 {
        let e = storage.spawn();
        assert_eq!(e.index(), i);
        assert_eq!(e.generation(), 1);
    }

    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 192);

    // Delete from both blocks: last 64 from first block, first 32 from second
    for i in 64..128 {
        delete_entity(&mut storage, i);
    }
    for i in 128..160 {
        delete_entity(&mut storage, i);
    }

    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 96);

    // Spawn again - should fill deleted slots in order
    // First fill 64..128, then 128..160
    for expected_idx in (64..128).chain(128..160) {
        let e = storage.spawn();
        assert_eq!(e.index(), expected_idx);
        assert_eq!(e.generation(), 1); // Reset
    }

    verify_storage_invariants(&storage).unwrap();
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
    verify_storage_invariants(&storage).unwrap();

    // Delete last 5 (indices 5..10)
    for i in 5..10 {
        delete_entity(&mut storage, i);
        expected_indices.retain(|&x| x != i);
    }
    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 5);

    // Second batch: spawn 10
    // Should reuse 5..10, then allocate 10..15
    for i in (5..10).chain(10..15) {
        let e = storage.spawn();
        assert_eq!(e.index(), i);
        expected_indices.push(i);
    }
    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 15);

    // Delete 10..15
    for i in 10..15 {
        delete_entity(&mut storage, i);
        expected_indices.retain(|&x| x != i);
    }
    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 10);

    // Third batch: spawn 10
    // Should reuse 10..15, then allocate 15..20
    for i in (10..15).chain(15..20) {
        let e = storage.spawn();
        assert_eq!(e.index(), i);
    }
    verify_storage_invariants(&storage).unwrap();
    assert_eq!(storage.len(), 20);
}

#[test]
fn test_changed_mask_propagation() {
    use crate::component::Destroyed;

    let mut storage = Storage::<Destroyed>::new();

    // Initially, all changed_masks should be 0
    assert_eq!(storage.root.changed_mask, 0);

    // Set a value at index 0 (ri=0, mi=0, ii=0)
    storage.set(0, &Destroyed {});

    // Verify changed_mask is set at all levels
    let root = &storage.root;
    assert_eq!(
        root.changed_mask & 1,
        1,
        "Root changed_mask bit 0 should be set"
    );

    let middle = unsafe { root.data[0].assume_init_ref() };
    assert_eq!(
        middle.changed_mask & 1,
        1,
        "Middle changed_mask bit 0 should be set"
    );

    let inner = unsafe { middle.data[0].assume_init_ref() };
    assert_eq!(
        inner.changed_mask & 1,
        1,
        "Inner changed_mask bit 0 should be set"
    );

    // Set another value in the same inner block (ri=0, mi=0, ii=5)
    storage.set(5, &Destroyed {});

    let root = &storage.root;
    let middle = unsafe { root.data[0].assume_init_ref() };
    let inner = unsafe { middle.data[0].assume_init_ref() };

    assert_eq!(
        inner.changed_mask & (1 << 5),
        1 << 5,
        "Inner changed_mask bit 5 should be set"
    );
    assert_eq!(
        inner.changed_mask & 1,
        1,
        "Inner changed_mask bit 0 should still be set"
    );

    // Set a value in a different middle block (ri=0, mi=1, ii=0) -> index 128
    storage.set(128, &Destroyed {});

    let root = &storage.root;
    assert_eq!(
        root.changed_mask & 1,
        1,
        "Root changed_mask bit 0 should still be set"
    );

    let middle1 = unsafe { root.data[0].assume_init_ref() };
    assert_eq!(
        middle1.changed_mask & (1 << 1),
        1 << 1,
        "Middle changed_mask bit 1 should be set"
    );

    let inner1 = unsafe { middle1.data[1].assume_init_ref() };
    assert_eq!(
        inner1.changed_mask & 1,
        1,
        "Second inner changed_mask bit 0 should be set"
    );

    // Set a value in a different root block (ri=1, mi=0, ii=0) -> index 16384
    storage.set(16384, &Destroyed {});

    let root = &storage.root;
    assert_eq!(
        root.changed_mask & (1 << 1),
        1 << 1,
        "Root changed_mask bit 1 should be set"
    );
    assert_eq!(
        root.changed_mask & 1,
        1,
        "Root changed_mask bit 0 should still be set"
    );
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

#[test]
fn test_rollback() {
    let mut storage = Storage::<Entity>::new();

    assert_eq!(storage.len(), 0);

    storage.set(48, &Entity::new(10u32, 10u32));

    assert_eq!(storage.len(), 1);
    assert_eq!(storage.get(48), Some(&Entity::new(10u32, 10u32)));

    storage.rollback(Tick::new(0));

    assert_eq!(storage.len(), 0);
}

impl Resource for u32 {
    fn type_index() -> usize {
        0
    }
}
impl Component for u32 {}

#[test]
fn test_get_mut_sets_changed_mask() {
    let mut storage = Storage::<u32>::new();

    // Set a value at index 0
    storage.set(0, &100);

    // Clear all changed_mask bits (simulating after cleanup)
    {
        let root = &mut storage.root;
        root.changed_mask = 0;
        let middle = unsafe { root.data[0].assume_init_mut() };
        middle.changed_mask = 0;
        let inner = unsafe { middle.data[0].assume_init_mut() };
        inner.changed_mask = 0;
    }

    // Verify all changed_mask bits are cleared
    assert_eq!(storage.root.changed_mask, 0);
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.changed_mask, 0);
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.changed_mask, 0);
    }

    // Call get_mut - this should set changed_mask
    let value = storage.get_mut(0);
    *value = 200;

    // Verify changed_mask is set at all levels
    let root = &storage.root;
    assert_eq!(
        root.changed_mask & 1,
        1,
        "Root changed_mask bit 0 should be set"
    );

    let middle = unsafe { root.data[0].assume_init_ref() };
    assert_eq!(
        middle.changed_mask & 1,
        1,
        "Middle changed_mask bit 0 should be set"
    );

    let inner = unsafe { middle.data[0].assume_init_ref() };
    assert_eq!(
        inner.changed_mask & 1,
        1,
        "Inner changed_mask bit 0 should be set"
    );

    // Verify the value was actually modified
    assert_eq!(storage.get(0), Some(&200));

    // Call get_mut again - changed_mask should NOT be modified again
    // Set a different bit at middle level to detect if it gets overwritten
    {
        let root = &mut storage.root;
        let middle = unsafe { root.data[0].assume_init_mut() };
        middle.changed_mask |= 1 << 5; // Set bit 5
    }

    let value2 = storage.get_mut(0);
    *value2 = 300;

    // Verify that bit 5 at middle level is still set (wasn't cleared by get_mut)
    let root = &storage.root;
    let middle = unsafe { root.data[0].assume_init_ref() };
    assert_eq!(
        middle.changed_mask & (1 << 5),
        1 << 5,
        "Middle changed_mask bit 5 should still be set"
    );
    assert_eq!(
        middle.changed_mask & 1,
        1,
        "Middle changed_mask bit 0 should still be set"
    );
}

#[test]
fn test_rollback_insert() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);

    assert_eq!(*storage.get(0).unwrap(), 100);

    storage.clear_changes();

    // Rollback to Tick 0 (before insert)
    storage.rollback(Tick::new(0));

    assert!(storage.get(0).is_none());
}

#[test]
fn test_rollback_update() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);

    storage.clear_changes();

    // Tick 2: Update
    storage.set_tick(Tick::new(2));
    storage.set(0, &200);

    assert_eq!(*storage.get(0).unwrap(), 200);

    storage.clear_changes();

    // Rollback to Tick 1
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
}

#[test]
fn test_rollback_remove() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);

    storage.clear_changes();

    // Tick 2: Remove
    storage.set_tick(Tick::new(2));
    storage.remove(0);

    assert!(storage.get(0).is_none());

    storage.clear_changes();

    // Rollback to Tick 1
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
}

#[test]
fn test_rollback_two_ticks() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);
    storage.set(1, &200);

    storage.clear_changes();

    // Tick 2: Update both
    storage.set_tick(Tick::new(2));
    storage.set(0, &150);
    storage.set(1, &250);

    assert_eq!(*storage.get(0).unwrap(), 150);
    assert_eq!(*storage.get(1).unwrap(), 250);

    storage.clear_changes();

    // Tick 3: Update again
    storage.set_tick(Tick::new(3));
    storage.set(0, &300);
    storage.set(1, &350);

    assert_eq!(*storage.get(0).unwrap(), 300);
    assert_eq!(*storage.get(1).unwrap(), 350);

    storage.clear_changes();

    // Rollback to Tick 1 (should undo both tick 2 and tick 3)
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
    assert_eq!(*storage.get(1).unwrap(), 200);
}

#[test]
fn test_rollback_multiple_ticks_mixed_operations() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);
    storage.set(1, &200);

    storage.clear_changes();

    // Tick 2: Update one, insert another
    storage.set_tick(Tick::new(2));
    storage.set(0, &150);
    storage.set(2, &300);

    assert_eq!(*storage.get(0).unwrap(), 150);
    assert_eq!(*storage.get(1).unwrap(), 200);
    assert_eq!(*storage.get(2).unwrap(), 300);

    storage.clear_changes();

    // Tick 3: Update all, remove one
    storage.set_tick(Tick::new(3));
    storage.set(0, &400);
    storage.set(1, &500);
    storage.set(2, &600);

    assert_eq!(*storage.get(0).unwrap(), 400);
    assert_eq!(*storage.get(1).unwrap(), 500);
    assert_eq!(*storage.get(2).unwrap(), 600);

    storage.clear_changes();

    // Tick 4: More updates
    storage.set_tick(Tick::new(4));
    storage.set(0, &700);
    storage.remove(1);

    assert_eq!(*storage.get(0).unwrap(), 700);
    assert!(storage.get(1).is_none());
    assert_eq!(*storage.get(2).unwrap(), 600);

    storage.clear_changes();

    // Rollback to Tick 1 (should undo ticks 2, 3, and 4)
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
    assert_eq!(*storage.get(1).unwrap(), 200);
    assert!(storage.get(2).is_none()); // Was inserted in tick 2, so should be gone
}

#[test]
fn test_rollback_same_slot_multiple_ticks() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);

    storage.clear_changes();

    // Tick 2: Update
    storage.set_tick(Tick::new(2));
    storage.set(0, &200);

    storage.clear_changes();

    // Tick 3: Update again
    storage.set_tick(Tick::new(3));
    storage.set(0, &300);

    storage.clear_changes();

    // Tick 4: Update again
    storage.set_tick(Tick::new(4));
    storage.set(0, &400);

    assert_eq!(*storage.get(0).unwrap(), 400);

    storage.clear_changes();

    // Rollback to Tick 1 (should restore to 100, skipping all intermediate values)
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
}

#[test]
fn test_rollback_partial_ticks() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);
    storage.set(1, &200);

    storage.clear_changes();

    // Tick 2: Update both
    storage.set_tick(Tick::new(2));
    storage.set(0, &150);
    storage.set(1, &250);

    storage.clear_changes();

    // Tick 3: Update both again
    storage.set_tick(Tick::new(3));
    storage.set(0, &300);
    storage.set(1, &350);

    assert_eq!(*storage.get(0).unwrap(), 300);
    assert_eq!(*storage.get(1).unwrap(), 350);

    storage.clear_changes();

    // Rollback to Tick 2 (should only undo tick 3)
    storage.rollback(Tick::new(2));

    assert_eq!(*storage.get(0).unwrap(), 150);
    assert_eq!(*storage.get(1).unwrap(), 250);

    // Rollback to Tick 1 (should undo tick 2 as well)
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
    assert_eq!(*storage.get(1).unwrap(), 200);
}

#[test]
fn test_rollback_multiple_slots_across_blocks() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert values across different blocks
    storage.set_tick(Tick::new(1));
    storage.set(0, &100); // First inner block
    storage.set(128, &200); // Second inner block
    storage.set(256, &300); // Third inner block

    storage.clear_changes();

    // Tick 2: Update all
    storage.set_tick(Tick::new(2));
    storage.set(0, &150);
    storage.set(128, &250);
    storage.set(256, &350);

    storage.clear_changes();

    // Tick 3: Update all again
    storage.set_tick(Tick::new(3));
    storage.set(0, &400);
    storage.set(128, &500);
    storage.set(256, &600);

    assert_eq!(*storage.get(0).unwrap(), 400);
    assert_eq!(*storage.get(128).unwrap(), 500);
    assert_eq!(*storage.get(256).unwrap(), 600);

    storage.clear_changes();

    // Rollback to Tick 1 (should restore all to original values)
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
    assert_eq!(*storage.get(128).unwrap(), 200);
    assert_eq!(*storage.get(256).unwrap(), 300);
}

#[test]
fn test_rollback_insert_and_remove_cycle() {
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert
    storage.set_tick(Tick::new(1));
    storage.set(0, &100);

    storage.clear_changes();

    // Tick 2: Remove
    storage.set_tick(Tick::new(2));
    storage.remove(0);

    assert!(storage.get(0).is_none());

    storage.clear_changes();

    // Tick 3: Insert again
    storage.set_tick(Tick::new(3));
    storage.set(0, &200);

    assert_eq!(*storage.get(0).unwrap(), 200);

    storage.clear_changes();

    // Tick 4: Update
    storage.set_tick(Tick::new(4));
    storage.set(0, &300);

    assert_eq!(*storage.get(0).unwrap(), 300);

    storage.clear_changes();

    // Rollback to Tick 1 (should restore to original insert)
    storage.rollback(Tick::new(1));

    assert_eq!(*storage.get(0).unwrap(), 100);
}

#[test]
fn test_rollback_inner_block_becomes_full() {
    use crate::safety::verify_storage_invariants;

    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert 127 values (one less than a full inner block of 128)
    // This will fill slots 0..127 in the first inner block
    storage.set_tick(Tick::new(1));
    for i in 0..127 {
        storage.set(i, &(i as u32));
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify the inner block is not full yet
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask.count_ones(), 127, "Inner block should have 127 slots filled");
        assert_ne!(inner.absence_mask, u128::MAX, "Inner block should not be full");
        assert_eq!(middle.absence_mask & 1, 0, "Middle should not mark inner block as full");
    }

    // Tick 2: Remove some values to create gaps
    storage.set_tick(Tick::new(2));
    for i in (0..127).step_by(2) {
        storage.remove(i); // Remove even indices
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify the inner block is no longer full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert!(inner.absence_mask.count_ones() < 127, "Inner block should have fewer slots after removal");
    }

    // Rollback to Tick 1: This should restore all 127 values, making the inner block almost full
    // But wait - we need to test the case where it becomes FULL, so let's add one more in tick 1
    // Actually, let's test a different scenario - fill it completely in tick 1
    
    // Reset and do a better test
    drop(storage);
    let mut storage = Storage::<u32>::new();

    // Tick 1: Insert 127 values (almost full inner block)
    storage.set_tick(Tick::new(1));
    for i in 0..127 {
        storage.set(i, &(i as u32));
    }
    storage.clear_changes();

    // Tick 2: Add the 128th value to make inner block full
    storage.set_tick(Tick::new(2));
    storage.set(127, &127);
    storage.clear_changes();

    // Verify inner block is now full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask, u128::MAX, "Inner block should be full");
        assert_eq!(middle.absence_mask & 1, 1, "Middle should mark inner block as full");
    }

    // Tick 3: Remove the 128th value
    storage.set_tick(Tick::new(3));
    storage.remove(127);
    storage.clear_changes();

    // Verify inner block is no longer full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_ne!(inner.absence_mask, u128::MAX, "Inner block should not be full");
        assert_eq!(middle.absence_mask & 1, 0, "Middle should not mark inner block as full");
    }

    // Rollback to Tick 2: Should restore the 128th value, making inner block full again
    storage.rollback(Tick::new(2));
    
    // Verify invariants are maintained after rollback
    verify_storage_invariants(&storage).unwrap();

    // Verify inner block is full again after rollback
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask, u128::MAX, "Inner block should be full after rollback");
        assert_eq!(middle.absence_mask & 1, 1, "Middle should mark inner block as full after rollback");
    }

    // Rollback to Tick 1: Should remove the 128th value again
    storage.rollback(Tick::new(1));
    
    // Verify invariants are maintained
    verify_storage_invariants(&storage).unwrap();

    // Verify inner block is not full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_ne!(inner.absence_mask, u128::MAX, "Inner block should not be full after rollback to tick 1");
        assert_eq!(middle.absence_mask & 1, 0, "Middle should not mark inner block as full after rollback to tick 1");
    }
}

#[test]
fn test_rollback_middle_block_becomes_full() {
    use crate::safety::verify_storage_invariants;

    let mut storage = Storage::<u32>::new();

    // Tick 1: Fill 127 inner blocks (almost fill a middle block)
    // Each inner block has 128 slots, so we need 127 * 128 = 16256 slots
    storage.set_tick(Tick::new(1));
    for inner_idx in 0..127 {
        for slot_idx in 0..128 {
            let global_idx = inner_idx * 128 + slot_idx;
            storage.set(global_idx, &(global_idx as u32));
        }
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify middle block is almost full (127 inner blocks are full)
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.absence_mask.count_ones(), 127, "Middle block should have 127 inner blocks marked as full");
        assert_ne!(middle.absence_mask, u128::MAX, "Middle block should not be full yet");
        assert_eq!(root.absence_mask & 1, 0, "Root should not mark middle block as full");
    }

    // Tick 2: Fill the 128th inner block to make middle block full
    storage.set_tick(Tick::new(2));
    for slot_idx in 0..128 {
        let global_idx = 127 * 128 + slot_idx; // 127th inner block, slot_idx
        storage.set(global_idx, &(global_idx as u32));
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify middle block is now full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.absence_mask, u128::MAX, "Middle block should be full");
        assert_eq!(root.absence_mask & 1, 1, "Root should mark middle block as full");
    }

    // Tick 3: Remove some values from the 128th inner block
    storage.set_tick(Tick::new(3));
    for slot_idx in 0..64 {
        let global_idx = 127 * 128 + slot_idx;
        storage.remove(global_idx);
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify middle block is no longer full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_ne!(middle.absence_mask, u128::MAX, "Middle block should not be full");
        assert_eq!(root.absence_mask & 1, 0, "Root should not mark middle block as full");
    }

    // Rollback to Tick 2: Should restore values, making middle block full again
    storage.rollback(Tick::new(2));
    
    // Verify invariants are maintained after rollback
    verify_storage_invariants(&storage).unwrap();

    // Verify middle block is full again after rollback
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.absence_mask, u128::MAX, "Middle block should be full after rollback");
        assert_eq!(root.absence_mask & 1, 1, "Root should mark middle block as full after rollback");
    }

    // Rollback to Tick 1: Should remove the 128th inner block values
    storage.rollback(Tick::new(1));
    
    // Verify invariants are maintained
    verify_storage_invariants(&storage).unwrap();

    // Verify middle block is not full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_ne!(middle.absence_mask, u128::MAX, "Middle block should not be full after rollback to tick 1");
        assert_eq!(root.absence_mask & 1, 0, "Root should not mark middle block as full after rollback to tick 1");
    }
}

#[test]
fn test_rollback_multiple_inner_blocks_become_full() {
    use crate::safety::verify_storage_invariants;

    let mut storage = Storage::<u32>::new();

    // Tick 1: Fill 5 inner blocks with 127 values each (almost full)
    storage.set_tick(Tick::new(1));
    for inner_idx in 0..5 {
        for slot_idx in 0..127 {
            let global_idx = inner_idx * 128 + slot_idx;
            storage.set(global_idx, &(global_idx as u32));
        }
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify all 5 inner blocks are not full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.absence_mask.count_ones(), 0, "No inner blocks should be marked as full");
    }

    // Tick 2: Fill the 128th slot in each of the 5 inner blocks, making them all full
    storage.set_tick(Tick::new(2));
    for inner_idx in 0..5 {
        let global_idx = inner_idx * 128 + 127;
        storage.set(global_idx, &(global_idx as u32));
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify all 5 inner blocks are now full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.absence_mask.count_ones(), 5, "All 5 inner blocks should be marked as full");
        for inner_idx in 0..5 {
            assert_eq!((middle.absence_mask >> inner_idx) & 1, 1, "Inner block {} should be marked as full", inner_idx);
        }
    }

    // Tick 3: Remove the 128th slot from each inner block
    storage.set_tick(Tick::new(3));
    for inner_idx in 0..5 {
        let global_idx = inner_idx * 128 + 127;
        storage.remove(global_idx);
    }

    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Verify all 5 inner blocks are no longer full
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.absence_mask.count_ones(), 0, "No inner blocks should be marked as full");
    }

    // Rollback to Tick 2: Should restore all 5 inner blocks to full
    storage.rollback(Tick::new(2));
    verify_storage_invariants(&storage).unwrap();

    // Verify all 5 inner blocks are full again
    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        assert_eq!(middle.absence_mask.count_ones(), 5, "All 5 inner blocks should be marked as full after rollback");
    }
}

#[test]
fn test_rollback_fullness_transitions_across_multiple_ticks() {
    use crate::safety::verify_storage_invariants;

    let mut storage = Storage::<u32>::new();

    // Tick 1: Fill inner block 0 with 126 values (not full)
    storage.set_tick(Tick::new(1));
    for i in 0..126 {
        storage.set(i, &(i as u32));
    }
    storage.clear_changes();

    // Tick 2: Add one more value to make it almost full (127 values)
    storage.set_tick(Tick::new(2));
    storage.set(126, &126);
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask.count_ones(), 127);
        assert_ne!(inner.absence_mask, u128::MAX);
    }

    // Tick 3: Add the 128th value to make it full
    storage.set_tick(Tick::new(3));
    storage.set(127, &127);
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 1);
    }

    // Tick 4: Remove the last value, making it non-full
    storage.set_tick(Tick::new(4));
    storage.remove(127);
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_ne!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 0);
    }

    // Rollback to Tick 3: Should make it full again
    storage.rollback(Tick::new(3));
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 1);
    }

    // Rollback to Tick 2: Should make it non-full (127 values)
    storage.rollback(Tick::new(2));
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask.count_ones(), 127);
        assert_ne!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 0);
    }

    // Rollback to Tick 1: Should have 126 values
    storage.rollback(Tick::new(1));
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask.count_ones(), 126);
    }
}

#[test]
fn test_rollback_add_and_remove_causing_fullness_changes() {
    use crate::safety::verify_storage_invariants;

    let mut storage = Storage::<u32>::new();

    // Tick 1: Fill inner block 0 completely (128 values)
    storage.set_tick(Tick::new(1));
    for i in 0..128 {
        storage.set(i, &(i as u32));
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 1);
    }

    // Tick 2: Remove all even-indexed values (64 removals)
    storage.set_tick(Tick::new(2));
    for i in (0..128).step_by(2) {
        storage.remove(i);
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask.count_ones(), 64);
        assert_ne!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 0);
    }

    // Tick 3: Add new values to fill the gaps (making it full again)
    storage.set_tick(Tick::new(3));
    for i in (0..128).step_by(2) {
        storage.set(i, &(i as u32 + 1000));
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 1);
    }

    // Rollback to Tick 2: Should restore the state with 64 values
    storage.rollback(Tick::new(2));
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask.count_ones(), 64);
        assert_ne!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 0);
    }

    // Rollback to Tick 1: Should restore the full block
    storage.rollback(Tick::new(1));
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle = unsafe { root.data[0].assume_init_ref() };
        let inner = unsafe { middle.data[0].assume_init_ref() };
        assert_eq!(inner.absence_mask, u128::MAX);
        assert_eq!(middle.absence_mask & 1, 1);
    }
}

#[test]
fn test_rollback_partial_blocks_across_different_middle_blocks() {
    use crate::safety::verify_storage_invariants;

    let mut storage = Storage::<u32>::new();

    // Tick 1: Partially fill 3 inner blocks in different middle blocks
    // Middle block 0, inner block 0: 100 values
    // Middle block 0, inner block 1: 100 values  
    // Middle block 1, inner block 0: 100 values
    storage.set_tick(Tick::new(1));
    for i in 0..100 {
        storage.set(i, &i); // Middle 0, inner 0
        storage.set(128 + i, &(128 + i)); // Middle 0, inner 1
        storage.set(16384 + i, &(16384 + i)); // Middle 1, inner 0
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    // Tick 2: Fill remaining slots to make all 3 inner blocks full
    storage.set_tick(Tick::new(2));
    for i in 100..128 {
        storage.set(i, &i); // Complete middle 0, inner 0
        storage.set(128 + i, &(128 + i)); // Complete middle 0, inner 1
        storage.set(16384 + i, &(16384 + i)); // Complete middle 1, inner 0
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle0 = unsafe { root.data[0].assume_init_ref() };
        let middle1 = unsafe { root.data[1].assume_init_ref() };
        assert_eq!(middle0.absence_mask & 0b11, 0b11, "Both inner blocks in middle 0 should be full");
        assert_eq!(middle1.absence_mask & 1, 1, "Inner block 0 in middle 1 should be full");
        // Root's absence_mask only tracks middle blocks that are FULL (all 128 inner blocks are full)
        // Middle 0 has only 2 inner blocks full (out of potentially many), so it's not full yet
        // Middle 1 has only 1 inner block full, so it's not full yet
        // Therefore, root.absence_mask should not have bits set for these middle blocks
        assert_eq!(root.absence_mask & 0b11, 0, "Neither middle block should be marked as full at root level (they're not completely full)");
    }

    // Tick 3: Remove some values from each inner block
    storage.set_tick(Tick::new(3));
    for i in 100..128 {
        storage.remove(i);
        storage.remove(128 + i);
        storage.remove(16384 + i);
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle0 = unsafe { root.data[0].assume_init_ref() };
        let middle1 = unsafe { root.data[1].assume_init_ref() };
        assert_eq!(middle0.absence_mask & 0b11, 0, "Neither inner block in middle 0 should be full");
        assert_eq!(middle1.absence_mask & 1, 0, "Inner block 0 in middle 1 should not be full");
    }

    // Rollback to Tick 2: Should restore all 3 inner blocks to full
    storage.rollback(Tick::new(2));
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        let middle0 = unsafe { root.data[0].assume_init_ref() };
        let middle1 = unsafe { root.data[1].assume_init_ref() };
        assert_eq!(middle0.absence_mask & 0b11, 0b11, "Both inner blocks in middle 0 should be full after rollback");
        assert_eq!(middle1.absence_mask & 1, 1, "Inner block 0 in middle 1 should be full after rollback");
    }
}

#[test]
fn test_rollback_root_block_fullness_propagation() {
    use crate::safety::verify_storage_invariants;

    let mut storage = Storage::<u32>::new();

    // Tick 1: Fill 127 middle blocks completely (each with 128 full inner blocks)
    // This makes root block almost full (127 middle blocks are full)
    storage.set_tick(Tick::new(1));
    for middle_idx in 0..127 {
        for inner_idx in 0..128 {
            for slot_idx in 0..128 {
                let global_idx = middle_idx * 16384 + inner_idx * 128 + slot_idx;
                storage.set(global_idx, &(global_idx as u32));
            }
        }
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        assert_eq!(root.absence_mask.count_ones(), 127, "Root should have 127 middle blocks marked as full");
        assert_ne!(root.absence_mask, u128::MAX, "Root should not be full yet");
    }

    // Tick 2: Fill the 128th middle block completely
    storage.set_tick(Tick::new(2));
    for inner_idx in 0..128 {
        for slot_idx in 0..128 {
            let global_idx = 127 * 16384 + inner_idx * 128 + slot_idx;
            storage.set(global_idx, &(global_idx as u32));
        }
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        // u128 has 128 bits (0-127), so if all present middle blocks are full, 
        // the absence_mask should have all 128 bits set, which is u128::MAX
        assert_eq!(root.absence_mask, u128::MAX, "Root should have all 128 middle blocks marked as full");
        assert_eq!(root.absence_mask.count_ones(), 128, "Root should have all 128 middle blocks marked as full");
    }

    // Tick 3: Remove some inner blocks from the 128th middle block
    storage.set_tick(Tick::new(3));
    for inner_idx in 64..128 {
        for slot_idx in 0..128 {
            let global_idx = 127 * 16384 + inner_idx * 128 + slot_idx;
            storage.remove(global_idx);
        }
    }
    storage.clear_changes();
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        assert_eq!(root.absence_mask.count_ones(), 127, "Root should have 127 middle blocks marked as full");
        assert_eq!(root.absence_mask & (1 << 127), 0, "128th middle block should not be marked as full");
    }

    // Rollback to Tick 2: Should restore the 128th middle block to full
    storage.rollback(Tick::new(2));
    verify_storage_invariants(&storage).unwrap();

    {
        let root = &storage.root;
        assert_eq!(root.absence_mask.count_ones(), 128, "Root should have all 128 middle blocks marked as full after rollback");
    }
}
