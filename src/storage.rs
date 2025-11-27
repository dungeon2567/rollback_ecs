use std::mem::MaybeUninit;

use crate::block::Block;
use crate::block::RollbackBlock;
use crate::component::Component;
use crate::tick::Tick;
use crate::world::World;

pub struct Storage<T> {
    pub root: Block<Box<Block<Box<Block<T>>>>>,
    pub snapshot: Option<Box<RollbackStorage<T>>>,
    pub current_tick: Tick,
}

pub struct RollbackStorage<T> {
    pub root: RollbackBlock<Box<RollbackBlock<Box<RollbackBlock<T>>>>>,
    pub tick: Tick,
    pub prev: Option<Box<RollbackStorage<T>>>,
}

impl<T: Component> RollbackStorage<T> {
    fn new(prev: Option<Box<Self>>, tick: Tick) -> Box<Self> {
        Box::new(RollbackStorage {
            tick,
            root: RollbackBlock {
                updated_mask: 0,
                added_mask: 0,
                data: std::array::from_fn(|_| std::mem::MaybeUninit::uninit()),
            },
            prev,
        })
    }

    pub fn mark_updated(&mut self, ri: u32, mi: u32, ii: u32, prev_value: &T) {
        debug_assert!(ri < 128, "ri index out of bounds: {}", ri);
        debug_assert!(mi < 128, "mi index out of bounds: {}", mi);
        debug_assert!(ii < 128, "ii index out of bounds: {}", ii);

        let root = &mut self.root;

        // Ensure middle block exists
        if (root.updated_mask >> ri) & 1 == 0 {
            let new_middle = RollbackBlock {
                updated_mask: 0,
                added_mask: 0,
                data: std::array::from_fn(|_| std::mem::MaybeUninit::uninit()),
            };

            root.data[ri as usize].write(Box::new(new_middle));
            root.updated_mask |= 1 << ri;
        }

        debug_assert!((root.updated_mask >> ri) & 1 != 0, "Middle block should exist");
        let middle = unsafe { root.data[ri as usize].assume_init_mut() };

        // Ensure inner block exists
        if (middle.updated_mask >> mi) & 1 == 0 {
            let new_inner = RollbackBlock {
                updated_mask: 0,
                added_mask: 0,
                data: std::array::from_fn(|_| std::mem::MaybeUninit::uninit()),
            };

            middle.data[mi as usize].write(Box::new(new_inner));
            middle.updated_mask |= 1 << mi;
        }

        debug_assert!((middle.updated_mask >> mi) & 1 != 0, "Inner block should exist");
        let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

        // Insert value
        // Note: We overwrite if it exists, but for rollback log we usually only insert once per tick (checked by changed_mask)

        debug_assert!(ii < 128, "ii index out of bounds in inner block: {}", ii);
        inner.updated_mask |= 1 << ii;

        inner.data[ii as usize] = MaybeUninit::new(prev_value.clone());
    }

    pub fn mark_added(&mut self, ri: u32, mi: u32, ii: u32) {
        debug_assert!(ri < 128, "ri index out of bounds: {}", ri);
        debug_assert!(mi < 128, "mi index out of bounds: {}", mi);
        debug_assert!(ii < 128, "ii index out of bounds: {}", ii);

        let root = &mut self.root;

        // Ensure middle block exists
        // Note: For intermediate nodes, we always use updated_mask to track existence of child snapshots.
        // added_mask is only used at the leaf level to indicate a slot was empty.
        if (root.updated_mask >> ri) & 1 == 0 {
            let new_middle = RollbackBlock {
                updated_mask: 0,
                added_mask: 0,
                data: std::array::from_fn(|_| std::mem::MaybeUninit::uninit()),
            };

            root.data[ri as usize].write(Box::new(new_middle));
            root.updated_mask |= 1 << ri;
        }

        debug_assert!((root.updated_mask >> ri) & 1 != 0, "Middle block should exist");
        let middle = unsafe { root.data[ri as usize].assume_init_mut() };

        // Ensure inner block exists
        if (middle.updated_mask >> mi) & 1 == 0 {
            let new_inner = RollbackBlock {
                updated_mask: 0,
                added_mask: 0,
                data: std::array::from_fn(|_| std::mem::MaybeUninit::uninit()),
            };

            middle.data[mi as usize].write(Box::new(new_inner));
            middle.updated_mask |= 1 << mi;
        }

        debug_assert!((middle.updated_mask >> mi) & 1 != 0, "Inner block should exist");
        let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

        debug_assert!(ii < 128, "ii index out of bounds in inner block: {}", ii);
        inner.added_mask |= 1 << ii;
    }
}

impl<T: Component> Storage<T> {
    pub fn new() -> Self {
        Storage {
            root: Block::new(),
            snapshot: None,
            current_tick: Tick::new(1),
        }
    }

    pub fn set_tick(&mut self, tick: Tick) {
        self.current_tick = tick;
    }

    /// Returns the cleanup system for this component type as a boxed PipelineStage.
    /// The caller should add it to the world's pending systems using `world.add_system_instance()`.
    ///
    /// # Example
    /// ```ignore
    /// let cleanup_system = storage.cleanup_system(&mut world);
    /// world.add_system_instance(cleanup_system);
    /// world.build_scheduler();
    /// world.run();
    /// ```
    pub fn cleanup_system(&self, world: &mut World) -> Box<dyn crate::scheduler::PipelineStage> {
        T::cleanup_system(world)
    }

    pub fn clear_changes(&mut self) {
        let root = &mut self.root;

        // Iterate only over middle blocks that have changes
        let mut middle_iter = root.changed_mask & root.presence_mask;
        debug_assert_eq!(root.changed_mask & !root.presence_mask, 0, "changed_mask should be subset of presence_mask");

        while middle_iter != 0 {
            let ri = middle_iter.trailing_zeros();
            debug_assert!(ri < 128, "ri index out of bounds: {}", ri);
            debug_assert!((root.presence_mask >> ri) & 1 != 0, "Middle block should exist");
            let middle = unsafe { root.data[ri as usize].assume_init_mut() };

            // Iterate only over inner blocks that have changes
            let mut inner_iter = middle.changed_mask & middle.presence_mask;
            debug_assert_eq!(middle.changed_mask & !middle.presence_mask, 0, "changed_mask should be subset of presence_mask");
            while inner_iter != 0 {
                let mi = inner_iter.trailing_zeros();
                debug_assert!(mi < 128, "mi index out of bounds: {}", mi);
                debug_assert!((middle.presence_mask >> mi) & 1 != 0, "Inner block should exist");
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

    pub fn rollback(&mut self, target_tick: Tick)
    where
        T: Clone,
    {
        // Collect all snapshots that need to be rolled back (tick > target_tick)
        // Pre-allocate with estimated capacity to avoid repeated allocations
        // Most rollbacks are shallow (1-10 snapshots), but we allocate for worst case
        let mut snapshots_to_rollback = Vec::with_capacity(32);
        let mut current_snapshot = self.snapshot.take();

        while let Some(mut snapshot) = current_snapshot {
            if snapshot.tick <= target_tick {
                // This snapshot is at or before target, keep it and stop
                self.snapshot = Some(snapshot);
                break;
            }

            // This snapshot needs to be rolled back
            let prev = snapshot.prev.take();
            snapshots_to_rollback.push(snapshot);
            current_snapshot = prev;
        }

        // Validate that we're not rolling back to a future tick
        // Only check if we actually have snapshots to rollback (otherwise it's a no-op)
        // If there are no snapshots, we can rollback to any tick (it's just updating current_tick)
        if !snapshots_to_rollback.is_empty() && target_tick.is_after(self.current_tick) {
            // Restore snapshot chain before panicking
            while let Some(mut snapshot) = snapshots_to_rollback.pop() {
                if let Some(existing) = self.snapshot.take() {
                    snapshot.prev = Some(existing);
                }
                self.snapshot = Some(snapshot);
            }
            panic!(
                "Cannot rollback to future tick: current tick is {}, target tick is {}",
                self.current_tick.value(),
                target_tick.value()
            );
        }

        // If we have snapshots to rollback, apply minimal changes using bitmasks
        if !snapshots_to_rollback.is_empty() {
            // Reverse so oldest (closest to target_tick) is first
            snapshots_to_rollback.reverse();
            Self::rollback_with_bitmasks(&snapshots_to_rollback, &mut self.root);
        }

        self.current_tick = target_tick;
    }

    fn rollback_with_bitmasks(
        snapshots: &[Box<RollbackStorage<T>>],
        block: &mut Block<Box<Block<Box<Block<T>>>>>,
    ) where
        T: Clone,
    {
        // Compute union of all masks to know which slots need to be restored
        let mut all_updated_mask = 0u128;
        let mut all_added_mask = 0u128;

        for snapshot in snapshots.iter() {
            all_updated_mask |= snapshot.root.updated_mask;
            all_added_mask |= snapshot.root.added_mask;
        }

        // Handle additions: if a slot was added in any snapshot, remove it
        // BUT: if a slot is both "added" and "updated", it means it existed before
        // (was updated), so we should restore from "updated" instead of removing
        let mut added = all_added_mask & !all_updated_mask;
        while added != 0 {
            let i = added.trailing_zeros();
            debug_assert!(i < 128, "Index out of bounds in rollback: {}", i);
            added &= !(1 << i);

            debug_assert!((block.presence_mask >> i) & 1 != 0, "Block should exist before removal in rollback");
            unsafe {
                block.data[i as usize].assume_init_drop();
            }
            block.presence_mask &= !(1 << i);
            block.absence_mask &= !(1 << i);
            block.changed_mask &= !(1 << i);
            debug_assert_eq!(block.absence_mask & !block.presence_mask, 0, "absence_mask should be subset of presence_mask");
        }

        // Handle updates: for each slot, find the earliest snapshot that has it
        // This includes slots that were both "added" and "updated" (they existed before)
        let mut updated = all_updated_mask;
        while updated != 0 {
            let i = updated.trailing_zeros();
            debug_assert!(i < 128, "Index out of bounds in rollback: {}", i);
            updated &= !(1 << i);

            // Find the earliest snapshot (first in vector) that has this slot
            let snapshot_idx = snapshots
                .iter()
                .position(|s| (s.root.updated_mask >> i) & 1 != 0);

            if let Some(idx) = snapshot_idx {
                debug_assert!(idx < snapshots.len(), "Snapshot index out of bounds");
                let snapshot = &snapshots[idx];
                debug_assert!((snapshot.root.updated_mask >> i) & 1 != 0, "Snapshot should have this block");
                let middle_snapshot = unsafe { snapshot.root.data[i as usize].assume_init_ref() };

                // Ensure middle block exists before restoring
                if (block.presence_mask >> i) & 1 == 0 {
                    let new_middle = Block::new();
                    block.data[i as usize].write(Box::new(new_middle));
                    block.presence_mask |= 1 << i;
                }

                debug_assert!((block.presence_mask >> i) & 1 != 0, "Middle block should exist");
                let middle_block = unsafe { block.data[i as usize].assume_init_mut() };

                Self::rollback_middle_block_with_bitmasks(
                    snapshots,
                    idx,
                    i,
                    middle_snapshot,
                    middle_block,
                );

                if middle_block.absence_mask == u128::MAX {
                    block.absence_mask |= 1 << i;
                } else {
                    block.absence_mask &= !(1 << i);
                }
                debug_assert_eq!(block.absence_mask & !block.presence_mask, 0, "absence_mask should be subset of presence_mask");
            }

            block.changed_mask &= !(1 << i);
        }
    }

    fn rollback_middle_block_with_bitmasks(
        snapshots: &[Box<RollbackStorage<T>>],
        snapshot_idx: usize,
        root_idx: u32,
        _snapshot: &RollbackBlock<Box<RollbackBlock<T>>>,
        block: &mut Block<Box<Block<T>>>,
    ) where
        T: Clone,
    {
        // Pre-compute and cache relevant snapshot middle blocks to avoid repeated unsafe accesses
        struct CachedMiddle<'a, U> {
            snapshot_idx: usize,
            middle: &'a RollbackBlock<Box<RollbackBlock<U>>>,
        }
        
        debug_assert!(root_idx < 128, "root_idx out of bounds: {}", root_idx);
        debug_assert!(snapshot_idx < snapshots.len(), "snapshot_idx out of bounds");
        
        let mut cached_middles: Vec<CachedMiddle<'_, T>> = Vec::with_capacity(snapshots.len() - snapshot_idx);
        
        for (idx, s) in snapshots.iter().enumerate().skip(snapshot_idx) {
            if (s.root.updated_mask >> root_idx) & 1 != 0 {
                debug_assert!((s.root.updated_mask >> root_idx) & 1 != 0, "Snapshot should have this middle block");
                let middle = unsafe { s.root.data[root_idx as usize].assume_init_ref() };
                cached_middles.push(CachedMiddle {
                    snapshot_idx: idx,
                    middle,
                });
            }
        }

        // Compute union of all masks for this middle block across relevant snapshots
        let mut all_updated_mask = 0u128;
        let mut all_added_mask = 0u128;

        for cached in &cached_middles {
            all_updated_mask |= cached.middle.updated_mask;
            all_added_mask |= cached.middle.added_mask;
        }

        // Handle additions
        // BUT: if a slot is both "added" and "updated", it means it existed before
        // (was updated), so we should restore from "updated" instead of removing
        let mut added = all_added_mask & !all_updated_mask;
        while added != 0 {
            let i = added.trailing_zeros();
            added &= !(1 << i);

            unsafe {
                block.data[i as usize].assume_init_drop();
            }
            block.presence_mask &= !(1 << i);
            block.absence_mask &= !(1 << i);
            block.changed_mask &= !(1 << i);
        }

        // Handle updates: for each slot, find the earliest snapshot that has it
        // This includes slots that were both "added" and "updated" (they existed before)
        let mut updated = all_updated_mask;
        while updated != 0 {
            let i = updated.trailing_zeros();
            updated &= !(1 << i);

            // Find the earliest snapshot (starting from snapshot_idx) that has this slot
            // Use cached references for efficiency - return the cached reference directly
            let cached_middle_opt = cached_middles
                .iter()
                .find(|cached| (cached.middle.updated_mask >> i) & 1 != 0);

            if let Some(cached) = cached_middle_opt {
                let idx = cached.snapshot_idx;
                debug_assert!((cached.middle.updated_mask >> i) & 1 != 0, "Cached middle should have this inner block");
                let inner_snapshot = unsafe { cached.middle.data[i as usize].assume_init_ref() };

                // Ensure inner block exists before restoring
                if (block.presence_mask >> i) & 1 == 0 {
                    let new_inner = Block::new();
                    block.data[i as usize].write(Box::new(new_inner));
                    block.presence_mask |= 1 << i;
                }

                debug_assert!((block.presence_mask >> i) & 1 != 0, "Inner block should exist");
                let inner_block = unsafe { block.data[i as usize].assume_init_mut() };

                Self::rollback_inner_block_with_bitmasks(
                    snapshots,
                    idx,
                    root_idx,
                    i,
                    inner_snapshot,
                    inner_block,
                );

                // Ensure parent absence_mask correctly reflects inner block fullness
                if inner_block.absence_mask == u128::MAX {
                    block.absence_mask |= 1 << i;
                } else {
                    block.absence_mask &= !(1 << i);
                }
                debug_assert_eq!(block.absence_mask & !block.presence_mask, 0, "absence_mask should be subset of presence_mask");
            }

            block.changed_mask &= !(1 << i);
        }
    }

    fn rollback_inner_block_with_bitmasks(
        snapshots: &[Box<RollbackStorage<T>>],
        snapshot_idx: usize,
        root_idx: u32,
        middle_idx: u32,
        _snapshot: &RollbackBlock<T>,
        block: &mut Block<T>,
    ) where
        T: Clone,
    {
        debug_assert!(root_idx < 128, "root_idx out of bounds: {}", root_idx);
        debug_assert!(middle_idx < 128, "middle_idx out of bounds: {}", middle_idx);
        debug_assert!(snapshot_idx < snapshots.len(), "snapshot_idx out of bounds");

        // Pre-compute and cache relevant snapshot inner blocks to avoid repeated unsafe accesses
        // This stores inner_block_reference for snapshots that have this block
        struct CachedInner<'a, U> {
            inner: &'a RollbackBlock<U>,
        }
        
        let mut cached_inners: Vec<CachedInner<'_, T>> = Vec::with_capacity(snapshots.len() - snapshot_idx);
        
        for (_idx, s) in snapshots.iter().enumerate().skip(snapshot_idx) {
            if (s.root.updated_mask >> root_idx) & 1 != 0 {
                let middle = unsafe { s.root.data[root_idx as usize].assume_init_ref() };
                if (middle.updated_mask >> middle_idx) & 1 != 0 {
                    let inner = unsafe { middle.data[middle_idx as usize].assume_init_ref() };
                    cached_inners.push(CachedInner {
                        inner,
                    });
                }
            }
        }

        // Compute union of all masks for this inner block across relevant snapshots
        let mut all_updated_mask = 0u128;
        let mut all_added_mask = 0u128;

        for cached in &cached_inners {
            all_updated_mask |= cached.inner.updated_mask;
            all_added_mask |= cached.inner.added_mask;
        }

        // Handle additions
        // BUT: if a slot is both "added" and "updated", it means it existed before
        // (was updated), so we should restore from "updated" instead of removing
        let mut added = all_added_mask & !all_updated_mask;
        while added != 0 {
            let i = added.trailing_zeros();
            debug_assert!(i < 128, "Index out of bounds in inner rollback: {}", i);
            added &= !(1 << i);

            // Slot might not exist if it was already removed or never created
            if (block.presence_mask >> i) & 1 != 0 {
                unsafe {
                    block.data[i as usize].assume_init_drop();
                }
            }
            block.presence_mask &= !(1 << i);
            block.absence_mask &= !(1 << i);
            block.changed_mask &= !(1 << i);
            debug_assert_eq!(block.absence_mask & !block.presence_mask, 0, "absence_mask should be subset of presence_mask");
        }

        // Handle updates: for each slot, find the earliest snapshot that has it and use that value
        // BUT: if the earliest snapshot has "added" (and no "updated"), the slot didn't exist
        // before that snapshot, so we should remove it instead of restoring
        // Optimized: use cached inner blocks to avoid repeated unsafe accesses
        let mut updated = all_updated_mask;
        while updated != 0 {
            let i = updated.trailing_zeros();
            debug_assert!(i < 128, "Index out of bounds in inner rollback: {}", i);
            updated &= !(1 << i);

            // Find both earliest snapshot with this slot (for removal check)
            // and earliest snapshot with "updated" (for restoration)
            // Use cached references for efficiency
            let mut earliest_added: Option<&CachedInner<'_, T>> = None;
            let mut earliest_updated: Option<&CachedInner<'_, T>> = None;

            for cached in &cached_inners {
                let has_updated = (cached.inner.updated_mask >> i) & 1 != 0;
                let has_added = (cached.inner.added_mask >> i) & 1 != 0;

                // Check for added mask (for removal check)
                if earliest_added.is_none() && (has_updated || has_added) {
                    earliest_added = Some(cached);
                }

                // Check for updated mask (for restoration)
                if earliest_updated.is_none() && has_updated {
                    earliest_updated = Some(cached);
                }

                // Early exit if we found both
                if earliest_added.is_some() && earliest_updated.is_some() {
                    break;
                }
            }

            // Check if the earliest snapshot has "added" (and no "updated")
            // If so, the slot didn't exist before that snapshot, so remove it
            if let Some(cached) = earliest_added {
                // If earliest snapshot has "added" but not "updated", remove the slot
                if (cached.inner.added_mask >> i) & 1 != 0 && (cached.inner.updated_mask >> i) & 1 == 0 {
                    // Slot was added in earliest snapshot, so it didn't exist before - remove it
                    unsafe {
                        if (block.presence_mask >> i) & 1 != 0 {
                            block.data[i as usize].assume_init_drop();
                        }
                    }
                    block.presence_mask &= !(1 << i);
                    block.absence_mask &= !(1 << i);
                    block.changed_mask &= !(1 << i);
                    debug_assert_eq!(block.absence_mask & !block.presence_mask, 0, "absence_mask should be subset of presence_mask");
                    continue; // Skip restoration
                }
            }

            // Restore from the earliest snapshot with "updated"
            if let Some(cached) = earliest_updated {
                unsafe {
                    // If currently present, drop current value (presence_mask tracks current existence)
                    // Only drop if we're actually going to restore (avoid redundant drops)
                    let needs_drop = (block.presence_mask >> i) & 1 != 0;
                    if needs_drop {
                        block.data[i as usize].assume_init_drop();
                    }

                    // Restore value from the earliest snapshot that has it
                    debug_assert!((cached.inner.updated_mask >> i) & 1 != 0, "Cached inner should have this slot");
                    let old_val = cached.inner.data[i as usize].assume_init_ref().clone();
                    block.data[i as usize].write(old_val);
                }

                // Restore masks
                block.presence_mask |= 1 << i;
                block.absence_mask |= 1 << i;
                debug_assert_eq!(block.absence_mask & !block.presence_mask, 0, "absence_mask should be subset of presence_mask");
            }

            block.changed_mask &= !(1 << i);
        }
    }

    fn ensure_snapshot(
        snapshot: &mut Option<Box<RollbackStorage<T>>>,
        tick: Tick,
    ) -> &mut RollbackStorage<T> {
        match snapshot {
            None => {
                *snapshot = Some(RollbackStorage::new(None, tick));
                snapshot.as_deref_mut().expect("Failed to get mutable reference to newly created snapshot")
            }
            Some(s) if s.tick != tick => {
                let old = snapshot.take().expect("Failed to take snapshot from Some variant");
                *snapshot = Some(RollbackStorage::new(Some(old), tick));
                snapshot.as_deref_mut().expect("Failed to get mutable reference to newly created snapshot")
            }
            Some(s) => {
                // Tick matches: reuse existing snapshot
                &mut **s
            }
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

    pub fn set(&mut self, index: u32, value: &T)
    where
        T: Clone,
    {
        // Decode global index to ri, mi, ii
        // ri (0..128) * 16384 + mi (0..128) * 128 + ii (0..128)
        let ri = index >> 14;
        let mi = (index >> 7) & 0x7F;
        let ii = index & 0x7F;

        // Validate index is in bounds
        debug_assert!(ri < 128, "ri index out of bounds: ri={}, index={}", ri, index);
        if ri >= 128 {
            panic!("Index out of bounds: {}", index);
        }
        debug_assert!(mi < 128, "mi index out of bounds: mi={}, index={}", mi, index);
        debug_assert!(ii < 128, "ii index out of bounds: ii={}, index={}", ii, index);

        let root = &mut self.root;

        // Ensure middle block exists
        root.ensure_child_exists(ri);

        debug_assert!((root.presence_mask >> ri) & 1 != 0, "Middle block should exist after ensure_child_exists");
        let middle = unsafe { root.data[ri as usize].assume_init_mut() };

        // Ensure inner block exists
        if (middle.presence_mask >> mi) & 1 == 0 {
            // Create new inner block
            let new_inner = Block::new();

            middle.data[mi as usize].write(Box::new(new_inner));

            middle.presence_mask |= 1 << mi;
        }

        debug_assert!((middle.presence_mask >> mi) & 1 != 0, "Inner block should exist");
        let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

        // Cache presence_mask check to avoid reading the same bit twice
        let is_present = (inner.presence_mask >> ii) & 1 != 0;
        debug_assert!(ii < 128, "ii index out of bounds: {}", ii);

        if (inner.changed_mask >> ii) & 1 == 0 {
            // Only track rollback for non-temporary components
            if !T::IS_TEMPORARY {
                if is_present {
                    Self::ensure_snapshot(&mut self.snapshot, self.current_tick).mark_updated(
                        ri,
                        mi,
                        ii,
                        unsafe { inner.data[ii as usize].assume_init_ref() },
                    );
                } else {
                    Self::ensure_snapshot(&mut self.snapshot, self.current_tick)
                        .mark_added(ri, mi, ii);
                }
            }
            // Mark as changed
            inner.changed_mask |= 1 << ii;

            // Propagate changed_mask up the hierarchy
            middle.changed_mask |= 1 << mi;
            root.changed_mask |= 1 << ri;
        }

        // Set the value
        if is_present {
            // Already initialized, overwrite (drops old value)
            // Optimized: drop old value first, then write new one to avoid double-drop
            unsafe {
                let ptr = inner.data[ii as usize].as_mut_ptr();
                std::ptr::drop_in_place(ptr);
                std::ptr::write(ptr, value.clone());
            }
        } else {
            // Not initialized, write directly
            inner.data[ii as usize].write(value.clone());
        }

        // Update presence and absence masks
        inner.presence_mask |= 1 << ii;
        inner.absence_mask |= 1 << ii;
        debug_assert_eq!(inner.absence_mask & !inner.presence_mask, 0, "absence_mask should be subset of presence_mask");

        if inner.absence_mask == u128::MAX {
            middle.absence_mask |= 1 << mi;
        }
        if middle.absence_mask == u128::MAX {
            root.absence_mask |= 1 << ri;
        }
        debug_assert_eq!(middle.absence_mask & !middle.presence_mask, 0, "middle absence_mask should be subset of presence_mask");
        debug_assert_eq!(root.absence_mask & !root.presence_mask, 0, "root absence_mask should be subset of presence_mask");
    }

    pub fn remove(&mut self, index: u32) {
        // Decode global index to ri, mi, ii
        let ri = index >> 14;
        let mi = (index >> 7) & 0x7F;
        let ii = index & 0x7F;

        debug_assert!(ri < 128, "ri index out of bounds: ri={}, index={}", ri, index);
        debug_assert!(mi < 128, "mi index out of bounds: mi={}, index={}", mi, index);
        debug_assert!(ii < 128, "ii index out of bounds: ii={}, index={}", ii, index);

        let root = &mut self.root;
        if (root.presence_mask >> ri) & 1 == 0 {
            return; // Middle block doesn't exist
        }

        debug_assert!((root.presence_mask >> ri) & 1 != 0, "Middle block should exist");
        let middle = unsafe { root.data[ri as usize].assume_init_mut() };
        if (middle.presence_mask >> mi) & 1 == 0 {
            return; // Inner block doesn't exist
        }

        debug_assert!((middle.presence_mask >> mi) & 1 != 0, "Inner block should exist");
        let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

        // Check if component actually exists before removing
        if (inner.presence_mask >> ii) & 1 == 0 {
            return; // Component doesn't exist, nothing to remove
        }
        debug_assert!((inner.presence_mask >> ii) & 1 != 0, "Component should exist before removal");

        // Log change for rollback if not already changed in this tick
        // Only track rollback for non-temporary components
        if (inner.changed_mask >> ii) & 1 == 0 {
            if !T::IS_TEMPORARY {
                Self::ensure_snapshot(&mut self.snapshot, self.current_tick).mark_updated(
                    ri,
                    mi,
                    ii,
                    unsafe { inner.data[ii as usize].assume_init_ref() },
                );
            }

            inner.changed_mask |= 1 << ii;
            middle.changed_mask |= 1 << mi;
            root.changed_mask |= 1 << ri;
        }

        // NOTE: We clear presence_mask here because it should track current existence
        // This means we treat the slot as uninitialized after removal

        // Drop the value
        unsafe {
            inner.data[ii as usize].assume_init_drop();
        }

        // Clear presence and absence bits
        inner.presence_mask &= !(1 << ii);
        inner.absence_mask &= !(1 << ii);
        debug_assert_eq!(inner.absence_mask & !inner.presence_mask, 0, "absence_mask should be subset of presence_mask after removal");

        // Maintain invariant: propagate non-fullness up the hierarchy
        if inner.absence_mask != u128::MAX {
            middle.absence_mask &= !(1 << mi);
        }

        if middle.absence_mask != u128::MAX {
            root.absence_mask &= !(1 << ri);
        }
        debug_assert_eq!(middle.absence_mask & !middle.presence_mask, 0, "middle absence_mask should be subset of presence_mask");
        debug_assert_eq!(root.absence_mask & !root.presence_mask, 0, "root absence_mask should be subset of presence_mask");
    }

    pub fn get(&self, index: u32) -> Option<&T> {
        let ri = index >> 14;
        let mi = (index >> 7) & 0x7F;
        let ii = index & 0x7F;

        debug_assert!(ri < 128, "ri index out of bounds: ri={}, index={}", ri, index);
        debug_assert!(mi < 128, "mi index out of bounds: mi={}, index={}", mi, index);
        debug_assert!(ii < 128, "ii index out of bounds: ii={}, index={}", ii, index);

        let root = &self.root;
        // Combine presence check with access - if block doesn't exist, we can't access it
        if (root.presence_mask >> ri) & 1 == 0 {
            return None;
        }

        debug_assert!((root.presence_mask >> ri) & 1 != 0, "Middle block should exist");
        let middle = unsafe { root.data[ri as usize].assume_init_ref() };
        // Combine presence check with access
        if (middle.presence_mask >> mi) & 1 == 0 {
            return None;
        }

        debug_assert!((middle.presence_mask >> mi) & 1 != 0, "Inner block should exist");
        let inner = unsafe { middle.data[mi as usize].assume_init_ref() };

        // Check if component exists (presence_mask is sufficient - absence_mask is kept in sync)
        if (inner.presence_mask >> ii) & 1 == 0 {
            return None;
        }

        debug_assert!((inner.presence_mask >> ii) & 1 != 0, "Component should exist");
        unsafe { Some(inner.data[ii as usize].assume_init_ref()) }
    }

    pub fn get_mut(&mut self, index: u32) -> &mut T
    where
        T: Component,
    {
        let ri = index >> 14;
        let mi = (index >> 7) & 0x7F;
        let ii = index & 0x7F;

        debug_assert!(ri < 128, "ri index out of bounds: ri={}, index={}", ri, index);
        debug_assert!(mi < 128, "mi index out of bounds: mi={}, index={}", mi, index);
        debug_assert!(ii < 128, "ii index out of bounds: ii={}, index={}", ii, index);

        let root = &mut self.root;

        // Combine presence check with access
        if (root.presence_mask >> ri) & 1 == 0 {
            panic!("Index out of bounds: {}", index);
        }

        debug_assert!((root.presence_mask >> ri) & 1 != 0, "Middle block should exist");
        let middle = unsafe { root.data[ri as usize].assume_init_mut() };

        // Combine presence check with access
        if (middle.presence_mask >> mi) & 1 == 0 {
            panic!("Index out of bounds: {}", index);
        }

        debug_assert!((middle.presence_mask >> mi) & 1 != 0, "Inner block should exist");
        let inner = unsafe { middle.data[mi as usize].assume_init_mut() };

        // Check if component exists (presence_mask is sufficient - absence_mask is kept in sync)
        if (inner.presence_mask >> ii) & 1 == 0 {
            panic!("Index out of bounds: {}", index);
        }
        debug_assert!((inner.presence_mask >> ii) & 1 != 0, "Component should exist");

        // Set changed_mask at all levels only if not already set at inner level
        // Only track rollback for non-temporary components
        // Note: We already verified presence_mask above, so we know it's set
        if (inner.changed_mask >> ii) & 1 == 0 {
            if !T::IS_TEMPORARY {
                Self::ensure_snapshot(&mut self.snapshot, self.current_tick).mark_updated(
                    ri,
                    mi,
                    ii,
                    unsafe { inner.data[ii as usize].assume_init_ref() },
                );
            }

            inner.changed_mask |= 1 << ii;
            middle.changed_mask |= 1 << mi;
            root.changed_mask |= 1 << ri;
        }

        unsafe { inner.data[ii as usize].assume_init_mut() }
    }
}

use crate::entity::Entity;

impl Storage<Entity> {
    pub fn spawn(&mut self) -> Entity {
        let root = &mut self.root;

        // 1. Find free slot in root
        let free_root = !root.absence_mask;

        if free_root == 0 {
            panic!("Storage is full");
        }

        let ri = free_root.trailing_zeros();
        debug_assert!(ri < 128, "ri index out of bounds: {}", ri);

        root.ensure_child_exists(ri);
        debug_assert!((root.presence_mask >> ri) & 1 != 0, "Middle block should exist after ensure_child_exists");

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
            debug_assert!(mi < 128, "mi index out of bounds: {}", mi);

            // Ensure inner block exists
            if (middle.presence_mask >> mi) & 1 == 0 {
                let new_inner = Block::new();
                middle.data[mi as usize].write(Box::new(new_inner));
                middle.presence_mask |= 1 << mi;
                // Ensure the new inner block is not marked as full (it's empty)
                middle.absence_mask &= !(1 << mi);
            }
            debug_assert!((middle.presence_mask >> mi) & 1 != 0, "Inner block should exist");
            debug_assert_eq!(middle.absence_mask & !middle.presence_mask, 0, "absence_mask should be subset of presence_mask");

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
                debug_assert!(ii < 128, "ii index out of bounds: {}", ii);

                // Initialize or update the entity
                let global_index = ri * 16384 + mi * 128 + ii;
                
                // Check if this is a respawn (entity was present but deleted) or a new spawn
                // We need to check BEFORE potentially initializing the slot
                let is_respawn = (inner.presence_mask >> ii) & 1 != 0;
                
                if !is_respawn {
                    // First time initializing this slot - it's a new spawn
                    inner.data[ii as usize].write(Entity::new(global_index, 0));
                    inner.presence_mask |= 1 << ii;
                }

                // Increment generation for the allocated entity
                let entity = unsafe { inner.data[ii as usize].assume_init_mut() };

                // Log to rollback storage before making changes
                // Only log if this slot hasn't been changed in this tick yet
                if (inner.changed_mask >> ii) & 1 == 0 {
                    if is_respawn {
                        // Entity was present, this is a respawn - save old state
                        Self::ensure_snapshot(&mut self.snapshot, self.current_tick)
                            .mark_updated(ri, mi, ii, entity);
                    } else {
                        // Entity slot was not present, this is a new spawn
                        Self::ensure_snapshot(&mut self.snapshot, self.current_tick)
                            .mark_added(ri, mi, ii);
                    }
                }

                entity.increment_generation();

                // Mark as occupied
                inner.absence_mask |= 1 << ii;
                debug_assert_eq!(inner.absence_mask & !inner.presence_mask, 0, "absence_mask should be subset of presence_mask");

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
            debug_assert_eq!(middle.absence_mask & !middle.presence_mask, 0, "absence_mask should be subset of presence_mask");
            debug_assert_eq!(root.absence_mask & !root.presence_mask, 0, "absence_mask should be subset of presence_mask");

            // Propagate changed_mask up the hierarchy
            middle.changed_mask |= 1 << mi;
        }

        root.changed_mask |= 1 << ri;

        // Re-traverse to return the reference.
        unsafe {
            let middle = root.data[ri as usize].assume_init_mut();
            let inner = middle.data[mi as usize].assume_init_mut();

            *inner.data[ii as usize].assume_init_ref()
        }
    }
}

#[cfg(test)]
#[path = "storage.tests.rs"]
mod tests;
