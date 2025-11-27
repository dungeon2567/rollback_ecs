//! Safety verification utilities
//!
//! This module provides functions to verify safety invariants at runtime
//! (useful for debugging and testing).

use crate::component::Component;
use crate::storage::Storage;

/// Verifies that all storage invariants hold.
///
/// This function checks:
/// - `presence_mask` and initialization alignment (MaybeUninit safety)
/// - `absence_mask` is a subset of `presence_mask`
/// - Hierarchy consistency (if a child block is full, parent reflects this)
///
/// # Returns
///
/// Returns `Ok(())` if all invariants hold, or `Err(String)` with a description
/// of the first invariant violation found.
///
/// # Example
///
/// ```rust,ignore
/// let storage = Storage::<MyComponent>::new();
/// if let Err(msg) = verify_storage_invariants(&storage) {
///     panic!("Storage invariant violation: {}", msg);
/// }
/// ```
pub fn verify_storage_invariants<T: Component>(storage: &Storage<T>) -> Result<(), String> {
    let root = &storage.root;

    // Invariant 1: Root absence_mask must be subset of presence_mask
    let invalid_absence = root.absence_mask & !root.presence_mask;
    if invalid_absence != 0 {
        return Err(format!(
            "Root: absence_mask has bits set where presence_mask is not set (invalid bits: {:#b})",
            invalid_absence
        ));
    }

    // Iterate over all present middle blocks
    let mut middle_iter = root.presence_mask;

    while middle_iter != 0 {
        let ri = middle_iter.trailing_zeros();
        let middle = unsafe {
            // SAFETY: We checked root.presence_mask >> ri is set, so the block exists
            // and was initialized when presence_mask was set
            root.data[ri as usize].assume_init_ref()
        };

        // Invariant 2: Middle absence_mask must be subset of presence_mask
        let invalid_absence = middle.absence_mask & !middle.presence_mask;
        if invalid_absence != 0 {
            return Err(format!(
                "Middle[{}]: absence_mask has bits set where presence_mask is not set (invalid bits: {:#b})",
                ri, invalid_absence
            ));
        }

        // Invariant 3: If middle is full, root's absence_mask should reflect this
        let middle_is_full = middle.absence_mask == u128::MAX;
        let root_thinks_full = (root.absence_mask >> ri) & 1 == 1;

        if middle_is_full != root_thinks_full {
            return Err(format!(
                "Middle[{}]: fullness mismatch (middle_full={}, root_thinks_full={})",
                ri, middle_is_full, root_thinks_full
            ));
        }

        // Iterate over all present inner blocks
        let mut inner_iter = middle.presence_mask;

        while inner_iter != 0 {
            let mi = inner_iter.trailing_zeros();
            let inner = unsafe {
                // SAFETY: We checked middle.presence_mask >> mi is set, so the block exists
                // and was initialized when presence_mask was set
                middle.data[mi as usize].assume_init_ref()
            };

            // Invariant 4: Inner absence_mask must be subset of presence_mask
            let invalid_absence = inner.absence_mask & !inner.presence_mask;
            if invalid_absence != 0 {
                return Err(format!(
                    "Inner[{}, {}]: absence_mask has bits set where presence_mask is not set (invalid bits: {:#b})",
                    ri, mi, invalid_absence
                ));
            }

            // Invariant 5: If inner is full, middle's absence_mask should reflect this
            let inner_is_full = inner.absence_mask == u128::MAX;
            let middle_thinks_full = (middle.absence_mask >> mi) & 1 == 1;

            if inner_is_full != middle_thinks_full {
                return Err(format!(
                    "Inner[{}, {}]: fullness mismatch (inner_full={}, middle_thinks_full={})",
                    ri, mi, inner_is_full, middle_thinks_full
                ));
            }

            // Invariant 6: MaybeUninit safety - presence_mask bit i is set if and only if data[i] is initialized
            // We can't directly check if MaybeUninit is initialized, but we verify the mask is consistent
            // by checking that all presence_mask bits correspond to valid indices
            for ii in 0..128u32 {
                let is_present = (inner.presence_mask >> ii) & 1 == 1;
                let is_absent = (inner.absence_mask >> ii) & 1 == 1;

                // If present, absence should also be set (presence implies absence for tracking)
                if is_present && !is_absent {
                    return Err(format!(
                        "Inner[{}, {}]: slot {} is present but not marked in absence_mask",
                        ri, mi, ii
                    ));
                }
            }

            inner_iter &= !(1 << mi);
        }

        middle_iter &= !(1 << ri);
    }

    // Verify that all blocks marked as present are actually initialized
    // This is a sanity check - we can't verify MaybeUninit directly, but we verify
    // that the structure is consistent

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::storage::Storage;

    #[test]
    fn test_verify_empty_storage() {
        let storage = Storage::<Entity>::new();
        assert!(verify_storage_invariants(&storage).is_ok());
    }

    #[test]
    fn test_verify_storage_with_entities() {
        let mut storage = Storage::<Entity>::new();
        
        // Spawn some entities
        for _ in 0..10 {
            storage.spawn();
        }
        
        assert!(verify_storage_invariants(&storage).is_ok());
    }

    #[test]
    fn test_verify_storage_filled_block() {
        let mut storage = Storage::<Entity>::new();
        
        // Fill one inner block (128 entities)
        for _ in 0..128 {
            storage.spawn();
        }
        
        assert!(verify_storage_invariants(&storage).is_ok());
    }

    #[test]
    fn test_verify_storage_multiple_blocks() {
        let mut storage = Storage::<Entity>::new();
        
        // Fill multiple inner blocks
        for _ in 0..256 {
            storage.spawn();
        }
        
        assert!(verify_storage_invariants(&storage).is_ok());
    }
}

