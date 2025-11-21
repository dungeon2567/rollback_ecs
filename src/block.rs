use std::mem::MaybeUninit;
use std::alloc::{Allocator, Global};
use crate::component::Component;

pub struct Block<T> {
    pub presence_mask: u128,
    pub absence_mask: u128,
    pub changed_mask: u128,
    pub data: [MaybeUninit<T>; 128]
}

pub struct SnapshotBlock<T, A: Allocator = Global> {
    pub presence_mask: u128,
    pub absence_mask: u128,
    pub data: Box<[T], A>
}

impl<T> Block<T> {
    pub fn new() -> Self {
        Block {
            presence_mask: 0,
            absence_mask: 0,
            changed_mask: 0,
            data: std::array::from_fn(|_| std::mem::MaybeUninit::uninit())
        }
    }

    pub fn restore_from<A: Allocator>(&mut self, snapshot: &SnapshotBlock<T, A>)
    where T: Clone
    {
        // First, drop any existing data that's present
        let mut m = self.presence_mask;
        unsafe {
            let ptr = self.data.as_mut_ptr();
            while m != 0 {
                let start = m.trailing_zeros();
                let run = (m >> start).trailing_ones();

                for i in 0..run {
                    ptr.add((start + i) as usize).read().assume_init_drop();
                }

                let range_mask = if run == 128 { u128::MAX } else { ((1u128 << run) - 1) << start };
                m &= !range_mask;
            }
        }

        // Restore masks from snapshot
        self.presence_mask = snapshot.presence_mask;
        self.absence_mask = snapshot.absence_mask;
        self.changed_mask = 0; // Reset changed mask on restore

        // Unpack dense data back into sparse array
        let mut mask = snapshot.presence_mask;
        let mut data_idx = 0;

        while mask != 0 {
            let start = mask.trailing_zeros();
            let run = (mask >> start).trailing_ones();

            unsafe {
                let src_ptr = snapshot.data.as_ptr().add(data_idx);
                let dst_ptr = self.data.as_mut_ptr().add(start as usize) as *mut T;

                // Clone each element in the run
                for i in 0..run as usize {
                    let val = (*src_ptr.add(i)).clone();
                    dst_ptr.add(i).write(val);
                }
            }

            data_idx += run as usize;

            if run == 128 {
                mask = 0;
            } else {
                let run_mask = ((1u128 << run) - 1) << start;
                mask &= !run_mask;
            }
        }
    }

    pub fn snapshot<A: Allocator>(&self, allocator: A) -> SnapshotBlock<T, A>
    where T: Clone
    {
        let count = self.presence_mask.count_ones() as usize;
        let mut data = Box::new_uninit_slice_in(count, allocator);

        let mut mask = self.presence_mask;
        let mut data_idx = 0;

        while mask != 0 {
            let start = mask.trailing_zeros();
            let run = (mask >> start).trailing_ones();

            unsafe {
                let src_ptr = self.data.as_ptr().add(start as usize) as *const T;
                let dst_ptr = data.as_mut_ptr().add(data_idx) as *mut T;

                // Clone each element in the run
                for i in 0..run as usize {
                    let val = (*src_ptr.add(i)).clone();
                    dst_ptr.add(i).write(val);
                }
            }

            data_idx += run as usize;

            if run == 128 {
                mask = 0;
            } else {
                let run_mask = ((1u128 << run) - 1) << start;
                mask &= !run_mask;
            }
        }

        SnapshotBlock {
            presence_mask: self.presence_mask,
            absence_mask: self.absence_mask,
            data: unsafe { data.assume_init() }
        }
    }
}

impl<T> Block<Box<Block<T>>> {
    pub fn ensure_child_exists(&mut self, index: u32) {
        if (self.presence_mask >> index) & 1 == 0 {
            let new_block = Block::new();
            self.data[index as usize].write(Box::new(new_block));
            self.presence_mask |= 1 << index;
            // Ensure the new child block is not marked as full (it's empty)
            self.absence_mask &= !(1 << index);
        }
    }
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        let mut m = self.presence_mask;

        unsafe {
            let ptr = self.data.as_mut_ptr();

            while m != 0 {
                let start = m.trailing_zeros();

                let run = (m >> start).trailing_ones();

                for i in 0..run {
                    ptr.add((start + i) as usize).read().assume_init_drop();
                }

                let range_mask = if run == 128 { u128::MAX } else { ((1u128 << run) - 1) << start };

                m &= !range_mask;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tick::Tick;

    #[test]
    fn test_snapshot_dense() {
        let mut block = Block::<u32>::new();
        
        // Set data at indices 0, 1, 2 (111) and 6, 7, 8 (111 shifted)
        // 111_000_111 pattern roughly
        
        // 0, 1, 2
        unsafe {
            block.data[0].write(10);
            block.data[1].write(11);
            block.data[2].write(12);
        }

        block.presence_mask |= 0b111;

        // 6, 7, 8
        unsafe {
            block.data[6].write(16);
            block.data[7].write(17);
            block.data[8].write(18);
        }

        block.presence_mask |= 0b111000000;

        let snapshot = block.snapshot(std::alloc::Global);

        assert_eq!(snapshot.presence_mask, block.presence_mask);
        assert_eq!(snapshot.data.len(), 6);

        assert_eq!(snapshot.data.as_ref(), &[10, 11, 12, 16, 17, 18]);
    }

    #[test]
    fn test_restore_from() {
        // Create a block with some data
        let mut block = Block::<u32>::new();
        
        // Set data at indices 0, 1, 2 and 6, 7, 8
        unsafe {
            block.data[0].write(10);
            block.data[1].write(11);
            block.data[2].write(12);
            block.data[6].write(16);
            block.data[7].write(17);
            block.data[8].write(18);
        }
        
        block.presence_mask = 0b111000111;
        block.absence_mask = 0b1000;
        block.changed_mask = 0b111;
        
        // Take a snapshot
        let snapshot = block.snapshot(std::alloc::Global);
        
        // Modify the block - change values and masks
        unsafe {
            // Drop existing data first
            for i in [0, 1, 2, 6, 7, 8] {
                block.data[i].assume_init_drop();
            }
            
            // Write different data at different positions
            block.data[5].write(99);
            block.data[10].write(88);
        }
        block.presence_mask = 0b10000100000;
        block.absence_mask = 0;
        block.changed_mask = 0b11111;
        
        // Restore from snapshot
        block.restore_from(&snapshot);
        
        // Verify masks are restored
        assert_eq!(block.presence_mask, 0b111000111);
        assert_eq!(block.absence_mask, 0b1000);
        assert_eq!(block.changed_mask, 0); // Should be reset
        
        // Verify data is restored correctly
        unsafe {
            assert_eq!(*block.data[0].as_ptr(), 10);
            assert_eq!(*block.data[1].as_ptr(), 11);
            assert_eq!(*block.data[2].as_ptr(), 12);
            assert_eq!(*block.data[6].as_ptr(), 16);
            assert_eq!(*block.data[7].as_ptr(), 17);
            assert_eq!(*block.data[8].as_ptr(), 18);
        }
    }
}
