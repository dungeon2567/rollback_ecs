use std::mem::MaybeUninit;

pub struct Block<T> {
    pub presence_mask: u128,
    pub absence_mask: u128,
    pub changed_mask: u128,
    pub data: [MaybeUninit<T>; 128],
}

pub struct RollbackBlock<T> {
    pub updated_mask: u128,
    pub added_mask: u128,
    pub data: [MaybeUninit<T>; 128],
}

impl<T> Block<T> {
    pub fn new() -> Self {
        Block {
            presence_mask: 0,
            absence_mask: 0,
            changed_mask: 0,
            data: std::array::from_fn(|_| std::mem::MaybeUninit::uninit()),
        }
    }

    pub fn restore_from(&mut self, snapshot: &RollbackBlock<T>)
    where
        T: Clone,
    {
        // First, drop any existing data that's present
        let mut m = self.presence_mask;

        while m != 0 {
            let start = m.trailing_zeros();
            let run = (m >> start).trailing_ones();

            for i in 0..run {
                let idx = (start + i) as usize;
                unsafe {
                    self.data[idx].assume_init_drop();
                }
            }

            let range_mask = if run == 128 {
                u128::MAX
            } else {
                ((1u128 << run) - 1) << start
            };
            m &= !range_mask;
        }

        // Restore masks from snapshot
        self.presence_mask = snapshot.updated_mask;
        self.absence_mask = snapshot.added_mask;
        self.changed_mask = 0; // Reset changed mask on restore

        // Copy sparse data from snapshot to self
        let mut mask = snapshot.updated_mask;

        while mask != 0 {
            let start = mask.trailing_zeros();
            let run = (mask >> start).trailing_ones();

            for i in 0..run {
                let idx = (start + i) as usize;
                unsafe {
                    let val = snapshot.data[idx].assume_init_ref().clone();
                    self.data[idx].write(val);
                }
            }

            if run == 128 {
                mask = 0;
            } else {
                let run_mask = ((1u128 << run) - 1) << start;
                mask &= !run_mask;
            }
        }
    }

    pub fn snapshot(&self) -> RollbackBlock<T>
    where
        T: Clone,
    {
        let mut data: [MaybeUninit<T>; 128] = std::array::from_fn(|_| MaybeUninit::uninit());

        let mut mask = self.presence_mask;

        while mask != 0 {
            let start = mask.trailing_zeros();
            let run = (mask >> start).trailing_ones();

            for i in 0..run {
                let idx = (start + i) as usize;
                unsafe {
                    let val = self.data[idx].assume_init_ref().clone();
                    data[idx].write(val);
                }
            }

            if run == 128 {
                mask = 0;
            } else {
                let run_mask = ((1u128 << run) - 1) << start;
                mask &= !run_mask;
            }
        }

        RollbackBlock {
            updated_mask: self.presence_mask,
            added_mask: self.absence_mask,
            data,
        }
    }
}

impl<T> Block<Box<Block<T>>> {
    pub fn ensure_child_exists(&mut self, index: u32) {
        debug_assert!(index < 128, "Index out of bounds: {}", index);
        if (self.presence_mask >> index) & 1 == 0 {
            let new_block = Block::new();
            self.data[index as usize].write(Box::new(new_block));
            self.presence_mask |= 1 << index;
            // Ensure the new child block is not marked as full (it's empty)
            self.absence_mask &= !(1 << index);
            debug_assert_eq!(self.absence_mask & !self.presence_mask, 0, "absence_mask should be subset of presence_mask");
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

                let range_mask = if run == 128 {
                    u128::MAX
                } else {
                    ((1u128 << run) - 1) << start
                };

                m &= !range_mask;
            }
        }
    }
}

impl<T> Drop for RollbackBlock<T> {
    fn drop(&mut self) {
        let mut m = self.updated_mask;

        unsafe {
            let ptr = self.data.as_mut_ptr();

            while m != 0 {
                let start = m.trailing_zeros();

                let run = (m >> start).trailing_ones();

                for i in 0..run {
                    ptr.add((start + i) as usize).read().assume_init_drop();
                }

                let range_mask = if run == 128 {
                    u128::MAX
                } else {
                    ((1u128 << run) - 1) << start
                };

                m &= !range_mask;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_sparse() {
        let mut block = Block::<u32>::new();

        // Set data at indices 0, 1, 2 (111) and 6, 7, 8 (111 shifted)
        // 111_000_111 pattern roughly

        // 0, 1, 2
        block.data[0].write(10);
        block.data[1].write(11);
        block.data[2].write(12);

        block.presence_mask |= 0b111;

        // 6, 7, 8
        block.data[6].write(16);
        block.data[7].write(17);
        block.data[8].write(18);

        block.presence_mask |= 0b111000000;

        let snapshot = block.snapshot();

        assert_eq!(snapshot.updated_mask, block.presence_mask);

        // Verify data in sparse snapshot
        unsafe {
            assert_eq!(*snapshot.data[0].assume_init_ref(), 10);
            assert_eq!(*snapshot.data[1].assume_init_ref(), 11);
            assert_eq!(*snapshot.data[2].assume_init_ref(), 12);
            assert_eq!(*snapshot.data[6].assume_init_ref(), 16);
            assert_eq!(*snapshot.data[7].assume_init_ref(), 17);
            assert_eq!(*snapshot.data[8].assume_init_ref(), 18);
        }
    }

    #[test]
    fn test_restore_from() {
        // Create a block with some data
        let mut block = Block::<u32>::new();

        // Set data at indices 0, 1, 2 and 6, 7, 8
        block.data[0].write(10);
        block.data[1].write(11);
        block.data[2].write(12);
        block.data[6].write(16);
        block.data[7].write(17);
        block.data[8].write(18);

        block.presence_mask = 0b111000111;
        block.absence_mask = 0b1000;
        block.changed_mask = 0b111;

        // Take a snapshot
        let snapshot = block.snapshot();

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
