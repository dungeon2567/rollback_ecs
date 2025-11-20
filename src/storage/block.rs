use std::mem::MaybeUninit;
use crate::component::Component;
use crate::tick::Tick;

pub struct Block<T> {
    pub presence_mask: u128,
    pub absence_mask: u128,
    pub changed_mask: u128,
    pub data: [MaybeUninit<T>; 128]
}

pub struct SnapshotBlock<'a, T> {
    pub presence_mask: u128,
    pub absence_mask: u128,
    pub tick: Tick,
    pub data: &'a [T]
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
