use std::ops::{Deref, DerefMut};
use crate::component::{Component};
use crate::storage::block::Block;

pub struct View<'a, T: Component> {
    pub data: &'a T
}

impl<'a, T: Component> View<'a, T> {
    pub fn new(data: &'a T) -> View<'a, T> {
        Self { data }
    }
}

impl<'a, T: Component> Deref for View<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.data
    }
}

pub struct ViewMut<'a, T: Component> {
    data: & 'a mut T,
    block: &'a mut Block<T>,
    index: usize
}

impl <'a, T: Component + PartialEq + Clone> ViewMut<'a, T> {
    pub fn new(data: &'a mut T, block: &'a mut Block<T>, index: usize) -> Self {
        Self { data, block, index }
    }

    pub fn set(&mut self, new_value: &T) -> bool {
        if (self.block.changed_mask & (1 << self.index)) != 0 {
            if *self.data != *  new_value {
                self.block.changed_mask |= 1u128 << self.index;

                *self.data = new_value.clone();

                return true
            }
        } else {
            *self.data = new_value.clone();

            return true;
        }

        false
    }
}

impl<'a, T: Component> Deref for ViewMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.data
    }
}