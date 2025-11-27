use crate::component::Component;
use crate::storage::Storage;
use std::ops::{Deref, DerefMut};

pub struct View<'a, T: Component> {
    pub data: &'a T,
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
    pub storage: &'a mut Storage<T>,
    pub index: u32,
}

impl<'a, T: Component + PartialEq + Clone> ViewMut<'a, T> {
    pub fn new(storage: &'a mut Storage<T>, index: u32) -> Self {
        Self { storage, index }
    }
}

impl<'a, T: Component> Deref for ViewMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.storage.get(self.index).expect("Index out of bounds")
    }
}

impl<'a, T: Component> DerefMut for ViewMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.storage.get_mut(self.index)
    }
}
