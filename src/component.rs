use std::any::Any;
use std::cell::OnceCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

pub fn next_id() -> usize {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Component: Any + Default where Self: Sized {
    fn type_index() -> usize;
}

pub trait Tag: Any where Self: Sized{

}

#[derive(Default, Clone)]
pub struct Destroyed {}

impl crate::component::Component for Destroyed {
    fn type_index() -> usize {
        static TYPE_INDEX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *TYPE_INDEX.get_or_init(|| crate::component::next_id())
    }
}


pub use rollback_macros::Component;
pub use rollback_macros::Tag;
