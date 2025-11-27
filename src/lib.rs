#![feature(allocator_api)]

// Allow this crate to reference itself as ::rollback_ecs::
// This enables proc macros to use absolute paths that work both internally and externally
extern crate self as rollback_ecs;

pub mod block;
pub mod component;
pub mod entity;
pub mod prelude;
pub mod rollback;
pub mod safety;
pub mod scheduler;
pub mod storage;
pub mod system;
pub mod tick;
pub mod view;
pub mod world;

#[cfg(target_arch = "wasm32")]
#[cfg(test)]
mod wasm_tests;

#[allow(unused_imports)]
use rollback_macros::pipeline_group;
