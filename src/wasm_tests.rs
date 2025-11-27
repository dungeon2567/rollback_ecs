// WASM-specific tests using wasm-bindgen-test
// These tests verify that the library works correctly when compiled to WASM

use wasm_bindgen_test::*;
use crate::prelude::*;
use crate::system::DestroySystem;

wasm_bindgen_test_configure!(run_in_browser);

// Test basic component functionality in WASM
#[wasm_bindgen_test]
fn test_wasm_component_basic() {
    #[derive(Component, Default, Clone, Debug, PartialEq)]
    struct TestComponent {
        value: i32,
    }

    let mut world = World::new();
    let entity = world.spawn();
    
    world.set(entity, &TestComponent { value: 42 });
    
    let storage = world.get_storage::<TestComponent>();
    let retrieved = unsafe { (*storage.get()).get(entity.index()) };
    assert_eq!(retrieved.unwrap().value, 42);
}

// Test entity spawn/destroy in WASM
#[wasm_bindgen_test]
fn test_wasm_entity_spawn_destroy() {
    let mut world = World::new();
    let ents = world.get_storage::<Entity>();
    
    let e1 = unsafe { (*ents.get()).spawn() };
    let _e2 = unsafe { (*ents.get()).spawn() };
    
    assert_eq!(unsafe { (*ents.get()).len() }, 2);
    
    world.destroy(e1);
    world.run_system::<DestroySystem>();
    
    assert_eq!(unsafe { (*ents.get()).len() }, 1);
}

// Test tick functionality in WASM
#[wasm_bindgen_test]
fn test_wasm_tick() {
    let tick1 = Tick::new(10);
    let tick2 = Tick::new(20);
    
    assert!(tick2.is_after(tick1));
    assert!(!tick1.is_after(tick2));
    
    let diff = tick2.diff(tick1);
    assert_eq!(diff.value(), 10);
}

// Test basic system execution in WASM (without parallel feature)
#[wasm_bindgen_test]
fn test_wasm_system_execution() {
    #[derive(Component, Default, Clone, Debug, PartialEq)]
    struct Position {
        x: f32,
        y: f32,
    }
    
    #[derive(Component, Default, Clone, Debug, PartialEq)]
    struct Velocity {
        x: f32,
        y: f32,
    }
    
    system! {
        MovementSystem {
            query! {
                fn move_entities(pos: &mut ViewMut<Position>, vel: View<Velocity>) {
                    pos.x += vel.x;
                    pos.y += vel.y;
                }
            }
        }
    }
    
    let mut world = World::new();
    let entity = world.spawn();
    
    world.set(entity, &Position { x: 0.0, y: 0.0 });
    world.set(entity, &Velocity { x: 1.0, y: 2.0 });
    
    world.run_system::<MovementSystem>();
    
    let pos_storage = world.get_storage::<Position>();
    let pos = unsafe { (*pos_storage.get()).get(entity.index()) };
    assert_eq!(pos.unwrap().x, 1.0);
    assert_eq!(pos.unwrap().y, 2.0);
}

