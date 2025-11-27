use rollback_ecs::prelude::*;

// 1. Define Components
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

// 2. Define a System
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

fn main() {
    let mut world = World::new();

    // 3. Spawn Entities
    let e = world.spawn();

    world.set(e, &Position { x: 0.0, y: 0.0 });
    world.set(e, &Velocity { x: 1.0, y: 0.0 });

    // 4. Run Simulation
    let storage = world.get_storage::<Position>();

    unsafe {
        (*storage.get()).set_tick(Tick::new(1));
    }

    // Run systems...
    world.run_system::<MovementSystem>();

    // 5. Rollback
    // Revert state to Tick 0 (before movement)
    world.rollback(Tick::new(0));
}
