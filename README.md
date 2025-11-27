# rollback_ecs

[![CI](https://github.com/dungeon2567/rollback_ecs/actions/workflows/ci.yml/badge.svg)](https://github.com/dungeon2567/rollback_ecs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/dungeon2567/rollback_ecs/branch/main/graph/badge.svg)](https://codecov.io/gh/dungeon2567/rollback_ecs)

A high-performance, deterministic Entity Component System (ECS) written in Rust, designed specifically for rollback networking (e.g., GGPO) in games.

## Key Features

### 🔄 Deterministic Rollback
Built from the ground up for rollback networking.
- **Efficient Snapshots**: Only stores deltas (`added_mask` and `updated_mask`) per tick, minimizing memory usage.
- **Fast Rollback**: Recursively reverts state to any target tick using the hierarchical storage structure.
- **Tick-based**: Explicit `Tick` management for precise time control.
- **Non-blittable Support**: Works with any `Clone` type, not just blittable (copy) types. Only clones components that actually changed, avoiding unnecessary work per tick.

### 🌳 Hierarchical Sparse Bitset Storage
Data is organized in a 3-level hierarchical structure (Root -> Middle -> Inner) using bitmasks.
- **Sparse & Dense**: Efficiently handles both sparse and dense component distributions.
- **Fast Iteration**: Systems iterate by intersecting bitmasks at each level, skipping empty blocks entirely.
- **Cache Friendly**: Data is stored in fixed-size blocks, improving cache locality.

### 🛠️ Automatic Parallel Scheduler
Scheduling is dependency-aware, so you get parallelism “for free” once systems declare how they relate to each other.
- **Parent / After / Before**: Pipeline groups and system-level `After`, `Before`, and `Parent` annotations form a DAG that constrains global order.
- **Read/Write Sets**: Each system declares which component types it reads and writes; incompatible writers are automatically separated while disjoint systems share a wavefront.
- **Wavefront Execution**: The scheduler computes deterministic wavefronts (layers) at build time and then runs each wavefront in parallel using the backing thread pool.
- **Sequential Escape Hatch**: `World::run_sequential()` reuses the same ordering but executes wavefronts one system at a time for debugging or non-`Send` code.

### 🛠️ Ergonomic Macros
Define systems easily with the `system!` macro.
- **Declarative Queries**: `All=[Position, Velocity]`, `Remove=[Bullet]`, `Changed=[Health]` `None=[Destroyed]`.
- **Auto-generated Boilerplate**: Generates the `System` struct, `run` method, and storage access code.

## Usage Example

```rust
use rollback_ecs::prelude::*;

// 1. Define Components
#[derive(Component, Default, Clone, Debug, PartialEq)]
struct Position { x: f32, y: f32 }

#[derive(Component, Default, Clone, Debug, PartialEq)]
struct Velocity { x: f32, y: f32 }

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
    let mut storage = world.get_storage::<Position>();

    storage.borrow_mut().set_tick(Tick::new(1));

    // Run systems...
    world.run::<MovementSystem>();

    // 5. Rollback
    // Revert state to Tick 0 (before movement)
    storage.borrow_mut().rollback(Tick::new(0));
}
```

## Architecture

The storage uses a 3-level hierarchy:
1.  **Root Block**: Covers the entire entity space. Contains pointers to Middle blocks.
2.  **Middle Block**: Covers 16,384 entities. Contains pointers to Inner blocks.
3.  **Inner Block**: Covers 128 entities. Stores the actual component data and bitmasks (`presence`, `absence`, `changed`).

Rollback snapshots mirror this structure using `RollbackBlock`, storing only the modified data for each tick.

## Development

- **Build**: `cargo build`
- **Test**: `cargo test`
- **Coverage**: `cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info`

## Code Coverage

Code coverage is automatically tracked via [Codecov](https://codecov.io/gh/dungeon2567/rollback_ecs) for all commits and pull requests. The coverage report is generated using `cargo-llvm-cov` and uploaded to Codecov as part of the CI/CD pipeline.

## License

MIT
