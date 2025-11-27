use criterion::{Criterion, criterion_group, criterion_main};
use rollback_ecs::entity::Entity;
use rollback_ecs::storage::Storage;
use rollback_ecs::world::World;

use rollback_ecs::system::DestroySystem;

fn benchmark_world_spawn_destroy_1000(c: &mut Criterion) {
    let mut world = World::new();

    // Prewarm
    {
        let ents = world.get_storage::<Entity>();
        let mut entities = Vec::with_capacity(1000);

        for _ in 0..1000 {
            entities.push(unsafe { (*ents.get()).spawn() });
        }

        for e in entities {
            world.destroy(e);
        }

        world.run_system::<DestroySystem>();
    }

    c.bench_function("world_spawn_destroy_1000", |b| {
        b.iter(|| {
            let ents = world.get_storage::<Entity>();
            let mut entities = Vec::with_capacity(1000);

            // Spawn 1000 entities
            for _ in 0..1000 {
                entities.push(unsafe { (*ents.get()).spawn() });
            }

            // Destroy each
            for e in entities {
                world.destroy(e);
            }

            // Run destroy system
            world.run_system::<DestroySystem>();
        })
    });
}

fn benchmark_storage_spawn(c: &mut Criterion) {
    c.bench_function("storage_spawn", |b| {
        b.iter(|| {
            let mut storage = Storage::<Entity>::new();
            // Spawn 1000 entities
            for _ in 0..1000 {
                storage.spawn();
            }
        })
    });
}

criterion_group!(
    benches,
    benchmark_world_spawn_destroy_1000,
    benchmark_storage_spawn
);
criterion_main!(benches);
