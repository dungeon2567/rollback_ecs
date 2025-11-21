use criterion::{criterion_group, criterion_main, Criterion};
use rollback_ecs::world::World;
use rollback_ecs::entity::Entity;
use rollback_ecs::storage::BitsetStorage;

use rollback_ecs::system::DestroySystem;

fn benchmark_world_spawn_destroy_1000(c: &mut Criterion) {
    let mut world = World::new();

    // Prewarm
    {
        let ents = world.get::<Entity>();
        let mut entities = Vec::with_capacity(1000);
        
        for _ in 0..1000 {
            entities.push(*ents.borrow_mut().spawn());
        }

        for e in entities {
            world.destroy(e);
        }

        world.run::<DestroySystem>();
    }

    c.bench_function("world_spawn_destroy_1000", |b| {
        b.iter(|| {
            let ents = world.get::<Entity>();
            let mut entities = Vec::with_capacity(1000);
            
            // Spawn 1000 entities
            for _ in 0..1000 {
                entities.push(*ents.borrow_mut().spawn());
            }

            // Destroy each
            for e in entities {
                world.destroy(e);
            }

            // Run destroy system
            world.run::<DestroySystem>();
        })
    });
}

fn benchmark_storage_spawn(c: &mut Criterion) {
    c.bench_function("storage_spawn", |b| {
        b.iter(|| {
            let mut storage = BitsetStorage::<Entity>::new();
            // Spawn 1000 entities
            for _ in 0..1000 {
                storage.spawn();
            }
        })
    });
}

criterion_group!(benches, benchmark_world_spawn_destroy_1000, benchmark_storage_spawn);
criterion_main!(benches);
