use criterion::{Criterion, criterion_group, criterion_main};
use rollback_ecs::component::Component;
use rollback_ecs::entity::Entity;
use rollback_ecs::system::system;
use rollback_ecs::world::World;

#[derive(Component, Default, Clone)]
struct ComponentA {
    value: i32,
}

#[derive(Component, Default, Clone)]
struct ComponentB {
    #[allow(dead_code)]
    value: i32,
}

#[derive(Component, Default, Clone)]
struct ComponentC {
    #[allow(dead_code)]
    value: i32,
}

static mut QUERY_NONE_VAL: i32 = 0;
static mut QUERY_NONE_COUNT: usize = 0;

system! {
    QueryNoneSystem {
        query! {
            fn query_none(a: View<ComponentA>) None=[ComponentB, ComponentC] {
                // Access the component to simulate work
                unsafe {
                    QUERY_NONE_VAL += a.value;
                    QUERY_NONE_COUNT += 1;
                }
            }
        }
    }
}

fn benchmark_none_query(c: &mut Criterion) {
    // --- Setup done ONCE (not benchmarked) ---
    let mut world = {
        let mut world = World::new();

        // Prewarm: Create 10k entities
        let entities: Vec<Entity> = {
            let ents = world.get_storage::<Entity>();
            let ents_mut = unsafe { &mut *ents.get() };
            (0..10000).map(|_| ents_mut.spawn()).collect()
        };

        // Add ComponentA to all entities (10k entities)
        {
            let comp_a = world.get_storage::<ComponentA>();
            let a = unsafe { &mut *comp_a.get() };
            for i in 0..10000 {
                a.set(entities[i].index(), &ComponentA { value: 10 });
            }
        }

        // Add ComponentB to first 5000 entities (50% have B)
        {
            let comp_b = world.get_storage::<ComponentB>();
            let b = unsafe { &mut *comp_b.get() };
            for i in 0..5000 {
                b.set(entities[i].index(), &ComponentB { value: 20 });
            }
        }

        // Add ComponentC to entities 2000..7000 (50% have C, overlapping with B)
        {
            let comp_c = world.get_storage::<ComponentC>();
            let c = unsafe { &mut *comp_c.get() };
            for i in 2000..7000 {
                c.set(entities[i].index(), &ComponentC { value: 30 });
            }
        }

        world
    };

    // Prewarm query
    unsafe {
        QUERY_NONE_COUNT = 0;
    }
    for _ in 0..1000 {
        world.run_system::<QueryNoneSystem>();
    }
    unsafe {
        QUERY_NONE_COUNT = 0;
    }

    // --- Benchmark only the system run() call ---
    // Expected: ComponentB on 0..5000, ComponentC on 2000..7000
    // Entities without B: 5000..9999 (5000 entities)
    // Entities without C: 0..1999 and 7000..9999
    // Intersection (without B AND without C): 7000..9999 = 3000 entities
    c.bench_function("none_query_3000_entities", |b| {
        b.iter(|| {
            unsafe {
                QUERY_NONE_COUNT = 0;
            }
            world.run_system::<QueryNoneSystem>();
        })
    });

    // Print the count after benchmarking
    unsafe {
        let count = QUERY_NONE_COUNT;
        println!("DEBUG: none_query_3000_entities matched {} entities", count);
    }
}

fn benchmark_none_query_sparse(c: &mut Criterion) {
    // --- Setup: Sparse distribution (few entities have excluded components) ---
    let mut world = {
        let mut world = World::new();

        // Create 10k entities
        let entities: Vec<Entity> = {
            let ents = world.get_storage::<Entity>();
            let ents_mut = unsafe { &mut *ents.get() };
            (0..10000).map(|_| ents_mut.spawn()).collect()
        };

        // Add ComponentA to all entities
        {
            let comp_a = world.get_storage::<ComponentA>();
            let a = unsafe { &mut *comp_a.get() };
            for i in 0..10000 {
                a.set(entities[i].index(), &ComponentA { value: 10 });
            }
        }

        // Add ComponentB to only 100 entities (1% have B) - sparse
        {
            let comp_b = world.get_storage::<ComponentB>();
            let b = unsafe { &mut *comp_b.get() };
            for i in 0..100 {
                b.set(entities[i].index(), &ComponentB { value: 20 });
            }
        }

        // Add ComponentC to only 100 entities (1% have C) - sparse
        {
            let comp_c = world.get_storage::<ComponentC>();
            let c = unsafe { &mut *comp_c.get() };
            for i in 5000..5100 {
                c.set(entities[i].index(), &ComponentC { value: 30 });
            }
        }

        world
    };

    // Prewarm query
    unsafe {
        QUERY_NONE_COUNT = 0;
    }
    for _ in 0..1000 {
        world.run_system::<QueryNoneSystem>();
    }
    unsafe {
        QUERY_NONE_COUNT = 0;
    }

    // --- Benchmark sparse None query ---
    // Expected: ComponentB on 0..100, ComponentC on 5000..5100
    // Entities without B: 100..9999 (9900 entities)
    // Entities without C: 0..4999 and 5100..9999
    // Intersection (without B AND without C): 100..4999 (4900) + 5100..9999 (4900) = 9800 entities
    c.bench_function("none_query_sparse_9800_entities", |b| {
        b.iter(|| {
            unsafe {
                QUERY_NONE_COUNT = 0;
            }
            world.run_system::<QueryNoneSystem>();
        })
    });

    // Print the count after benchmarking
    unsafe {
        let count = QUERY_NONE_COUNT;
        println!(
            "DEBUG: none_query_sparse_9800_entities matched {} entities",
            count
        );
    }
}

fn benchmark_none_query_dense(c: &mut Criterion) {
    // --- Setup: Dense distribution (most entities have excluded components) ---
    let mut world = {
        let mut world = World::new();

        // Create 10k entities
        let entities: Vec<Entity> = {
            let ents = world.get_storage::<Entity>();
            let ents_mut = unsafe { &mut *ents.get() };
            (0..10000).map(|_| ents_mut.spawn()).collect()
        };

        // Add ComponentA to all entities
        {
            let comp_a = world.get_storage::<ComponentA>();
            let a = unsafe { &mut *comp_a.get() };
            for i in 0..10000 {
                a.set(entities[i].index(), &ComponentA { value: 10 });
            }
        }

        // Add ComponentB to 9500 entities (95% have B) - dense
        // Entities 0-9499 have B, so entities 9500-9999 DON'T have B (500 entities)
        {
            let comp_b = world.get_storage::<ComponentB>();
            let b = unsafe { &mut *comp_b.get() };
            for i in 0..9500 {
                b.set(entities[i].index(), &ComponentB { value: 20 });
            }
        }

        // Add ComponentC to 9500 entities (95% have C) - dense
        // Entities 500-9999 have C, so entities 0-499 DON'T have C (500 entities)
        // To get overlap: entities 9500-9999 don't have B, and we want some to also not have C
        // So we need entities 9500-9999 to not have C too
        // Let's make entities 0-9499 have C, so entities 9500-9999 don't have C
        {
            let comp_c = world.get_storage::<ComponentC>();
            let c = unsafe { &mut *comp_c.get() };
            for i in 0..9500 {
                c.set(entities[i].index(), &ComponentC { value: 30 });
            }
        }

        world
    };

    // Prewarm query
    unsafe {
        QUERY_NONE_COUNT = 0;
    }
    for _ in 0..1000 {
        world.run_system::<QueryNoneSystem>();
    }
    unsafe {
        QUERY_NONE_COUNT = 0;
    }

    // --- Benchmark dense None query (only ~500 entities match) ---
    c.bench_function("none_query_dense_500_entities", |b| {
        b.iter(|| {
            unsafe {
                QUERY_NONE_COUNT = 0;
            }
            world.run_system::<QueryNoneSystem>();
        })
    });

    // Print the count after benchmarking
    unsafe {
        let count = QUERY_NONE_COUNT;
        println!("DEBUG: none_query_dense matched {} entities", count);
    }
}

criterion_group!(
    benches,
    benchmark_none_query,
    benchmark_none_query_sparse,
    benchmark_none_query_dense
);
criterion_main!(benches);
