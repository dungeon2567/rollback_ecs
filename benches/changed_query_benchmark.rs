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
    value: i32,
}

static mut QUERY_CHANGED_VAL: i32 = 0;
static mut QUERY_CHANGED_COUNT: usize = 0;

system! {
    QueryChangedSystem {
        query! {
            fn query_changed(a: View<ComponentA>, b: View<ComponentB>) Changed=[ComponentA, ComponentB] {
                // Access the components to simulate work
                unsafe {
                    QUERY_CHANGED_VAL += a.value + b.value;
                    QUERY_CHANGED_COUNT += 1;
                }
            }
        }
    }
}

fn benchmark_changed_query(c: &mut Criterion) {
    // --- Setup done ONCE (not benchmarked) ---
    let mut world = {
        let mut world = World::new();

        // Prewarm: Create 10k entities
        let entities: Vec<Entity> = {
            let ents = world.get_storage::<Entity>();
            let ents_mut = unsafe { &mut *ents.get() };
            (0..10000).map(|_| ents_mut.spawn()).collect()
        };

        // Add ComponentA on 1000..1100
        {
            let comp_a = world.get_storage::<ComponentA>();
            let a = unsafe { &mut *comp_a.get() };
            for i in 0..10000 {
                a.set(entities[i].index(), &ComponentA { value: 10 });
            }
        }

        // Add ComponentB on 1050..1150
        {
            let comp_b = world.get_storage::<ComponentB>();
            let b = unsafe { &mut *comp_b.get() };
            for i in 0..10000 {
                b.set(entities[i].index(), &ComponentB { value: 20 });
            }
        }

        // Clear changes after initial setup
        unsafe {
            (*world.get_storage::<ComponentA>().get()).clear_changes();
            (*world.get_storage::<ComponentB>().get()).clear_changes();
        }

        // Add ComponentA on 1000..1100
        {
            let comp_a = world.get_storage::<ComponentA>();
            let a = unsafe { &mut *comp_a.get() };
            for i in 1000..1100 {
                a.set(entities[i].index(), &ComponentA { value: 20 });
            }
        }

        // Add ComponentB on 1050..1150
        {
            let comp_b = world.get_storage::<ComponentB>();
            let b = unsafe { &mut *comp_b.get() };
            for i in 1050..1150 {
                b.set(entities[i].index(), &ComponentB { value: 30 });
            }
        }

        world
    };

    // Prewarm query
    unsafe {
        QUERY_CHANGED_COUNT = 0;
    }
    for _ in 0..1000 {
        world.run_system::<QueryChangedSystem>();
    }
    unsafe {
        QUERY_CHANGED_COUNT = 0;
    }

    // --- Benchmark only the system run() call ---
    c.bench_function("changed_query_overlapping", |b| {
        b.iter(|| {
            unsafe {
                QUERY_CHANGED_COUNT = 0;
            }
            world.run_system::<QueryChangedSystem>();
        })
    });

    // Print the count after benchmarking
    unsafe {
        let count = QUERY_CHANGED_COUNT;
        println!(
            "DEBUG: changed_query_overlapping matched {} entities",
            count
        );
    }
}

criterion_group!(benches, benchmark_changed_query);
criterion_main!(benches);
