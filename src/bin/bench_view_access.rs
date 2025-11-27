use std::hint::black_box;
use std::time::Instant;

// Mock structures to simulate the depth
struct Inner {
    data: [u64; 128],
}

struct Middle {
    inners: Vec<Box<Inner>>,
}

struct Root {
    middles: Vec<Box<Middle>>,
}

struct Storage {
    root: Root,
}

impl Storage {
    fn new() -> Self {
        let inner = Inner { data: [1u64; 128] };
        let middle = Middle {
            inners: vec![Box::new(inner)],
        };
        let root = Root {
            middles: vec![Box::new(middle)],
        };
        Storage { root }
    }

    #[inline(always)]
    fn get_mut(&mut self, r: usize, m: usize, i: usize) -> &mut u64 {
        &mut self.root.middles[r].inners[m].data[i]
    }
}

fn main() {
    let mut storage = Storage::new();
    let iterations = 100_000_000;

    println!("Benchmarking with {} iterations...", iterations);

    // --- Prewarm ---
    println!("Prewarming...");
    for _ in 0..1_000_000 {
        let val = storage.get_mut(0, 0, 0);
        *val += 1;
        black_box(*val);
    }

    // --- Benchmark Direct Access ---
    let start = Instant::now();
    {
        // Simulate getting the block reference once (like ViewMut creation)
        let data_ref = &mut storage.root.middles[0].inners[0].data[0];
        for _ in 0..iterations {
            *data_ref += 1;
            black_box(*data_ref);
        }
    }
    let duration_direct = start.elapsed();
    println!("Direct Access: {:?}", duration_direct);

    // --- Benchmark Storage Traversal ---
    let start = Instant::now();
    {
        for _ in 0..iterations {
            // Simulate looking up the location every time
            let val = storage.get_mut(0, 0, 0);
            *val += 1;
            black_box(*val);
        }
    }
    let duration_storage = start.elapsed();
    println!("Storage Traversal: {:?}", duration_storage);

    let ratio = duration_storage.as_secs_f64() / duration_direct.as_secs_f64();
    println!("Ratio (Storage / Direct): {:.2}x slower", ratio);
}
