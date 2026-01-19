use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use xdb::{
    bplustree::{Tree, algorithms::insert},
    debug::BigKey,
    storage::in_memory::InMemoryStorage,
};

fn sorted_insert(c: &mut Criterion) {
    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();
    let transaction = tree.transaction().unwrap();

    c.bench_function("sorted_insert (8 byte value)", |b| {
        b.iter(|| {
            for i in 0..5000 {
                insert::insert(&transaction, BigKey::<u64>::new(i), &i.to_ne_bytes()).unwrap();
            }
        })
    });

    black_box(tree);

    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();
    let transaction = tree.transaction().unwrap();

    c.bench_function("sorted_insert (512 byte value)", |b| {
        b.iter(|| {
            for i in 0..5000 {
                insert::insert(&transaction, BigKey::<u64>::new(i), &vec![0xff; 512]).unwrap();
            }
        })
    });

    black_box(tree);
}

criterion_group!(benches, sorted_insert);
criterion_main!(benches);
