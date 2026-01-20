use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use xdb::{
    bplustree::{
        Tree,
        algorithms::{delete::delete, insert::insert},
    },
    debug::BigKey,
    storage::in_memory::InMemoryStorage,
};

fn reverse_delete(c: &mut Criterion) {
    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();
    let transaction = tree.transaction().unwrap();
    for i in 0..50000 {
        insert(&transaction, BigKey::<u64, 256>::new(i), &i.to_ne_bytes()).unwrap();
    }

    c.bench_function("reverse delete", |b| {
        b.iter(|| {
            for i in 50000..0 {
                delete(&transaction, BigKey::new(i)).unwrap();
            }
        })
    });

    black_box(tree.iter().unwrap().collect::<Vec<_>>());
}

criterion_group!(benches, reverse_delete);
criterion_main!(benches);
