use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use xdb::bplustree::Tree;
use xdb::bplustree::algorithms::delete::delete;
use xdb::bplustree::algorithms::insert::insert;
use xdb::debug::BigKey;
use xdb::storage::in_memory::InMemoryStorage;

fn reverse_delete(c: &mut Criterion) {
    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();
    let mut transaction = tree.transaction().unwrap();
    for i in 0..50000 {
        insert(
            &mut transaction,
            BigKey::<u64, 256>::new(i),
            &i.to_ne_bytes(),
        )
        .unwrap();
    }

    c.bench_function("reverse delete", |b| {
        b.iter(|| {
            for i in 50000..0 {
                delete(&mut transaction, BigKey::new(i)).unwrap();
            }
        })
    });

    black_box(tree.iter().unwrap().collect::<Vec<_>>());
}

criterion_group!(benches, reverse_delete);
criterion_main!(benches);
