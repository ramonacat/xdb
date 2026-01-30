#[cfg(test)]
mod tests {
    use shuttle::{sync::Arc, thread};
    use xdb::{
        bplustree::{
            Tree,
            algorithms::{find, insert::insert},
            debug::assert_properties,
        },
        storage::in_memory::InMemoryStorage,
    };

    #[test]
    fn parallel_read_and_write() {
        shuttle::check_random(
            || {
                let storage = InMemoryStorage::new();
                let tree = Arc::new(Tree::<_, u64>::new(storage).unwrap());

                let t1 = {
                    let tree = tree.clone();
                    thread::spawn(move || {
                        let mut transaction = tree.transaction().unwrap();

                        find(&mut transaction, 1).unwrap();
                        transaction.commit().unwrap();
                    })
                };

                let t2 = {
                    let tree = tree.clone();
                    thread::spawn(move || {
                        let mut transaction = tree.transaction().unwrap();

                        insert(&mut transaction, 1, &vec![123]).unwrap();
                        transaction.commit().unwrap();
                    })
                };

                t1.join().unwrap();
                t2.join().unwrap();

                assert_properties(&mut tree.transaction().unwrap());
                assert_eq!(
                    &tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>(),
                    &vec![(1, vec![123])]
                );
            },
            100,
        );
    }
}
