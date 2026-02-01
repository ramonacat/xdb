#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use shuttle::{
        Config, PortfolioRunner,
        scheduler::{DfsScheduler, PctScheduler},
        sync::Arc,
        thread,
    };
    use xdb::{
        bplustree::{
            Tree, TreeError, TreeKey,
            algorithms::{find, insert::insert},
            debug::assert_properties,
        },
        debug::BigKey,
        storage::{StorageError, in_memory::InMemoryStorage},
    };

    struct Test {
        closure: Box<dyn Fn() -> Box<dyn Fn() + Send + Sync>>,
    }

    impl Test {
        fn new<
            TKey: TreeKey,
            TThread1: Fn() -> Result<(), TreeError> + Send + 'static,
            TThread2: Fn() -> Result<(), TreeError> + Send + 'static,
        >(
            thread1: impl Fn(Arc<Tree<InMemoryStorage, TKey>>) -> TThread1
            + Sync
            + Send
            + Clone
            + 'static,
            thread2: impl Fn(Arc<Tree<InMemoryStorage, TKey>>) -> TThread2
            + Sync
            + Send
            + Clone
            + 'static,
            verify: impl Fn(Arc<Tree<InMemoryStorage, TKey>>) + Send + Sync + Clone + 'static,
        ) -> Self {
            Self {
                closure: Box::new(move || {
                    let thread1 = thread1.clone();
                    let thread2 = thread2.clone();
                    let verify = verify.clone();
                    Box::new(move || {
                        let storage = InMemoryStorage::new();
                        let tree = Arc::new(Tree::<_, TKey>::new(storage).unwrap());

                        let t1 = {
                            let tree = tree.clone();

                            thread::spawn((thread1)(tree))
                        };

                        let t2 = {
                            let tree = tree.clone();
                            thread::spawn((thread2)(tree))
                        };

                        if matches!(
                            t1.join().unwrap(),
                            Err(TreeError::StorageError(StorageError::Deadlock(_)))
                        ) {
                            return;
                        }
                        if matches!(
                            t2.join().unwrap(),
                            Err(TreeError::StorageError(StorageError::Deadlock(_)))
                        ) {
                            return;
                        }

                        verify(tree);
                    })
                }),
            }
        }

        fn run(&self) {
            let mut config = Config::new();
            config.max_steps = shuttle::MaxSteps::ContinueAfter(1_000_000);
            let mut runner = PortfolioRunner::new(true, config);

            runner.add(PctScheduler::new(1_000, 10_000));
            runner.add(DfsScheduler::new(Some(10_000), false));

            runner.run((self.closure)());
        }

        #[allow(unused)]
        fn replay(&self, schedule: &str) {
            shuttle::annotate_replay((self.closure)(), schedule);
        }
    }

    #[test]
    fn parallel_read_and_write() {
        let test = Test::new(
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;

                    let result = find(&mut transaction, 1);

                    transaction.commit()?;

                    result?;

                    Ok(())
                }
            },
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;

                    let result = insert(&mut transaction, 1, &vec![123]);

                    transaction.commit()?;

                    result?;

                    Ok(())
                }
            },
            |tree| {
                assert_properties(&mut tree.transaction().unwrap());
                assert_eq!(
                    &tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>(),
                    &vec![(1, vec![123])]
                );
            },
        );
        test.run();
    }

    #[test]
    fn big_writes_no_overlap() {
        let test = Test::new(
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;

                    for i in 5..10u64 {
                        insert(
                            &mut transaction,
                            BigKey::<u64, 1024>::new(i),
                            &i.to_ne_bytes(),
                        )?;
                    }

                    transaction.commit()?;

                    Ok(())
                }
            },
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;

                    for i in 0..5 {
                        insert(&mut transaction, BigKey::new(i), &i.to_ne_bytes())?;
                    }

                    transaction.commit()?;

                    Ok(())
                }
            },
            |tree| {
                assert_properties(&mut tree.transaction().unwrap());
                assert_eq!(
                    &tree
                        .iter()
                        .unwrap()
                        .map(|x| x.unwrap())
                        .map(|(k, v)| (k.value(), v))
                        .collect::<Vec<_>>(),
                    &(0..10u64)
                        .map(|x| (x, x.to_ne_bytes().into_iter().collect::<Vec<_>>()))
                        .collect::<Vec<_>>()
                );
            },
        );
        test.run();
    }

    #[test]
    fn big_writes() {
        let test = Test::new(
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;

                    for i in (0..10).filter(|x| x % 2 == 0) {
                        insert(
                            &mut transaction,
                            BigKey::<u64, 1024>::new(i),
                            &i.to_ne_bytes(),
                        )?;
                    }

                    transaction.commit()?;

                    Ok(())
                }
            },
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;

                    for i in (0..10).filter(|x| x % 2 != 0) {
                        insert(&mut transaction, BigKey::new(i), &i.to_ne_bytes())?;
                    }

                    transaction.commit()?;

                    Ok(())
                }
            },
            |tree| {
                assert_properties(&mut tree.transaction().unwrap());
                assert_eq!(
                    &tree
                        .iter()
                        .unwrap()
                        .map(|x| x.unwrap())
                        .map(|(k, v)| (k.value(), v))
                        .collect::<Vec<_>>(),
                    &(0..10u64)
                        .map(|x| (x, x.to_ne_bytes().into_iter().collect::<Vec<_>>()))
                        .collect::<Vec<_>>()
                );
            },
        );
        test.run();
    }

    #[test]
    fn big_writes_and_read() {
        let test = Test::new(
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;
                    for i in (0..10).filter(|x| x % 2 == 0) {
                        insert(
                            &mut transaction,
                            BigKey::<u64, 1024>::new(i),
                            &i.to_ne_bytes(),
                        )?;
                    }

                    for i in (0..10).filter(|x| x % 2 == 1) {
                        insert(
                            &mut transaction,
                            BigKey::<u64, 1024>::new(i),
                            &i.to_ne_bytes(),
                        )?;
                    }
                    transaction.commit()?;

                    Ok(())
                }
            },
            |tree| {
                move || {
                    let mut transaction = tree.transaction()?;
                    for i in 10..0 {
                        find(&mut transaction, BigKey::new(i))?;
                    }
                    transaction.commit()?;

                    Ok(())
                }
            },
            |tree| {
                assert_properties(&mut tree.transaction().unwrap());
                assert_eq!(
                    &tree
                        .iter()
                        .unwrap()
                        .map(|x| x.unwrap())
                        .map(|(k, v)| (k.value(), v))
                        .collect::<Vec<_>>(),
                    &(0..10u64)
                        .map(|x| (x, x.to_ne_bytes().into_iter().collect::<Vec<_>>()))
                        .collect::<Vec<_>>()
                );
            },
        );
        test.run();
    }
}
