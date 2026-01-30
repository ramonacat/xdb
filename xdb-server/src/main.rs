use log::{error, info};
use std::{
    io::{self, Read},
    net::TcpListener,
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};

use bytemuck::{AnyBitPattern, checked::from_bytes};
use xdb::{
    bplustree::{Tree, algorithms::insert::insert},
    storage::in_memory::InMemoryStorage,
};

fn read_struct<T: AnyBitPattern>(reader: &mut dyn Read) -> Result<T, std::io::Error> {
    let mut buffer = vec![0u8; size_of::<T>()];
    reader.read_exact(&mut buffer)?;

    Ok(*from_bytes(&buffer))
}

// TODO real error handling
// TODO let the client manage the transaction
// TODO make the protocol handling less chaotic
// TODO cleanup all the hacky handling of client threads, nonblocking accept and hacky exit
// condition
fn main() {
    env_logger::init();

    let storage = InMemoryStorage::new();
    // TODO can we avoid using the Arc???
    let tree = Arc::new(Tree::<_, u64>::new(storage).unwrap());

    let mut threads: Vec<JoinHandle<()>> = vec![];

    let socket = TcpListener::bind("0.0.0.0:9969").unwrap();
    socket.set_nonblocking(true).unwrap();
    let mut waitloops = 0;
    loop {
        let (mut client, client_addr) = match socket.accept() {
            Ok(next) => next,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                waitloops += 1;
                if !threads.is_empty() && waitloops >= 10 {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(e) => {
                error!("{e}");
                break;
            }
        };

        let tree = tree.clone();
        let handle = thread::spawn(move || {
            info!("New client connected @{client_addr:?}");

            loop {
                let command_count = read_struct::<u16>(&mut client).unwrap();

                let mut commands = vec![];
                // TODO support various command types (e.g. delete)
                for _ in 0..command_count {
                    let key = read_struct::<u64>(&mut client).unwrap();
                    let value_size = read_struct::<u64>(&mut client).unwrap();

                    assert!(value_size <= 512);

                    let mut value = vec![0; value_size as usize];
                    client.read_exact(&mut value).unwrap();

                    commands.push((key, value));
                }

                let mut retry_count = 0;
                // TODO clean up the error handling
                'retries: loop {
                    let mut transaction = tree.transaction().unwrap();

                    for command in &commands {
                        match insert(&mut transaction, command.0, &command.1) {
                            Ok(_) => {}
                            Err(error) => match error {
                                xdb::bplustree::TreeError::StorageError(ref storage_error) => {
                                    match storage_error {
                                        xdb::storage::StorageError::PageNotFound(_) => {
                                            panic!("{error:?}");
                                        }
                                        xdb::storage::StorageError::Deadlock(_) => {
                                            transaction.rollback().unwrap();

                                            if retry_count == 10 {
                                                panic!("{error:?}");
                                            }

                                            crate::thread::yield_now();
                                            retry_count += 1;

                                            continue 'retries;
                                        }
                                    }
                                }
                            },
                        }
                    }

                    transaction.commit().unwrap();
                    break 'retries;
                }
            }
        });
        threads.push(handle);
    }

    for thread in threads {
        let result = thread.join();
        if let Err(err) = result {
            error!("thread panicked: {err:?}");
        }
    }
}
