use log::info;
use std::{io::Read, net::TcpListener, sync::Arc, thread};

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
fn main() {
    env_logger::init();

    let storage = InMemoryStorage::new();
    // TODO can we avoid using the Arc???
    let tree = Arc::new(Tree::<_, u64>::new(storage).unwrap());

    let socket = TcpListener::bind("0.0.0.0:9969").unwrap();
    while let Ok((mut client, client_addr)) = socket.accept() {
        let tree = tree.clone();
        thread::spawn(move || {
            info!("New client connected @{client_addr:?}");

            loop {
                let command_count = read_struct::<u16>(&mut client).unwrap();
                let mut transaction = tree.transaction().unwrap();

                // TODO support various command types (e.g. delete)
                for _ in 0..command_count {
                    let key = read_struct::<u64>(&mut client).unwrap();
                    let value_size = read_struct::<u64>(&mut client).unwrap();

                    assert!(value_size <= 512);

                    let mut value = vec![0; value_size as usize];
                    client.read_exact(&mut value).unwrap();

                    insert(&mut transaction, key, &value).unwrap();
                }

                transaction.commit().unwrap();
            }
        });
    }
}
