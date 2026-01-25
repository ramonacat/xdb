use std::{io::Write, net::TcpStream};

use rand::{Rng, rng};

fn main() {
    let mut rng = rng();
    let mut socket = TcpStream::connect("127.0.0.1:9969").unwrap();

    loop {
        let command_count: u16 = rng.random_range(1..32u16);

        socket.write_all(&command_count.to_ne_bytes()).unwrap();
        for _ in 0..command_count {
            let key: u64 = rng.random();
            let value_size: usize = rng.random_range(1..512);

            let value = rng
                .clone()
                .random_iter()
                .take(value_size)
                .collect::<Vec<u8>>();

            socket.write_all(&key.to_ne_bytes()).unwrap();
            socket
                .write_all(&(value_size as u64).to_ne_bytes())
                .unwrap();
            socket.write_all(&value).unwrap();
        }
    }
}
