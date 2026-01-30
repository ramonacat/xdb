use std::{io::Write, net::TcpStream, sync::Barrier, thread};

use rand::{Rng, rng};

const THREAD_COUNT: usize = 4;
const LOOP_COUNT: usize = 100;

fn main() {
    let barrier = Barrier::new(THREAD_COUNT);

    thread::scope(|s| {
        for _ in 0..THREAD_COUNT {
            s.spawn(|| {
                let mut rng = rng();
                let mut socket = TcpStream::connect("127.0.0.1:9969").unwrap();

                barrier.wait();

                for _ in 0..LOOP_COUNT {
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
            });
        }
    });
}
