use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

use crate::page::{PAGE_DATA_SIZE, PAGE_SIZE, Page};

mod checksum;
mod page;

fn main() {
    let mut file = OpenOptions::new()
        .truncate(true)
        .write(true)
        .open("data.db")
        .unwrap();

    for page_index in 0..256 {
        let mut page = Page::new();

        for i in 0..PAGE_DATA_SIZE {
            page.data_mut()[i] = u8::try_from((i ^ page_index) % usize::from(u8::MAX)).unwrap();
        }

        file.write_all(&page.serialize()).unwrap();
        file.sync_data().unwrap();
    }

    drop(file);

    let mut file = File::open("data.db").unwrap();

    let mut buf = [0u8; PAGE_SIZE];

    loop {
        match file.read_exact(&mut buf) {
            Ok(_) => {}
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                panic!("{err}")
            }
        };
        Page::deserialize(buf).unwrap();
    }
}
