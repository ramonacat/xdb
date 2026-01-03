use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

use bytemuck::{Pod, Zeroable};
use crc32c::crc32c;

use crate::page::{PAGE_DATA_SIZE, PAGE_SIZE, Page};

mod page;

#[repr(transparent)]
#[derive(Pod, Clone, Copy, Zeroable, Debug, PartialEq, Eq)]
struct Checksum(u32);

const CHECKSUM_BYTES: usize = size_of::<Checksum>();

impl Checksum {
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }
    pub fn of(bytes: &[u8]) -> Self {
        Self(crc32c(bytes))
    }

    fn clear(&mut self) {
        self.0 = 0;
    }
}

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
