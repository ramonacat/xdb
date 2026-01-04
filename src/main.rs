use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

use bytemuck::bytes_of;

use crate::{
    bplustree::Tree,
    page::{PAGE_SIZE, Page},
};

mod bplustree;
mod checksum;
mod page;
mod storage;

fn main() {
    let mut file = OpenOptions::new()
        .truncate(true)
        .write(true)
        .open("data.db")
        .unwrap();

    let mut tree = Tree::new(32, 32).unwrap();
    tree.insert(&[1; 32], &[2; 32]).unwrap();

    let mut page = Page::new();

    page.data_mut().copy_from_slice(bytes_of(&tree));

    file.write_all(&page.serialize()).unwrap();
    file.sync_data().unwrap();

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
