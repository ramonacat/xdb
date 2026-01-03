use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
};

use bytemuck::{Pod, Zeroable, bytes_of, must_cast};
use crc32c::crc32c;
use thiserror::Error;

const PAGE_SIZE: usize = 4096;
const PAGE_DATA_SIZE: usize = PAGE_SIZE - size_of::<PageHeader>();

#[derive(Debug, Error)]
enum PageError {
    #[error("Incorrect checksum")]
    Checksum,
}

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

#[repr(C)]
#[derive(Pod, Clone, Copy, Zeroable)]
struct PageHeader {
    checksum: Checksum,
}

#[repr(C)]
#[derive(Pod, Clone, Copy, Zeroable)]
struct Page {
    header: PageHeader,
    data: [u8; PAGE_DATA_SIZE],
}

const _: () = assert!(size_of::<Page>() == PAGE_SIZE);

impl Page {
    fn new() -> Self {
        Self {
            header: PageHeader::zeroed(),
            data: [0; _],
        }
    }

    pub fn serialize(mut self) -> [u8; PAGE_SIZE] {
        self.header.checksum.clear();

        let mut bytes: [u8; PAGE_SIZE] = must_cast(self);
        let checksum = Checksum::of(&bytes);

        for (i, byte) in bytes_of(&checksum).iter().enumerate() {
            bytes[i] = *byte;
        }

        bytes
    }

    fn deserialize(mut bytes: [u8; PAGE_SIZE]) -> Result<Self, PageError> {
        let expected_checksum = Checksum::from_bytes(bytes[0..CHECKSUM_BYTES].try_into().unwrap());

        for byte in bytes.iter_mut().take(CHECKSUM_BYTES) {
            *byte = 0;
        }

        if Checksum::of(&bytes) == expected_checksum {
            Ok(must_cast(bytes))
        } else {
            Err(PageError::Checksum)
        }
    }

    #[cfg(test)] // TODO remove?
    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn serialize_has_correct_crc32() {
        let page = Page::new();

        let serialized = page.serialize();

        assert_eq!(&serialized[0..4], &[137, 65, 249, 152]);
    }

    #[test]
    pub fn deserialize_errors_on_wrong_checksum() {
        let mut bytes = [0; PAGE_SIZE];
        bytes[0] = 1;
        bytes[1] = 2;
        bytes[2] = 3;
        bytes[3] = 4;

        let page = Page::deserialize(bytes);

        assert!(matches!(page, Err(PageError::Checksum)))
    }

    #[test]
    pub fn deserializes_with_correct_checksum() {
        let mut bytes = [0; PAGE_SIZE];
        bytes[0] = 137;
        bytes[1] = 65;
        bytes[2] = 249;
        bytes[3] = 152;

        let page = Page::deserialize(bytes);

        assert_eq!(&[0; PAGE_DATA_SIZE], page.unwrap().data());
    }
}
