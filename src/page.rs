use bytemuck::{Pod, Zeroable, bytes_of, must_cast};
use thiserror::Error;

use crate::checksum::Checksum;

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_DATA_SIZE: usize = PAGE_SIZE - size_of::<PageHeader>();

#[derive(Debug, Error)]
pub enum PageError {
    #[error("Incorrect checksum")]
    Checksum,
}

#[repr(C)]
#[derive(Pod, Clone, Copy, Zeroable)]
struct PageHeader {
    checksum: Checksum,
}

#[repr(C)]
#[derive(Pod, Clone, Copy, Zeroable)]
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_DATA_SIZE],
}

const _: () = assert!(size_of::<Page>() == PAGE_SIZE);

impl Page {
    pub fn new() -> Self {
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

    pub fn deserialize(mut bytes: [u8; PAGE_SIZE]) -> Result<Self, PageError> {
        let expected_checksum =
            Checksum::from_bytes(bytes[0..size_of::<Checksum>()].try_into().unwrap());

        for byte in bytes.iter_mut().take(size_of::<Checksum>()) {
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

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
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
