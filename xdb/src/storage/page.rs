use std::fmt::Debug;

use bytemuck::{AnyBitPattern, NoUninit, Pod, Zeroable, bytes_of, from_bytes_mut, must_cast};
use thiserror::Error;

use crate::Size;
use crate::checksum::Checksum;

pub const PAGE_SIZE: Size = Size::B(4096);
pub const PAGE_DATA_SIZE: Size = PAGE_SIZE.subtract(Size::of::<PageHeader>());

#[derive(Debug, Error)]
pub enum PageError {
    #[error("Incorrect checksum")]
    Checksum,
}

bitflags::bitflags! {
    #[derive(Debug, Pod, Zeroable, Clone, Copy)]
    #[repr(transparent)]
    pub struct PageFlags: u16 {
        const IS_FREE = 1 << 0;
    }
}

#[derive(Debug, Pod, Clone, Copy, Zeroable)]
#[repr(C, align(8))]
// TODO this header is growing in size, are all of these fields really neccessary?
// TODO the versioning and visibility stuff should be moved into another datastructure, the
// physical page doesn't really care about anything other than a checksum and raw data
pub struct PageHeader {
    checksum: Checksum,
    flags: PageFlags,
    _unused1: u16,
}

impl PageHeader {
    pub fn new() -> Self {
        Self {
            checksum: Checksum::zeroed(),
            flags: PageFlags::empty(),
            _unused1: 0,
        }
    }
}

const _: () = assert!(size_of::<PageHeader>() == size_of::<u64>());

#[derive(Pod, Clone, Copy, Zeroable)]
#[repr(C, align(8))]
// TODO page should be visible at pub(crate) probably
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_DATA_SIZE.as_bytes()],
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}

const _: () = assert!(Size::of::<Page>().is_equal(PAGE_SIZE));

impl Page {
    pub(crate) fn new() -> Self {
        Self {
            header: PageHeader::new(),
            data: [0; _],
        }
    }

    #[allow(unused)] // TODO we don't need the whole method, but we need a checksum check on read,
    // and an update on write
    pub fn serialize(mut self) -> [u8; PAGE_SIZE.as_bytes()] {
        self.header.checksum.clear();

        let mut bytes: [u8; PAGE_SIZE.as_bytes()] = must_cast(self);
        let checksum = Checksum::of(&bytes);

        for (i, byte) in bytes_of(&checksum).iter().enumerate() {
            bytes[i] = *byte;
        }

        bytes
    }

    #[allow(unused)] // TODO we don't need most of this, but we need to do a checksum update on
    // write
    pub fn deserialize(mut bytes: [u8; PAGE_SIZE.as_bytes()]) -> Result<Self, PageError> {
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

    pub fn data_mut<T: AnyBitPattern + NoUninit>(&mut self) -> &mut T {
        from_bytes_mut(&mut self.data)
    }

    /// This is used to mark the page as free and allow for it to be reallocated, regardless of
    /// `valid_from`/`valid_until`. It can be used e.g. to free cow copies after a transaction rolled
    /// back (as then we know that the validity timestamps are irrelevant, as there will not be any
    /// references to these pages).
    pub fn mark_free(&mut self) {
        self.header.flags.set(PageFlags::IS_FREE, true);
    }

    pub const fn is_free(&self) -> bool {
        self.header.flags.contains(PageFlags::IS_FREE)
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
        let mut bytes = [0; PAGE_SIZE.as_bytes()];
        bytes[0] = 1;
        bytes[1] = 2;
        bytes[2] = 3;
        bytes[3] = 4;

        let page = Page::deserialize(bytes);

        assert!(matches!(page, Err(PageError::Checksum)))
    }

    #[test]
    pub fn deserializes_with_correct_checksum() {
        let mut bytes = [0; PAGE_SIZE.as_bytes()];
        bytes[0] = 137;
        bytes[1] = 65;
        bytes[2] = 249;
        bytes[3] = 152;

        let page = Page::deserialize(bytes);

        assert_eq!(
            &[0; _],
            page.unwrap().data_mut::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
        );
    }
}
