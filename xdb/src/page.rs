use std::fmt::Debug;

use bytemuck::{
    AnyBitPattern, NoUninit, Pod, Zeroable, bytes_of, from_bytes, from_bytes_mut, must_cast,
};
use thiserror::Error;

use crate::{
    Size,
    checksum::Checksum,
    storage::{PageIndex, TransactionalTimestamp},
};

pub const PAGE_SIZE: Size = Size::B(4096);
pub const PAGE_DATA_SIZE: Size = PAGE_SIZE.subtract(Size::of::<PageHeader>());

#[derive(Debug, Error)]
pub enum PageError {
    #[error("Incorrect checksum")]
    Checksum,
}

#[derive(Debug, Pod, Clone, Copy, Zeroable)]
#[repr(C, align(8))]
// TODO this header is growing in size, are all of these fields really neccessary?
// TODO the versioning and visibility stuff should be moved into another datastructure, the
// physical page doesn't really care about anything other than a checksum and raw data
struct PageHeader {
    checksum: Checksum,
    _unused1: u32,
    visible_from: TransactionalTimestamp,
    visible_until: TransactionalTimestamp,
    next_version: PageIndex,
    previous_version: PageIndex,
}
impl PageHeader {
    fn new() -> Self {
        Self {
            checksum: Checksum::zeroed(),
            _unused1: 0,
            visible_from: TransactionalTimestamp::zeroed(),
            visible_until: TransactionalTimestamp::zeroed(),
            next_version: PageIndex::max(),
            previous_version: PageIndex::max(),
        }
    }
}

const _: () = assert!(size_of::<PageHeader>() == 5 * size_of::<u64>());

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
    pub fn from_data<T: AnyBitPattern + NoUninit>(data: T) -> Self {
        Self {
            header: PageHeader::new(),
            data: must_cast(data),
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

    pub fn data<T: AnyBitPattern>(&self) -> &T {
        from_bytes(&self.data)
    }

    pub fn data_mut<T: AnyBitPattern + NoUninit>(&mut self) -> &mut T {
        from_bytes_mut(&mut self.data)
    }

    pub fn is_visible_at(&self, timestamp: TransactionalTimestamp) -> bool {
        if self.header.visible_from > timestamp {
            return false;
        }

        if self.header.visible_until != TransactionalTimestamp::zero()
            && self.header.visible_until < timestamp
        {
            return false;
        }

        true
    }

    pub fn set_visible_from(&mut self, timestamp: Option<TransactionalTimestamp>) {
        self.header.visible_from = timestamp.unwrap_or_else(TransactionalTimestamp::zero);
    }

    pub fn set_visible_until(&mut self, timestamp: Option<TransactionalTimestamp>) {
        self.header.visible_until = timestamp.unwrap_or_else(TransactionalTimestamp::zero);
    }

    pub fn visible_until(&self) -> Option<TransactionalTimestamp> {
        if self.header.visible_until == TransactionalTimestamp::zero() {
            None
        } else {
            debug_assert!(self.header.visible_until >= self.header.visible_from);

            Some(self.header.visible_until)
        }
    }

    pub fn visible_from(&self) -> Option<TransactionalTimestamp> {
        if self.header.visible_from == TransactionalTimestamp::zero() {
            None
        } else {
            debug_assert!(
                self.header.visible_until == TransactionalTimestamp::zero()
                    || self.header.visible_until >= self.header.visible_from
            );

            Some(self.header.visible_from)
        }
    }

    pub fn next_version(&self) -> Option<PageIndex> {
        if self.header.next_version == PageIndex::max() {
            None
        } else {
            Some(self.header.next_version)
        }
    }

    pub fn previous_version(&self) -> Option<PageIndex> {
        if self.header.previous_version == PageIndex::max() {
            None
        } else {
            Some(self.header.previous_version)
        }
    }

    pub fn set_next_version(&mut self, link: Option<PageIndex>) {
        if let Some(link) = link {
            assert!(link != PageIndex::max());

            self.header.next_version = link;
        } else {
            self.header.next_version = PageIndex::max();
        }
    }

    pub fn set_previous_version(&mut self, link: Option<PageIndex>) {
        if let Some(link) = link {
            assert!(link != PageIndex::max());

            self.header.previous_version = link;
        } else {
            self.header.previous_version = PageIndex::max();
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            header: PageHeader::new(),
            data: [0; _],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn serialize_has_correct_crc32() {
        let page = Page::new();

        let serialized = page.serialize();

        assert_eq!(&serialized[0..4], &[195, 136, 198, 29]);
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
            page.unwrap().data::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
        );
    }
}
