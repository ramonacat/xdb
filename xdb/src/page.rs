use bytemuck::{
    AnyBitPattern, NoUninit, Pod, Zeroable, bytes_of, from_bytes, from_bytes_mut, must_cast,
};
use thiserror::Error;

use crate::{Size, checksum::Checksum, storage::TransactionId};

pub const PAGE_SIZE: Size = Size::B(4096);
pub const PAGE_DATA_SIZE: Size = PAGE_SIZE.subtract(Size::of::<PageHeader>());

#[derive(Debug, Error)]
pub enum PageError {
    #[error("Incorrect checksum")]
    Checksum,
}

#[derive(Debug, Pod, Zeroable, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct PageVersion(u64);

impl PageVersion {
    const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug, Pod, Clone, Copy, Zeroable)]
#[repr(C, align(8))]
struct PageHeader {
    checksum: Checksum,
    _unused1: u32,
    visible_from: Option<TransactionId>,
    visible_until: Option<TransactionId>,
    version: PageVersion,
}

const _: () = assert!(size_of::<PageHeader>() == 4 * size_of::<u64>());

#[derive(Debug, Pod, Clone, Copy, Zeroable)]
#[repr(C, align(8))]
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_DATA_SIZE.as_bytes()],
}

const _: () = assert!(Size::of::<Page>().is_equal(PAGE_SIZE));

impl Page {
    pub fn from_data<T: AnyBitPattern + NoUninit>(data: T) -> Self {
        Self {
            header: PageHeader::zeroed(),
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

    pub fn is_visible_in(&self, txid: TransactionId) -> bool {
        if let Some(from) = self.header.visible_from
            && from > txid
        {
            return false;
        }

        if let Some(to) = self.header.visible_until
            && to < txid
        {
            return false;
        }

        true
    }

    pub const fn set_visible_from(&mut self, txid: TransactionId) {
        self.header.visible_from = Some(txid);
    }

    pub const fn set_visible_until(&mut self, txid: TransactionId) {
        self.header.visible_until = Some(txid);
    }

    pub const fn version(&self) -> PageVersion {
        self.header.version
    }

    pub const fn increment_version(&mut self) {
        self.header.version = self.header.version.next();
    }

    pub const fn visible_until(&self) -> Option<TransactionId> {
        self.header.visible_until
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn serialize_has_correct_crc32() {
        let page = Page::zeroed();

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
            page.unwrap().data::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
        );
    }
}
