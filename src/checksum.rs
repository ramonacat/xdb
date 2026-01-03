use bytemuck::{Pod, Zeroable};
use crc32c::crc32c;

#[repr(transparent)]
#[derive(Pod, Clone, Copy, Zeroable, Debug, PartialEq, Eq)]
pub struct Checksum(u32);

impl Checksum {
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }

    pub fn of(bytes: &[u8]) -> Self {
        Self(crc32c(bytes))
    }

    pub fn clear(&mut self) {
        self.0 = 0;
    }
}
