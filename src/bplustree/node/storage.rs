use bytemuck::from_bytes;

use crate::storage::{PageIndex, StorageError};
use crate::{bplustree::Node, storage::Storage};

pub(in crate::bplustree) struct NodeStorage<'storage> {
    storage: &'storage mut dyn Storage,
}

impl<'storage> NodeStorage<'storage> {
    pub fn new(storage: &'storage mut dyn Storage) -> Self {
        Self { storage }
    }

    pub fn get(&self, index: PageIndex) -> Result<&Node, StorageError> {
        Ok(from_bytes(self.storage.get(index)?.data()))
    }
}
