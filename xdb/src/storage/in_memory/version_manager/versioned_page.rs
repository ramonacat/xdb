use crate::Size;
use crate::storage::page::{PAGE_SIZE, PageHeader};
use crate::storage::{Page, PageIndex, TransactionalTimestamp};
use bytemuck::checked::from_bytes_mut;
use bytemuck::{AnyBitPattern, NoUninit, Pod, Zeroable, from_bytes, must_cast};
use tracing::debug;

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct VersionedPageHeader {
    header: PageHeader,
    visible_from: TransactionalTimestamp,
    visible_until: TransactionalTimestamp,
    next_version: PageIndex,
    previous_version: PageIndex,
}

pub const VERSIONED_PAGE_DATA_SIZE: Size = PAGE_SIZE.subtract(Size::of::<VersionedPageHeader>());

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(C)]
pub struct VersionedPage {
    header: VersionedPageHeader,
    data: [u8; VERSIONED_PAGE_DATA_SIZE.as_bytes()],
}

const _: () = assert!(Size::of::<VersionedPage>().is_equal(PAGE_SIZE));

impl VersionedPage {
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
        debug!(
            "setting visible_until to {:?}",
            timestamp.unwrap_or_else(TransactionalTimestamp::zero)
        );

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
}

impl Page for VersionedPage {
    fn from_data<T: AnyBitPattern + NoUninit>(data: T) -> Self {
        Self {
            header: VersionedPageHeader {
                header: PageHeader::new(),
                visible_from: TransactionalTimestamp::zero(),
                visible_until: TransactionalTimestamp::zero(),
                next_version: PageIndex::max(),
                previous_version: PageIndex::max(),
            },
            data: must_cast(data),
        }
    }

    fn data<T: AnyBitPattern>(&self) -> &T {
        from_bytes(&self.data)
    }

    fn data_mut<T: AnyBitPattern + bytemuck::NoUninit>(&mut self) -> &mut T {
        from_bytes_mut(&mut self.data)
    }
}
