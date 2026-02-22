use bytemuck::{Pod, Zeroable};

use crate::Size;
use crate::storage::in_memory::InMemoryPageId;
use crate::storage::in_memory::block::Block;
use crate::storage::page::PAGE_DATA_SIZE;
use crate::storage::{PageIndex, StorageError};

#[derive(Debug, Zeroable, Pod, Clone, Copy)]
#[repr(C)]
struct BitmapPage {
    count: u16,
    _unused1: u16,
    _unused2: u32,

    data: BitmapData,
}

const BITMAP_DATA_SIZE: Size = PAGE_DATA_SIZE.subtract(Size::of::<u16>());
const _: () = assert!(Size::of::<BitmapPage>().is_equal(PAGE_DATA_SIZE));

type BitmapEntry = u64;
type BitmapData = [BitmapEntry; BITMAP_DATA_SIZE.divide(Size::of::<BitmapEntry>())];

impl BitmapPage {
    fn set(&mut self, bit_location: BitLocation) {
        self.count += 1;

        self.data[usize::try_from(bit_location.item_in_page).unwrap()] |=
            1 << bit_location.bit_in_item;
    }

    fn find_and_unset(&mut self, count: usize) -> Vec<usize> {
        debug_assert!(self.count > 0);

        let mut result = vec![];

        for (i, item) in self.data.iter_mut().enumerate() {
            if self.count == 0 || count == result.len() {
                break;
            }

            while *item != 0 {
                let bit_index = item.trailing_zeros();

                *item &= !(1 << bit_index);

                self.count -= 1;

                result.push(
                    (i * usize::try_from(BitmapEntry::BITS).unwrap())
                        + (usize::try_from(bit_index).unwrap()),
                );

                if result.len() == count {
                    break;
                }
            }
        }

        result
    }
}

#[derive(Debug, Clone, Copy)]
struct BitLocation {
    page: PageIndex,
    item_in_page: u32,
    bit_in_item: u8,
}

impl BitLocation {
    fn new(index: u64) -> Self {
        let bits_per_page = (Size::of::<BitmapData>().as_bytes() * 8) as u64;

        let page = PageIndex::from_value(index / bits_per_page);
        let bit_in_page = u32::try_from(index % bits_per_page).unwrap();

        Self {
            page,
            item_in_page: bit_in_page / BitmapEntry::BITS,
            bit_in_item: (bit_in_page % BitmapEntry::BITS) as u8,
        }
    }
}

#[derive(Debug)]
pub(super) struct Bitmap {
    block: Block,
}

impl Bitmap {
    pub fn new(name: String) -> Self {
        Self {
            // TODO we should get the block as an argument, so the user can pass whatever storage
            // they need
            block: Block::new(name),
        }
    }

    pub fn set(&self, index: u64) -> Result<(), StorageError<InMemoryPageId>> {
        let bit_location = BitLocation::new(index);
        let mut page = self.block.get_or_allocate_zeroed(bit_location.page)?;

        page.data_mut::<BitmapPage>().set(bit_location);

        Ok(())
    }

    /// This will find at most count bits and flip each of them atomically.
    /// The bit will not neccesairly be the first bit (as for example there could be a race
    /// condition while looking for it). It might not find any bits, even if some values are set.
    /// This is very much best-effort, in terms of accuracy of finding a bit.
    pub fn find_and_unset(&self, count: usize) -> Vec<usize> {
        let mut result = vec![];

        for page_index in 0..=self.block.allocated_page_count() {
            if result.len() == count {
                break;
            }

            let Some(page_ref) = self.block.try_get(PageIndex::from_value(page_index)) else {
                continue;
            };

            let Ok(mut page) = page_ref.try_upgrade() else {
                continue;
            };

            let data = page.data_mut::<BitmapPage>();
            if data.count == 0 {
                continue;
            }

            let page_bit_offset =
                usize::try_from(page_index).unwrap() * Size::of::<BitmapData>().as_bytes() * 8;

            for in_page_bit_index in data.find_and_unset(count - result.len()) {
                result.push(page_bit_offset + in_page_bit_index);
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::*;

    fn find_and_unset_retries(bitmap: &Bitmap, count: usize) -> Vec<usize> {
        let mut result = HashSet::new();
        for _ in 0..10 {
            for x in bitmap.find_and_unset(count) {
                result.insert(x);
            }

            crate::thread::yield_now();
        }

        result.into_iter().collect()
    }

    #[test]
    fn bitmap() {
        let bitmap = Bitmap::new("test".into());

        bitmap.set(12).unwrap();

        assert_eq!(find_and_unset_retries(&bitmap, 10), vec![12]);
        assert_eq!(find_and_unset_retries(&bitmap, 10), Vec::<usize>::new());

        bitmap.set(50_000).unwrap();

        assert_eq!(find_and_unset_retries(&bitmap, 10), vec![50_000]);

        bitmap.set(1).unwrap();
        bitmap.set(50_000).unwrap();
        bitmap.set(3).unwrap();

        let mut found = find_and_unset_retries(&bitmap, 10);
        found.sort();

        assert_eq!(found, vec![1, 3, 50_000]);
    }

    #[test]
    fn bitmap_respects_count() {
        let bitmap = Bitmap::new("test".into());

        for i in 25_000..30_000 {
            bitmap.set(i).unwrap();
        }

        let found = bitmap.find_and_unset(100);
        assert!(
            found.len() <= 100,
            "requested 100 items, found: {}",
            found.len()
        );
    }
}
