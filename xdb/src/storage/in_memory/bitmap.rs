use crate::{
    page::PAGE_DATA_SIZE,
    storage::{PageIndex, in_memory::block::Block},
};

struct BitLocation {
    page: PageIndex,
    byte_in_page: u64,
    bit_in_byte: u8,
}

impl BitLocation {
    fn new(index: u64) -> Self {
        let bits_per_page = 8 * u64::try_from(PAGE_DATA_SIZE.as_bytes()).unwrap();

        let page = PageIndex::from_value(index / bits_per_page);
        let bit_in_page = index % bits_per_page;

        Self {
            page,
            byte_in_page: bit_in_page / 8,
            bit_in_byte: (bit_in_page % 8) as u8,
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
            block: Block::new(name),
        }
    }

    pub fn set(&self, index: u64) {
        let bit_location = BitLocation::new(index);
        let page = self.block.get_or_allocate_zeroed(bit_location.page);

        page.lock().data_mut::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
            [usize::try_from(bit_location.byte_in_page).unwrap()] |= bit_location.bit_in_byte;
    }

    /// This will find **A** bit and flip it, atomically.
    /// The bit will not neccesairly be the first bit (as for example there could be a race
    /// condition while looking for it). It might not find any bits, even if some values are set.
    /// This is very much best-effort, in terms of accuracy of finding a bit.
    pub fn find_and_unset(&self) -> Option<usize> {
        for page_index in 0..self.block.page_count_lower_bound() {
            let Ok(mut page) = self
                .block
                .get(PageIndex::from_value(page_index), None)
                .lock_nowait()
            else {
                continue;
            };

            for (i, byte) in page
                .data_mut::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
                .iter_mut()
                .enumerate()
            {
                if *byte != 0 {
                    let bit_index = byte.leading_zeros();

                    *byte &= !(1 >> bit_index);

                    return Some(
                        (usize::try_from(page_index).unwrap() * PAGE_DATA_SIZE.as_bytes() * 8)
                            + (i * 8)
                            + (usize::try_from(bit_index).unwrap()),
                    );
                }
            }
        }

        None
    }
}
