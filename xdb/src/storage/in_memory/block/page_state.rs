use std::sync::atomic::{AtomicU64, Ordering};

const fn mask(start_bit: u64, end_bit: u64) -> u64 {
    assert!(end_bit <= start_bit);

    if start_bit == end_bit {
        return 1 << start_bit;
    }

    1 << start_bit | mask(start_bit - 1, end_bit)
}

#[derive(Debug)]
#[repr(transparent)]
// TODO can we fit everything in u32 or even u16?
pub struct PageState(AtomicU64);

const _: () = assert!(size_of::<PageState>() == size_of::<u64>());

impl PageState {
    const MASK_IS_INITIALIZED: u64 = 1 << 63;
    #[allow(unused)]
    const MASK_READERS_WAITING: u64 = 1 << 62;
    #[allow(unused)]
    const MASK_WRITERS_WAITING: u64 = 1 << 61;
    const SHIFT_READER_COUNT: u64 = 44;
    const MASK_READER_COUNT: u64 = mask(60, Self::SHIFT_READER_COUNT);
    const MASK_HAS_WRITER: u64 = 1 << 43;

    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn mark_initialized(&self) {
        let previous_state = self
            .0
            .fetch_or(Self::MASK_IS_INITIALIZED, Ordering::Release);
        assert!(previous_state & Self::MASK_IS_INITIALIZED == 0);
    }

    pub fn is_initialized(&self) -> bool {
        self.0.load(Ordering::Acquire) & Self::MASK_IS_INITIALIZED > 0
    }

    pub fn lock_write(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |f| {
                if f & Self::MASK_READER_COUNT >> Self::SHIFT_READER_COUNT > 0 {
                    return None;
                }

                if f & Self::MASK_HAS_WRITER > 0 {
                    return None;
                }

                Some(f | Self::MASK_HAS_WRITER)
            })
            .expect("cannot lock for write, already locked");
    }

    pub fn unlock_write(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Release, Ordering::Relaxed, |x| {
                Some(x & !Self::MASK_HAS_WRITER)
            })
            .unwrap();
    }

    pub fn lock_read(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |x| {
                if x & Self::MASK_HAS_WRITER > 0 {
                    return None;
                }

                let reader_count = (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT;
                let new_reader_count = reader_count + 1;
                let shifted_new_reader_count = new_reader_count << Self::SHIFT_READER_COUNT;

                assert!(shifted_new_reader_count & !Self::MASK_READER_COUNT == 0);

                Some((x & !Self::MASK_READER_COUNT) | shifted_new_reader_count)
            })
            .expect("cannot block for read, as there already is a writer");
    }

    pub fn unlock_read(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Release, Ordering::Relaxed, |x| {
                assert!(x & Self::MASK_HAS_WRITER == 0);

                let reader_count = (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT;
                assert!(reader_count > 0);
                let shifted_new_reader_count = (reader_count - 1) << Self::SHIFT_READER_COUNT;

                Some((x & !Self::MASK_READER_COUNT) | shifted_new_reader_count)
            })
            .unwrap();
    }

    pub fn lock_upgrade(&self) {
        self.0
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                // TODO we should wait on a futex here instead once we have multiple threads
                assert!((x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT == 1);
                assert!(x & Self::MASK_HAS_WRITER == 0);

                Some((x & !Self::MASK_READER_COUNT) | Self::MASK_HAS_WRITER)
            })
            .unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn mask_tests() {
        assert_eq!(mask(7, 0), 0b1111_1111);
        assert_eq!(mask(15, 8), 0b1111_1111_0000_0000);
    }
}
