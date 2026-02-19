use tracing::{debug, trace};

use crate::{
    storage::in_memory::{
        Bitmap,
        block::{Block, UninitializedPageGuard},
        version_manager::vacuum::Vacuum,
    },
    sync::{Arc, Mutex},
};
use std::{
    mem::MaybeUninit,
    ptr::NonNull,
    time::{Duration, Instant},
};

use crate::{page::Page, storage::PageIndex};

#[derive(Debug)]
pub struct Recycler {
    pages: Mutex<Vec<(NonNull<MaybeUninit<Page>>, PageIndex)>>,
    // TODO data and freemap together should be a struct (VersionManagedBlock or smth, idk)
    data: Arc<Block>,
    freemap: Arc<Bitmap>,
    last_free_page_scan: Mutex<Option<Instant>>,
    vacuum: Vacuum,
}

unsafe impl Send for Recycler {}

impl Recycler {
    pub const fn new(data: Arc<Block>, freemap: Arc<Bitmap>, vacuum: Vacuum) -> Self {
        Self {
            pages: Mutex::new(vec![]),
            data,
            freemap,
            last_free_page_scan: Mutex::new(None),
            vacuum,
        }
    }

    fn next(&'_ self) -> Option<UninitializedPageGuard<'_>> {
        let page = self.pages.try_lock().ok()?.pop()?;

        debug!(
            queue_length = self.pages.try_lock().ok().map(|x| x.len()),
            "got a page from recycled_page_queue",
        );

        Some(unsafe { UninitializedPageGuard::new(&self.data, page.0, page.1) })
    }

    pub fn get_recycled_page(&self) -> Option<UninitializedPageGuard<'_>> {
        // don't bother with all this if there aren't many allocated pages (TODO figure out if this
        // number makes sense)
        if self.data.allocated_page_count() < 50000 {
            trace!("not recycling pages, too few were allocated");

            return None;
        }

        let recycled_page = self.next();

        if let Some(page) = recycled_page {
            return Some(page);
        }

        let since_last_free_page_scan = self
            .last_free_page_scan
            .lock()
            .unwrap()
            .map_or(Duration::MAX, |x| x.elapsed());

        // TODO we should allow the scan to happen as often as it wants to if there's no space in
        // storage anymore
        if since_last_free_page_scan < Duration::from_secs(1) {
            trace!(?since_last_free_page_scan, "skipping page scan",);

            return None;
        }

        // TODO we need a better API for this - we must stop vacuum from marking the page as unused
        // again before we have a chance to reuse it, potentially resulting in multiple threads
        // getting the same page
        let lock = self.vacuum.freeze();
        let mut pages = self.pages.try_lock().ok()?;

        for free_page in self
            .freemap
            .find_and_unset(10000)
            .into_iter()
            .map(|index| self.data.get_uninitialized(PageIndex(index as u64)))
        {
            pages.push((free_page.as_ptr(), free_page.physical_index()));
        }

        drop(lock);
        *self.last_free_page_scan.lock().unwrap() = Some(Instant::now());

        debug!(queue_length = ?pages.len(), "recycled page queue filled up");

        pages
            .pop()
            .map(|page| unsafe { UninitializedPageGuard::new(&self.data, page.0, page.1) })
    }
}
