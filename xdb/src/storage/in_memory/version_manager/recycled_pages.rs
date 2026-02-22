use std::time::{Duration, Instant};

use tracing::{debug, trace};

use crate::storage::PageIndex;
use crate::storage::in_memory::version_manager::VersionedBlock;
use crate::storage::in_memory::version_manager::transaction::UninitializedPageGuard;
use crate::storage::in_memory::version_manager::vacuum::Vacuum;
use crate::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Recycler {
    pages: Mutex<Vec<PageIndex>>,
    data: Arc<VersionedBlock>,
    last_free_page_scan: Mutex<Option<Instant>>,
    #[allow(unused)] // TODO who should own vacuum?
    vacuum: Vacuum,
}

unsafe impl Send for Recycler {}

impl Recycler {
    pub const fn new(data: Arc<VersionedBlock>, vacuum: Vacuum) -> Self {
        Self {
            pages: Mutex::new(vec![]),
            data,
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

        Some(self.data.get_uninitialized(page))
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

        let mut pages = self.pages.try_lock().ok()?;
        pages.append(&mut self.data.take_free_pages(10000));

        *self.last_free_page_scan.lock().unwrap() = Some(Instant::now());

        debug!(queue_length = ?pages.len(), "recycled page queue filled up");

        pages.pop().map(|page| self.data.get_uninitialized(page))
    }
}
