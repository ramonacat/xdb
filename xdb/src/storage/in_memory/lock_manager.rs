use std::{
    collections::{HashMap, HashSet},
    sync::RwLock,
};

use arbitrary::Result;
use log::debug;

use crate::storage::{
    PageIndex, StorageError, TransactionId,
    in_memory::block::{PageGuard, PageGuardMut, PageRef},
};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
enum LockKind {
    Read,
    Write,
}

#[derive(Debug)]
struct LockedPages {
    read: HashMap<PageIndex, usize>,
    write: HashSet<PageIndex>,
}

#[derive(Debug, Clone, Copy)]
enum LockStatus {
    Read(usize),
    Write,
}

impl From<LockStatus> for LockKind {
    fn from(value: LockStatus) -> Self {
        match value {
            LockStatus::Read(_) => Self::Read,
            LockStatus::Write => Self::Write,
        }
    }
}

impl LockedPages {
    pub fn new() -> Self {
        Self {
            read: HashMap::new(),
            write: HashSet::new(),
        }
    }

    fn add(&mut self, index: PageIndex, kind: LockKind) {
        match kind {
            LockKind::Read => {
                debug_assert!(!self.write.contains(&index));

                *self.read.entry(index).or_insert(0) += 1;
            }
            LockKind::Write => {
                debug_assert!(*self.read.get(&index).unwrap_or(&0) <= 1);

                // this is to handle upgrades (TODO should those be requested explicitly?)
                if let Some(read_locks) = self.read.get_mut(&index) {
                    if *read_locks <= 1 {
                        *read_locks = 0;
                    } else {
                        panic!("more than one read lock found");
                    }
                }

                self.write.insert(index);
            }
        }
    }

    #[must_use]
    fn remove(&mut self, index: PageIndex, kind: LockKind) -> bool {
        match kind {
            LockKind::Read => {
                debug_assert!(!self.write.contains(&index));

                let reader_count = self.read.get_mut(&index).unwrap();
                *reader_count = reader_count.strict_sub(1);

                *reader_count == 0
            }
            LockKind::Write => {
                debug_assert!(*self.read.get(&index).unwrap_or(&0) == 0);

                let removed = self.write.remove(&index);
                assert!(removed);

                true
            }
        }
    }

    fn get_status(&self, index: PageIndex) -> Option<LockStatus> {
        if self.write.contains(&index) {
            return Some(LockStatus::Write);
        }
        let read_count = *self.read.get(&index).unwrap_or(&0);
        if read_count > 0 {
            return Some(LockStatus::Read(read_count));
        }

        None
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct Edge {
    from: TransactionId,
    to: TransactionId,
    page: PageIndex,
    kind: LockKind,
}

#[derive(Debug)]
struct LockManagerState {
    edges: HashSet<Edge>,
    pages: HashMap<TransactionId, LockedPages>,
}

impl LockManagerState {
    fn new() -> Self {
        Self {
            edges: HashSet::new(),
            pages: HashMap::new(),
        }
    }

    #[must_use]
    fn add_page(&mut self, txid: TransactionId, index: PageIndex, kind: LockKind) -> bool {
        let mut my_blockers = HashSet::new();

        for (blocker_txid, pages) in &self.pages {
            if let Some(blocker_status) = pages.get_status(index) {
                if *blocker_txid == txid {
                    let blocks_with_self = match (kind, blocker_status) {
                        (LockKind::Read, LockStatus::Read(_))
                        // upgrade
                        | (LockKind::Write, LockStatus::Read(1)) => false,
                        (LockKind::Read | LockKind::Write, LockStatus::Write)
                        | (LockKind::Write, LockStatus::Read(_))
                         => true,
                    };

                    if blocks_with_self {
                        log::debug!(
                            "would block with self: {index:?} {txid:?} {blocker_status:?} {kind:?} :: {self:?}"
                        );

                        return false;
                    }
                } else {
                    // TODO should the special casing happen in would_cycle_with instead?
                    my_blockers.insert(Edge {
                        from: *blocker_txid,
                        to: txid,
                        page: index,
                        kind: blocker_status.into(),
                    });
                }
            }
        }

        if self.would_cycle_with(&my_blockers, kind) {
            log::info!("would create a cycle: {txid:?} {index:?} {kind:?} :: {self:?}");

            return false;
        }

        self.pages_for_mut(txid).add(index, kind);

        for edge in my_blockers {
            self.edges.insert(edge);
        }

        true
    }

    fn remove_page(&mut self, txid: TransactionId, index: PageIndex, kind: LockKind) {
        let lock_removed = self.pages_for_mut(txid).remove(index, kind);
        if !lock_removed {
            return;
        }

        let removed = self.edges.extract_if(|edge| {
            (edge.from == txid && edge.kind == kind && edge.page == index)
                || (edge.to == txid && edge.page == index)
        });

        for _edge in removed {
            // TODO wake up the related waiters
        }
    }

    fn pages_for_mut(&mut self, txid: TransactionId) -> &mut LockedPages {
        self.pages.entry(txid).or_insert_with(LockedPages::new)
    }

    fn would_cycle_with(&self, virtual_edges: &HashSet<Edge>, kind: LockKind) -> bool {
        struct Visitor<'a> {
            edges: &'a HashSet<Edge>,
            virtual_edges: &'a HashSet<Edge>,

            visited: HashSet<TransactionId>,
            finished: HashSet<TransactionId>,
        }

        impl Visitor<'_> {
            fn visit(mut self, vertices: Vec<TransactionId>) -> HashSet<TransactionId> {
                for vertex in vertices {
                    if self.visit_inner(vertex) {
                        return self.visited;
                    }
                }

                // TODO return an Option<> instead?
                HashSet::new()
            }

            fn neighbours_of(&self, from: TransactionId) -> Vec<TransactionId> {
                self.edges
                    .iter()
                    .filter(move |x| x.from == from)
                    .chain(self.virtual_edges.iter().filter(move |x| x.from == from))
                    .map(|x| x.to)
                    .collect()
            }

            fn visit_inner(&mut self, vertex: TransactionId) -> bool {
                if self.finished.contains(&vertex) {
                    return false;
                }

                if self.visited.contains(&vertex) {
                    return true;
                }

                self.visited.insert(vertex);

                for to in self.neighbours_of(vertex) {
                    if self.visit_inner(to) {
                        return true;
                    }
                }

                self.finished.insert(vertex);

                false
            }
        }

        let cycle = (Visitor {
            edges: &self.edges,
            virtual_edges,
            visited: HashSet::new(),
            finished: HashSet::new(),
        })
        .visit(self.pages.keys().copied().collect());

        #[allow(clippy::match_same_arms)]
        match kind {
            LockKind::Write => !cycle.is_empty(),
            // TODO we don't care about cycles if all the locks involved are for read
            LockKind::Read => !cycle.is_empty(),
        }
    }
}

#[derive(Debug)]
pub struct LockManager {
    state: RwLock<LockManagerState>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(LockManagerState::new()),
        }
    }

    // TODO all these lock_* methods are practically the same, can we unify them?

    pub fn lock_read<'storage>(
        &self,
        txid: TransactionId,
        page: PageRef<'storage>,
    ) -> Result<PageGuard<'storage>, StorageError> {
        let mut state_guard = self.state.write().unwrap();

        if !state_guard.add_page(txid, page.index(), LockKind::Read) {
            return Err(StorageError::Deadlock(page.index()));
        }

        debug!("locking for read {txid:?} {:?}", page.index());

        Ok(page.get())
    }

    pub fn lock_upgrade<'storage>(
        &self,
        txid: TransactionId,
        guard: PageGuard<'storage>,
    ) -> Result<PageGuardMut<'storage>, StorageError> {
        let mut state_guard = self.state.write().unwrap();

        // TODO do we need to differentiate between LockKind::Write and LockKind::Upgrade? All the
        // transaction code guards against it, but the API doesn't stop anyone from requesting a separate write
        // lock when there's already a read lock
        if !state_guard.add_page(txid, guard.index(), LockKind::Write) {
            return Err(StorageError::Deadlock(guard.index()));
        }

        debug!("upgrading lock {txid:?} {:?}", guard.index());

        Ok(guard.upgrade())
    }

    pub fn lock_write<'storage>(
        &self,
        txid: TransactionId,
        page: PageRef<'storage>,
    ) -> Result<PageGuardMut<'storage>, StorageError> {
        let mut state_guard = self.state.write().unwrap();

        if !state_guard.add_page(txid, page.index(), LockKind::Write) {
            return Err(StorageError::Deadlock(page.index()));
        }

        debug!("locking for write {txid:?} {:?}", page.index());

        Ok(page.get_mut())
    }

    // TODO we should probably deal with the guards internally here, so that it is impossible to
    // drop one without being accounted for
    pub fn unlock_read(&self, txid: TransactionId, page: PageGuard<'_>) {
        let index = page.index();

        let mut state_guard = self.state.write().unwrap();

        drop(page);

        state_guard.remove_page(txid, index, LockKind::Read);
    }

    pub fn unlock_write(&self, txid: TransactionId, page: PageGuardMut<'_>) {
        let index = page.index();

        let mut state_guard = self.state.write().unwrap();

        drop(page);

        state_guard.remove_page(txid, index, LockKind::Write);
    }
}
