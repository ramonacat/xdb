use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use log::{Level, debug, log_enabled};

use crate::storage::{PageIndex, TransactionId};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum LockKind {
    Read,
    Write,
    Upgrade,
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

#[derive(Debug)]
struct LockedPages {
    read: HashMap<PageIndex, usize>,
    write: HashSet<PageIndex>,
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
                debug_assert!(*self.read.get(&index).unwrap_or(&0) == 0);

                self.write.insert(index);
            }
            LockKind::Upgrade => {
                debug_assert!(*self.read.get(&index).unwrap_or(&0) == 1);

                *self.read.get_mut(&index).unwrap() = 0;

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
            LockKind::Upgrade => todo!(),
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

impl Edge {
    pub fn debug(&self, highlight: Option<PageIndex>) -> String {
        let highlight = if highlight.is_some_and(|x| x == self.page) {
            "**"
        } else {
            ""
        };

        format!(
            "{:?} -> {:?} ({:?} {}{:?}{})",
            self.from, self.to, self.kind, highlight, self.page, highlight
        )
    }
}

#[derive(Debug)]
pub struct LockManagerState {
    edges: HashSet<Edge>,
    pages: HashMap<TransactionId, LockedPages>,
}

impl LockManagerState {
    pub fn new() -> Self {
        Self {
            edges: HashSet::new(),
            pages: HashMap::new(),
        }
    }

    #[must_use]
    pub fn add_page(&mut self, txid: TransactionId, index: PageIndex, kind: LockKind) -> bool {
        let mut my_blockers = HashSet::new();

        for (blocker_txid, pages) in &self.pages {
            if let Some(blocker_status) = pages.get_status(index) {
                if *blocker_txid == txid {
                    let blocks_with_self = match (kind, blocker_status) {
                        (LockKind::Read, LockStatus::Read(_))
                        | (LockKind::Upgrade, LockStatus::Read(1)) => false,
                        (
                            LockKind::Read | LockKind::Write | LockKind::Upgrade,
                            LockStatus::Write,
                        )
                        | (LockKind::Write | LockKind::Upgrade, LockStatus::Read(_)) => true,
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
                        from: txid,
                        to: *blocker_txid,
                        page: index,
                        kind: blocker_status.into(),
                    });
                }
            }
        }

        // TODO if my_blockers.is_empty(), do we still need to even call this?
        if self.would_cycle_with(txid, &my_blockers, kind) {
            return false;
        }

        self.pages_for_mut(txid).add(index, kind);

        if kind == LockKind::Upgrade {
            self.edges
                .retain(|x| !(x.from == txid && x.kind == LockKind::Read && x.page == index));
        }

        for edge in my_blockers {
            self.edges.insert(edge);
        }

        true
    }

    #[must_use]
    pub fn remove_page(
        &mut self,
        txid: TransactionId,
        index: PageIndex,
        kind: LockKind,
    ) -> Vec<PageIndex> {
        let lock_removed = self.pages_for_mut(txid).remove(index, kind);
        if !lock_removed {
            return vec![];
        }

        let removed_edges = self.edges.extract_if(|edge| {
            (edge.from == txid && edge.kind == kind && edge.page == index)
                || (edge.to == txid && edge.page == index)
        });

        removed_edges
            .map(|x| x.page)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn pages_for_mut(&mut self, txid: TransactionId) -> &mut LockedPages {
        self.pages.entry(txid).or_insert_with(LockedPages::new)
    }

    fn would_cycle_with(
        &self,
        txid: TransactionId,
        virtual_edges: &HashSet<Edge>,
        kind: LockKind,
    ) -> bool {
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

        if log_enabled!(Level::Debug) {
            let formatted_virtual_edges = if virtual_edges.is_empty() {
                "[none]".to_string()
            } else {
                "\n".to_string()
                    + &virtual_edges
                        .iter()
                        .map(|x| x.debug(None))
                        .fold(String::new(), |a, x| a + "\n    " + &x)
            };
            let own_edges = if self.edges.is_empty() {
                String::new()
            } else {
                format!("\n    ===\n{}", self.edges_debug(None))
            };
            debug!(
                "checking for cycle {txid:?} {kind:?} edges: {formatted_virtual_edges}{own_edges}",
            );
        }

        let cycle = (Visitor {
            edges: &self.edges,
            virtual_edges,
            visited: HashSet::new(),
            finished: HashSet::new(),
        })
        .visit(self.pages.keys().copied().collect());

        // TODO if kind is LockKind::Read and the cycle is only reads, then we should return false,
        // as read cycles are okay
        if !cycle.is_empty() {
            log::info!("would create a cycle: {txid:?} {kind:?} :: {cycle:?}");
            return true;
        }

        false
    }

    pub fn edges_debug(&self, highlight: Option<PageIndex>) -> String {
        let mut result = String::new();

        for edge in &self.edges {
            writeln!(result, "    {}", edge.debug(highlight)).unwrap();
        }

        result
    }
}
