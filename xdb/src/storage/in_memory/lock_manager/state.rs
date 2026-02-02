use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use log::{Level, debug, log_enabled};

use crate::storage::{PageIndex, TransactionId};

#[derive(Debug)]
struct LockedPages {
    pages: HashSet<PageIndex>,
}

impl LockedPages {
    pub fn new() -> Self {
        Self {
            pages: HashSet::new(),
        }
    }

    fn add(&mut self, index: PageIndex) {
        self.pages.insert(index);
    }

    fn remove(&mut self, index: PageIndex) {
        let removed = self.pages.remove(&index);
        assert!(removed);
    }

    fn is_locked(&self, index: PageIndex) -> bool {
        self.pages.contains(&index)
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct Edge {
    from: TransactionId,
    to: TransactionId,
    page: PageIndex,
}

impl Edge {
    pub fn debug(&self, highlight: Option<PageIndex>) -> String {
        let highlight = if highlight.is_some_and(|x| x == self.page) {
            "**"
        } else {
            ""
        };

        format!(
            "{:?} -> {:?} ({}{:?}{})",
            self.from, self.to, highlight, self.page, highlight
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
    pub fn add_page(&mut self, txid: TransactionId, index: PageIndex) -> bool {
        let mut my_blockers = HashSet::new();

        for (blocker_txid, pages) in &self.pages {
            if !pages.is_locked(index) {
                continue;
            }
            if *blocker_txid == txid {
                log::debug!("would block with self: {index:?} {txid:?} :: {self:?}");

                return false;
            }

            my_blockers.insert(Edge {
                from: txid,
                to: *blocker_txid,
                page: index,
            });
        }

        if !my_blockers.is_empty() && self.would_cycle_with(txid, &my_blockers) {
            return false;
        }

        self.pages_for_mut(txid).add(index);

        for edge in my_blockers {
            self.edges.insert(edge);
        }

        true
    }

    #[must_use]
    pub fn remove_page(&mut self, txid: TransactionId, index: PageIndex) -> Vec<PageIndex> {
        self.pages_for_mut(txid).remove(index);

        let removed_edges = self
            .edges
            .extract_if(|edge| edge.from == txid && edge.page == index);

        removed_edges
            .map(|x| x.page)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn pages_for_mut(&mut self, txid: TransactionId) -> &mut LockedPages {
        self.pages.entry(txid).or_insert_with(LockedPages::new)
    }

    fn would_cycle_with(&self, txid: TransactionId, virtual_edges: &HashSet<Edge>) -> bool {
        struct Visitor<'a> {
            edges: &'a HashSet<Edge>,
            virtual_edges: &'a HashSet<Edge>,

            visited: HashSet<TransactionId>,
            finished: HashSet<TransactionId>,
        }

        impl Visitor<'_> {
            fn visit(mut self, vertices: Vec<TransactionId>) -> Option<HashSet<TransactionId>> {
                for vertex in vertices {
                    if self.visit_inner(vertex) {
                        return Some(self.visited);
                    }
                }

                None
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
            debug!("checking for cycle {txid:?} edges: {formatted_virtual_edges}{own_edges}",);
        }

        let cycle = (Visitor {
            edges: &self.edges,
            virtual_edges,
            visited: HashSet::new(),
            finished: HashSet::new(),
        })
        .visit(self.pages.keys().copied().collect());

        if cycle.is_some() {
            log::info!("would create a cycle: {txid:?} :: {cycle:?}");
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
