use crate::bplustree::node::AnyNodeKind;
use crate::bplustree::{AnyNodeId, Node, TreeKey, TreeTransaction};
use crate::{
    bplustree::{Tree, TreeError},
    storage::Storage,
};
use std::fmt::Write;

impl<T: Storage, TKey: TreeKey> Tree<T, TKey> {
    pub fn to_dot(&self, stringify_value: impl Fn(&[u8]) -> String) -> Result<String, TreeError> {
        let mut output = String::new();

        let transaction = self.transaction()?;
        let root_node_index = transaction.get_root()?;

        output += "digraph {\n";
        output += &Self::node_to_dot(&transaction, root_node_index, &stringify_value)?;
        output += "}\n";

        Ok(output)
    }

    fn node_to_dot(
        transaction: &TreeTransaction<'_, T, TKey>,
        node_index: AnyNodeId,
        stringify_value: &impl Fn(&[u8]) -> String,
    ) -> Result<String, TreeError> {
        let output = transaction.read_nodes(node_index, |node| {
            let mut output = String::new();

            match node.as_any() {
                AnyNodeKind::Interior(node) => {
                    let mut label: Vec<String> = vec![
                        format!("index: {node_index}"),
                        format!(
                            "parent: {}",
                            node.parent()
                                .map_or_else(|| "none".to_string(), |x| x.to_string())
                        ),
                    ];

                    for key in node.keys() {
                        label.push(format!("{key:?}"));
                    }

                    let label = label.join("\\n");

                    writeln!(output, "N{node_index}[label=\"{label}\"];").unwrap();

                    for (index, value) in node.values() {
                        writeln!(output, "N{node_index} -> N{value}[label=\"{index:?}\"];")
                            .unwrap();

                        output += &Self::node_to_dot(transaction, value, stringify_value)?;
                    }
                }
                AnyNodeKind::Leaf(node) => {
                    let mut label: Vec<String> = vec![
                        format!("index: {node_index}"),
                        format!(
                            "parent: {}",
                            node.parent()
                                .map_or_else(|| "none".to_string(), |x| x.to_string())
                        ),
                    ];

                    if let Some(previous) = node.previous() {
                        label.push(format!("previous: {previous}"));
                    }

                    if let Some(next) = node.next() {
                        label.push(format!("next: {next}"));
                    }

                    for entry in node.entries() {
                        label.push(format!(
                            "{:?}/{}",
                            entry.key(),
                            (stringify_value)(entry.value())
                        ));
                    }

                    let label = label.join("\\n");

                    writeln!(output, "N{node_index}[label=\"{label}\"];").unwrap();
                }
            }

            Ok(output)
        });

        output?
    }
}
