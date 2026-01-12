use std::fmt::Display;

use bytemuck::Pod;

use crate::bplustree::{AnyNodeId, TreeTransaction};
use crate::{
    bplustree::{Tree, TreeError},
    storage::Storage,
};

impl<T: Storage, TKey: Pod + PartialOrd + Display> Tree<T, TKey> {
    pub fn into_dot(self, stringify_value: impl Fn(&[u8]) -> String) -> Result<String, TreeError> {
        let mut output = String::new();

        let transaction = self.transaction()?;
        let root_node_index = AnyNodeId::new(transaction.read_header(|h| h.root)?);

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
        let output = transaction.read_node(node_index, |node| {
            let mut output = String::new();

            match node {
                super::AnyNodeReader::Interior(reader) => {
                    let mut label: Vec<String> = vec![format!("index: {node_index}")];

                    for key in reader.keys() {
                        label.push(key.to_string());
                    }

                    let label = label.join("\\n");

                    output += &format!("N{node_index}[label=\"{label}\"];\n");

                    for (index, value) in reader.values().enumerate() {
                        output += &format!("N{node_index} -> N{value}[label=\"{index}\"];\n");

                        output += &Self::node_to_dot(transaction, value, stringify_value)?;
                    }
                }
                super::AnyNodeReader::Leaf(reader) => {
                    let mut label: Vec<String> = vec![format!("index: {node_index}")];

                    if let Some(previous) = reader.previous() {
                        label.push(format!("previous: {previous}"));
                    }

                    if let Some(next) = reader.next() {
                        label.push(format!("next: {next}"));
                    }

                    for entry in reader.entries() {
                        label.push(format!(
                            "{}/{}",
                            entry.key(),
                            (stringify_value)(entry.value())
                        ));
                    }

                    let label = label.join("\\n");

                    output += &format!("N{node_index}[label=\"{label}\"];\n");
                }
            }

            Ok(output)
        });

        output?
    }
}
