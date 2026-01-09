use crate::{
    bplustree::{InteriorNodeReader, LeafNodeReader, Tree, TreeError},
    storage::{PageIndex, Storage},
};

impl<T: Storage> Tree<T> {
    pub fn to_dot(
        &self,
        stringify_key: impl Fn(&[u8]) -> String,
        stringify_value: impl Fn(&[u8]) -> String,
    ) -> Result<String, TreeError> {
        let mut output = String::new();

        let root_node_index = self.header()?.root;

        output += "digraph {\n";
        output += &self.node_to_dot(root_node_index, &stringify_key, &stringify_value)?;
        output += "}\n";

        Ok(output)
    }

    fn node_to_dot(
        &self,
        node_index: PageIndex,
        stringify_key: &impl Fn(&[u8]) -> String,
        stringify_value: &impl Fn(&[u8]) -> String,
    ) -> Result<String, TreeError> {
        let key_size = self.key_size;
        let value_size = self.value_size;

        let node = self.node(node_index)?;

        let mut output = String::new();

        if node.is_leaf() {
            let mut label: Vec<String> = vec![];

            for entry in LeafNodeReader::new(node, key_size, value_size).entries() {
                label.push(format!(
                    "{}/{}",
                    (stringify_key)(entry.key()),
                    (stringify_value)(entry.value())
                ));
            }

            let label = label.join("\\n");

            output += &format!("N{node_index}[label=\"{label}\"];\n");
        } else {
            let mut label: Vec<String> = vec![];

            for key in InteriorNodeReader::new(node, key_size).keys() {
                label.push(stringify_key(key));
            }

            let label = label.join("\\n");

            output += &format!("N{node_index}[label=\"{label}\"];\n");

            for (index, value) in InteriorNodeReader::new(node, key_size).values().enumerate() {
                output += &format!("N{node_index} -> N{value}[label=\"{index}\"];\n");

                output += &self.node_to_dot(value, stringify_key, stringify_value)?;
            }
        }

        Ok(output)
    }
}
