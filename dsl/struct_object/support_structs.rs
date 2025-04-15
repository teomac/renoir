use crate::dsl::ir::{AggregateFunction, ColumnRef, JoinType};
use crate::dsl::struct_object::object::QueryObject;
use indexmap::IndexMap;

#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub id: String,                                        // Unique stream identifier
    pub source_table: String,                              // Original table/CSV source
    pub alias: String, // Single, unique alias. If my query does not have a join, this is empty. Otherwise it is the alias of the table or the table name.
    pub initial_columns: IndexMap<String, String>, // Column name → type mappings
    pub access: AccessPath, // Access path for tuple
    pub is_keyed: bool, // Whether the stream is keyed
    pub key_columns: Vec<ColumnRef>, // Key columns
    pub op_chain: Vec<String>, // Operator chain
    pub final_struct: IndexMap<String, String>, // Final structure of the stream
    pub final_struct_name: Vec<String>, // Name of the final structure
    pub join_tree: Option<JoinTree>, // Join tree
    pub agg_position: IndexMap<AggregateFunction, String>, // Aggregate function → position mappings
    pub limit: Option<(usize, usize)>, // Limit and offset for the stream
    pub order_by: Vec<(ColumnRef, String)>, // Column name and order (ASC/DESC)
    pub distinct: bool,
}

#[derive(Debug, Clone)]
pub struct AccessPath {
    pub base_path: String,         // Base tuple access (e.g., ".0.1")
    pub null_check_required: bool, // Whether code needs to check is_some() first
}

impl AccessPath {
    pub fn new(base_path: String, null_check_required: bool) -> Self {
        AccessPath {
            base_path,
            null_check_required,
        }
    }

    pub fn get_base_path(&self) -> String {
        self.base_path.clone()
    }

    pub fn is_null_check_required(&self) -> bool {
        self.null_check_required
    }

    pub fn update_base_path(&mut self, new_path: String) {
        // Prepend .0 to the existing path if it exists
        if !self.base_path.is_empty() {
            self.base_path = format!(".0{}", self.base_path);
        } else {
            self.base_path = new_path;
        }
    }
}

impl StreamInfo {
    pub fn new(id: String, source_table: String, alias: String) -> Self {
        StreamInfo {
            id,
            source_table,
            alias,
            initial_columns: IndexMap::new(),
            access: AccessPath {
                base_path: String::new(),
                null_check_required: false,
            },
            is_keyed: false,
            key_columns: Vec::new(),
            op_chain: Vec::new(),
            final_struct: IndexMap::new(),
            final_struct_name: Vec::new(),
            join_tree: None,
            agg_position: IndexMap::new(),
            limit: None,
            order_by: Vec::new(),
            distinct: false,
        }
    }

    pub fn update_columns(&mut self, columns: IndexMap<String, String>) {
        self.initial_columns = columns;
    }

    pub fn insert_op(&mut self, op: String) {
        self.op_chain.push(op);
    }

    pub fn equals(&self, other: &StreamInfo) -> bool {
        self.id == other.id
    }

    pub fn source_equals(&self, other: &StreamInfo) -> bool {
        self.source_table == other.source_table && self.alias == other.alias
    }

    pub fn get_access(&self) -> AccessPath {
        self.access.clone()
    }

    pub fn check_if_column_exists(&self, column: &String) -> bool {
        self.initial_columns.get(column).is_some()
    }

    pub fn get_field_type(&self, field: &String) -> String {
        self.initial_columns.get(field).unwrap().clone()
    }

    pub fn update_agg_position(&mut self, agg: IndexMap<AggregateFunction, String>) {
        self.agg_position = agg;
    }

    pub fn get_initial_struct_name(&self) -> String {
        if self.final_struct_name.len() > 1 {
            self.final_struct_name.first().unwrap().clone()
        } else {
            format!("Struct_{}", self.source_table)
        }
    }

    pub fn get_second_struct_name(&self, query_object: &QueryObject) -> String {
        // Get the immediate right child stream from the join tree
        if let Some(ref join_tree) = self.join_tree {
            match join_tree {
                JoinTree::Join { right, .. } => {
                    match &**right {
                        JoinTree::Leaf(stream_name) => {
                            // Get the stream info for the right stream and return its final struct name
                            let right_stream = query_object.get_stream(stream_name);
                            if right_stream.final_struct_name.len() > 1 {
                                right_stream
                                    .final_struct_name
                                    .get(right_stream.final_struct_name.len() - 2)
                                    .unwrap()
                                    .clone()
                            } else {
                                format!("Struct_{}", right_stream.source_table)
                            }
                        }
                        _ => panic!("Unexpected join tree structure"),
                    }
                }
                _ => panic!("Expected a join tree"),
            }
        } else {
            // Fallback
            format!("Struct_{}", self.source_table)
        }
    }

    pub fn generate_nested_default(&self, query_object: &QueryObject) -> String {
        if self.join_tree.is_none() {
            return format!(
                "{}::default()",
                if self.final_struct_name.len() > 1 {
                    self.final_struct_name
                        .get(self.final_struct_name.len() - 2)
                        .unwrap()
                        .clone()
                } else {
                    format!("Struct_{}", self.source_table)
                }
            );
        }

        let join_tree = self.join_tree.as_ref().unwrap();

        // Recursively build the nested default structure based on the actual join tree
        fn build_default(tree: &JoinTree, query_object: &QueryObject) -> String {
            match tree {
                JoinTree::Leaf(stream_name) => {
                    let stream = query_object.get_stream(stream_name);
                    if stream.final_struct_name.len() > 1 {
                        format!(
                            "{}::default()",
                            stream
                                .final_struct_name
                                .get(stream.final_struct_name.len() - 2)
                                .unwrap()
                        )
                    } else {
                        format!("Struct_{}::default()", stream.source_table)
                    }
                }
                JoinTree::Join { left, right, .. } => {
                    let left_str = build_default(left, query_object);
                    let right_str = build_default(right, query_object);
                    format!("({}, {})", left_str, right_str)
                }
            }
        }

        build_default(join_tree, query_object)
    }
}

#[derive(Debug, Clone)]
pub enum JoinTree {
    Leaf(String), // Stream name
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
        join_type: JoinType,
    },
}

impl JoinTree {
    // Helper method to get all streams involved in this join tree
    pub fn get_involved_streams(&self) -> Vec<String> {
        match self {
            JoinTree::Leaf(stream) => vec![stream.clone()],
            JoinTree::Join { left, right, .. } => {
                let mut streams = left.get_involved_streams();
                streams.extend(right.get_involved_streams());
                streams
            }
        }
    }

    // Helper method to update access paths based on the join tree
    pub fn update_access_paths(&self, query_object: &mut QueryObject) {
        match self {
            JoinTree::Leaf(_) => {}
            JoinTree::Join { left, right, .. } => {
                // Update the access paths for this join
                let left_streams = left.get_involved_streams();
                let right_streams = right.get_involved_streams();

                // For left side - if it's a leaf append .0, if it's a join append .0.0 for left and .0.1 for right
                match &**left {
                    JoinTree::Leaf(_) => {
                        for stream in left_streams {
                            query_object
                                .get_mut_stream(&stream)
                                .access
                                .update_base_path(".0".to_string());
                        }
                    }
                    JoinTree::Join {
                        left: nested_left,
                        right: nested_right,
                        ..
                    } => {
                        // Get the streams from nested join
                        let nested_left_streams = nested_left.get_involved_streams();
                        let nested_right_streams = nested_right.get_involved_streams();

                        // Update nested left streams with .0.0
                        for stream in nested_left_streams {
                            query_object
                                .get_mut_stream(&stream)
                                .access
                                .update_base_path(".0.0".to_string());
                        }

                        // Update nested right streams with .0.1
                        for stream in nested_right_streams {
                            query_object
                                .get_mut_stream(&stream)
                                .access
                                .update_base_path(".0.1".to_string());
                        }
                    }
                }

                // For right side - always append .1
                for stream in right_streams {
                    query_object
                        .get_mut_stream(&stream)
                        .access
                        .update_base_path(".1".to_string());
                }
            }
        }
    }

    pub fn get_nesting_level(&self) -> usize {
        match self {
            JoinTree::Leaf(_) => 0,
            JoinTree::Join { left, right, .. } => {
                1 + left.get_nesting_level().max(right.get_nesting_level())
            }
        }
    }
}
