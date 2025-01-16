use std::{collections::HashMap, hash::Hash};

use crate::dsl::ir::aqua::AquaAST;

pub struct query_object {
    pub has_join: bool, // true if the query has a join
    pub table_to_alias: HashMap<String, String>,    // key: table name, value: alias
    pub table_to_csv: HashMap<String, String>,  // key: table name, value: csv file path
    pub table_to_struct: HashMap<String, HashMap<String, String>>,  // key: table name, value: HashMap of column name and data type
    pub table_to_struct_name: HashMap<String, String>,   // key: table name, value: struct name
}

impl query_object {

    pub fn new() -> Self {
        query_object {
            has_join: false,
            table_to_alias: HashMap::new(),
            table_to_csv: HashMap::new(),
            table_to_struct: HashMap::new(),
            table_to_struct_name: HashMap::new(),
        }
    }

    pub fn get_alias(&self, table: &str) -> Option<&String> {
        self.table_to_alias.get(table)
    }

    pub fn get_csv(&self, table: &str) -> Option<&String> {
        self.table_to_csv.get(table)
    }

    pub fn get_struct(&self, table: &str) -> Option<&HashMap<String, String>> {
        self.table_to_struct.get(table)
    }

    pub fn get_struct_field(&self, table: &str, field: &str) -> Option<&String> {
        self.table_to_struct.get(table).and_then(|s| s.get(field))
    }

    pub fn get_struct_name(&self, table: &str) -> Option<&String> {
        self.table_to_struct_name.get(table)
    }

    pub fn populate(mut self, aqua_ast: AquaAST, csv_paths: Vec<String>, hash_maps: Vec<HashMap<String, String>>) -> Self {
        // Set has_join based on join condition in AST
        self.has_join = aqua_ast.from.join.is_some();

        // Get main table name from scan clause
        let main_table = aqua_ast.from.scan.stream_name.clone();
        
        // Add main table alias if present
        if let Some(alias) = aqua_ast.from.scan.alias {
            self.table_to_alias.insert(main_table.clone(), alias);
        }

        // Add joined table alias if present
        if let Some(join) = &aqua_ast.from.join {
            let join_table = join.scan.stream_name.clone();
            if let Some(join_alias) = &join.scan.alias {
                self.table_to_alias.insert(join_table.clone(), join_alias.clone());
            }
        }

        // Initialize a vector of table names
        let mut table_names = vec![main_table.clone()];
        if let Some(join) = &aqua_ast.from.join {
            table_names.push(join.scan.stream_name.clone());
        }

        // Zip table names with CSV paths
        for (table, path) in table_names.iter().zip(csv_paths.iter()) {
            self.table_to_csv.insert(table.clone(), path.clone());
        }

        for (table, hash_map) in table_names.iter().zip(hash_maps.iter()) {
            self.table_to_struct.insert(table.clone(), hash_map.clone());
        }

        for (i, table) in table_names.iter().enumerate() {
            let struct_name = format!("Struct_var_{}", i);
            self.table_to_struct_name.insert(table.clone(), struct_name);
        }

        self
    }

}