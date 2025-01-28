use indexmap::IndexMap;

use crate::dsl::ir::aqua::{ast_structure::JoinClause, AquaAST, ColumnRef};

#[derive(Clone)]
pub struct QueryObject {
    pub has_join: bool, // true if the query has a join
    pub joined_tables: Vec<String>, // list of joined tables
    pub table_names_list: Vec<String>, // list of table names
    pub field_lists: Vec<Vec<(String, String)>>, // list of field lists (eg. [("int1", "i64"), ("float1", "f64")]

    pub table_to_alias: IndexMap<String, String>,    // key: table name, value: alias
    pub table_to_csv: IndexMap<String, String>,  // key: table name, value: csv file path
    pub table_to_struct: IndexMap<String, IndexMap<String, String>>,  // key: table name, value: HashMap of column name and data type 
    pub table_to_struct_name: IndexMap<String, String>,   // key: table name, value: struct name
    pub table_to_tuple_access: IndexMap<String, String>, // key: table name, value: tuple field access
    pub renoir_string: String, // renoir final string
}

impl QueryObject {

    pub fn new() -> Self {
        QueryObject {
            has_join: false,
            joined_tables: Vec::new(),
            table_names_list: Vec::new(),
            field_lists: Vec::new(),

            table_to_alias: IndexMap::new(),
            table_to_csv: IndexMap::new(),
            table_to_struct: IndexMap::new(),
            table_to_struct_name: IndexMap::new(),
            table_to_tuple_access: IndexMap::new(),
            renoir_string: String::new(),
        }
    }

    pub fn get_alias(&self, table: &str) -> Option<&String> {
        self.table_to_alias.get(table)
    }

    pub fn get_csv(&self, table: &str) -> Option<&String> {
        self.table_to_csv.get(table)
    }

    pub fn get_struct(&self, table: &str) -> Option<&IndexMap<String, String>> {
        self.table_to_struct.get(table)
    }

    pub fn get_struct_field(&self, table: &str, field: &str) -> Option<&String> {
        self.table_to_struct.get(table).and_then(|s| s.get(field))
    }

    pub fn get_struct_name(&self, table: &str) -> Option<&String> {
        self.table_to_struct_name.get(table)
    }

    pub fn set_renoir_string(&mut self, renoir_string: &String) {
        self.renoir_string = renoir_string.to_string();
    }

    pub fn get_all_structs(&self) -> Vec<String> {
        self.table_to_struct_name.values().cloned().collect()
    }

    pub fn get_all_table_names(&self) -> Vec<String> {
        self.table_to_csv.keys().cloned().collect()
    }

    pub fn add_field_list(&mut self, field_list: Vec<(String, String)>) {
        self.field_lists.push(field_list);
    }

    pub fn get_type(&self, column: &ColumnRef) -> String {
        let tab;
        match &column.table {
            Some(table) => tab = table.clone(),
            None => tab = self.get_all_table_names().first().unwrap().clone(),
        }

        let field = &column.column;
        let str = self.get_struct_field(&tab, field).unwrap().to_string();

        if str.contains("int") {
            "i64".to_string()
        } else if str.contains("float") {
            "f64".to_string()
        } else if str.contains("bool") {
            "bool".to_string()
        } else {
            "String".to_string()
        }

    }

    pub fn update_tuple_access(&mut self, map: &IndexMap<String, String>) {
        self.table_to_tuple_access = map.clone();
    }

    pub fn populate(mut self, aqua_ast: &AquaAST, csv_paths: &Vec<String>, hash_maps: &Vec<IndexMap<String, String>>) -> Self {
        let mut joins_vec: Vec<JoinClause> = Vec::new();

        // Check if query has join
        match &aqua_ast.from.joins {
            Some(joins) => {
                self.has_join = true;
                joins_vec = joins.clone();

            },
            None => {
                self.has_join = false;
            }
        }
    
        // Add main table
        let main_table = aqua_ast.from.scan.stream_name.clone();
        self.table_names_list.push(main_table.clone());
        
        if let Some(alias) = &aqua_ast.from.scan.alias {
            self.table_to_alias.insert(main_table.clone(), alias.to_string());
        }
    
        // Add all joined tables
        for join in &joins_vec {
            let join_table = join.scan.stream_name.clone();
            self.joined_tables.push(join_table.clone());
            self.table_names_list.push(join_table.clone());
            if let Some(join_alias) = &join.scan.alias {
                self.table_to_alias.insert(join_table.clone(), join_alias.clone());
            }
        }
    
        // Collect all table names in order
        let mut table_names = vec![main_table.clone()];
        for join in &joins_vec {
            table_names.push(join.scan.stream_name.clone());
        }
    
        // Process paths
        let paths: Vec<String> = csv_paths.iter().map(|path| {
            std::env::current_dir()
                .unwrap()
                .join(path)
                .to_string_lossy()
                .replace('\\', "/")
        }).collect();
    
        // Validate input counts
        assert_eq!(
            table_names.len(), 
            paths.len(), 
            "Number of tables ({}) and CSV paths ({}) must match", 
            table_names.len(), 
            paths.len()
        );
        assert_eq!(
            table_names.len(), 
            hash_maps.len(), 
            "Number of tables ({}) and hash maps ({}) must match",
            table_names.len(), 
            hash_maps.len()
        );
    
        // Set up mappings for each table
        for i in 0..table_names.len() {
            let table = &table_names[i];
            let path = &paths[i];
            let hash_map = &hash_maps[i];
            
            self.table_to_csv.insert(table.clone(), path.clone());
            self.table_to_struct.insert(table.clone(), hash_map.clone());
            self.table_to_struct_name.insert(table.clone(), format!("StructVar{}", i));
        }
    
        println!("table to struct name: {:?}", self.table_to_struct_name);
        self
    }
}