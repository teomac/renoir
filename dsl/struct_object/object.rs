use crate::dsl::ir::{
    ir_ast_structure::{AggregateType, ComplexField, JoinClause, SelectColumn},
    ColumnRef, IrAST, IrLiteral,
};
use indexmap::IndexMap;

#[derive(Clone, Debug)]
pub struct QueryObject {
    pub has_join: bool, // true if the query has a join

    pub renoir_string: String, //Renoir final string

    pub output_path: String, //output path

    pub ir_ast: Option<IrAST>,         //ir ast
    pub joined_tables: Vec<String>,    // list of joined tables
    pub table_names_list: Vec<String>, // list of table names

    pub table_to_alias: IndexMap<String, String>, // key: table name, value: alias
    pub table_to_csv: IndexMap<String, String>,   // key: table name, value: csv file path
    pub table_to_struct: IndexMap<String, IndexMap<String, String>>, // key: table name, value: IndexMap of column name and data type
    pub table_to_struct_name: IndexMap<String, String>, // key: table name, value: struct name
    pub table_to_tuple_access: IndexMap<String, String>, // key: table name, value: tuple field access

    //IndexMap to store the result column name and its corresponding data type
    pub result_column_types: IndexMap<String, String>,
}
// key: result column name, value: data type

//ex. SELECT power * total_km AS product FROM table1
//this indexMap will be filled with:
//"product" -> f64 || i64

//ex. SELECT SUM(total_km) AS total_distance FROM table1
//this indexMap will be filled with:
//"total_distance" -> f64 || i64

//ex. SELECT SUM(total_km) FROM table1
//this indexMap will be filled with:
//"sum_total_km" -> f64 || i64

//ex. SELECT * FROM table1
//this indexMap will be filled with:
//all the columns from all the tables -> corresponding type

//ex. SELECT power, power FROM table1
//this indexMap will be filled with:
//"power" -> f64 || i64
//"power_1" -> f64 || i64

impl QueryObject {
    pub fn new() -> Self {
        QueryObject {
            has_join: false,
            joined_tables: Vec::new(),
            table_names_list: Vec::new(),
            table_to_alias: IndexMap::new(),
            table_to_csv: IndexMap::new(),
            table_to_struct: IndexMap::new(),
            table_to_struct_name: IndexMap::new(),
            table_to_tuple_access: IndexMap::new(),
            result_column_types: IndexMap::new(),
            renoir_string: String::new(),
            output_path: String::new(),
            ir_ast: None,
        }
    }

    pub fn set_output_path(&mut self, output_path: &str) {
        self.output_path = output_path.to_string();
    }

    pub fn get_alias(&self, table: &str) -> Option<&String> {
        self.table_to_alias.get(table)
    }

    pub fn get_table_from_alias(&self, alias: &str) -> Option<&String> {
        self.table_to_alias
            .iter()
            .find(|(_, v)| v == &alias)
            .map(|(k, _)| k)
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

    pub fn get_type(&self, column: &ColumnRef) -> String {
        let mut tab;
        match &column.table {
            Some(table) => tab = table.clone(),
            None => tab = self.get_all_table_names().first().unwrap().clone(),
        }

        let table_name = self.get_table_from_alias(&tab);

        match table_name {
            Some(name) => tab = name.clone(),
            None => {}
        }

        let field = &column.column;
        let str = if self.get_struct_field(&tab, field).is_none() {
            "f64".to_string()
        } else {
            self.get_struct_field(&tab, field).unwrap().to_string()
        };

        str
    }

    pub fn update_tuple_access(&mut self, map: &IndexMap<String, String>) {
        self.table_to_tuple_access = map.clone();
    }

    pub fn insert_result_col(&mut self, result_col: &str, result_type: &str) {
        self.result_column_types
            .insert(result_col.to_string(), result_type.to_string());
    }

    pub fn populate(
        mut self,
        ir_ast: &IrAST,
        csv_paths: &Vec<String>,
        hash_maps: &Vec<IndexMap<String, String>>,
    ) -> Self {
        //insert the ir ast
        self.ir_ast = Some(ir_ast.clone());
        let mut joins_vec: Vec<JoinClause> = Vec::new();

        // Check if query has join
        match &ir_ast.from.joins {
            Some(joins) => {
                self.has_join = true;
                joins_vec = joins.clone();
            }
            None => {
                self.has_join = false;
            }
        }

        // Add main table
        let main_table = ir_ast.from.scan.stream_name.clone();
        self.table_names_list.push(main_table.clone());

        if let Some(alias) = &ir_ast.from.scan.alias {
            self.table_to_alias
                .insert(main_table.clone(), alias.to_string());
        }

        // Add all joined tables
        for join in &joins_vec {
            let join_table = join.join_scan.stream_name.clone();

            //check if the table is already in the list
            if self.table_names_list.contains(&join_table) {
                panic!(
                    "Table {} is already in the list. Please use unique names.",
                    join_table
                );
            }

            //if it is not in the list, add it
            self.table_names_list.push(join_table.clone());
            self.joined_tables.push(join_table.clone());

            if let Some(join_alias) = &join.join_scan.alias {
                //check if the alias is already in the list
                for (_, alias) in &self.table_to_alias {
                    if alias == join_alias {
                        panic!(
                            "Alias {} is already in the list. Please use unique alias names.",
                            join_alias
                        );
                    }
                }

                //if it is not in the list, add it
                self.table_to_alias
                    .insert(join_table.clone(), join_alias.clone());
            }
        }

        // Collect all table names in order
        let mut table_names: Vec<String> = vec![main_table.clone()];
        for join in &joins_vec {
            table_names.push(join.join_scan.stream_name.clone());
        }

        // Process paths
        let paths: Vec<String> = csv_paths
            .iter()
            .map(|path| {
                std::env::current_dir()
                    .unwrap()
                    .join(path)
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();

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
            self.table_to_struct_name
                .insert(table.clone(), format!("StructVar{}", i));
        }

        // Populate the result column types based on select clauses
        if let Some(ref ir_ast) = self.ir_ast {
            let mut used_names = std::collections::HashSet::new();

            for select_clause in &ir_ast.select.select {
                match select_clause {
                    SelectColumn::Column(col_ref, alias) => {
                        // Handle SELECT * case
                        if col_ref.column == "*" {
                            // Check if there's a GROUP BY clause
                            if let Some(ref group_by) = ir_ast.group_by {
                                // If GROUP BY is present, only include the columns from the GROUP BY clause
                                for group_col in &group_by.columns {
                                    let table = if let Some(table_ref) = &group_col.table {
                                        // Use check_alias to properly retrieve the table name
                                        if self.has_join {
                                            // Import the check_alias function
                                            use crate::dsl::ir::r_utils::check_alias;
                                            check_alias(table_ref, &self)
                                        } else {
                                            self.table_names_list[0].to_string()
                                        }
                                    } else {
                                        // If table is not specified, use the first table
                                        self.table_names_list[0].to_string()
                                    };

                                    if let Some(struct_map) = self.table_to_struct.get(&table) {
                                        if let Some(col_type) = struct_map.get(&group_col.column) {
                                            let suffix =
                                                self.table_to_alias.get(&table).unwrap_or(&table);

                                            let full_col_name =
                                                format!("{}_{}", group_col.column, suffix);
                                            self.result_column_types
                                                .insert(full_col_name, col_type.clone());
                                        }
                                    }
                                }
                            } else {
                                // If no GROUP BY, include all columns from all tables (existing behavior)
                                for table_name in &self.table_names_list {
                                    if let Some(struct_map) = self.table_to_struct.get(table_name) {
                                        // Get the suffix (alias or table name)
                                        let suffix = self
                                            .table_to_alias
                                            .get(table_name)
                                            .unwrap_or(table_name);

                                        // Add each column with the appropriate suffix
                                        for (col_name, col_type) in struct_map {
                                            let full_col_name = format!("{}_{}", col_name, suffix);
                                            self.result_column_types
                                                .insert(full_col_name, col_type.clone());
                                        }
                                    }
                                }
                            }
                        } else {
                            //check if the column is valid
                            self.check_column_validity(col_ref, &String::new());

                            // Regular column selection
                            let col_name = alias.clone().unwrap_or_else(|| {
                                if self.has_join {
                                    // Add table suffix in join case
                                    let table = col_ref.table.as_ref().expect(
                                        "Column reference must have table name in JOIN query",
                                    );
                                    let suffix = self.table_to_alias.get(table).unwrap_or(table);
                                    format!("{}_{}", col_ref.column, suffix)
                                } else {
                                    col_ref.column.clone()
                                }
                            });

                            let col_name = self.get_unique_name(&col_name, &mut used_names);
                            let col_type = self.get_type(col_ref);
                            self.result_column_types.insert(col_name, col_type);
                        }
                    }

                    SelectColumn::Aggregate(agg_func, alias) => {
                        //check if the column is valid
                        if agg_func.column.column != "*" {
                            self.check_column_validity(&agg_func.column, &String::new());
                        }

                        let col_name = if let Some(alias_name) = alias {
                            self.get_unique_name(alias_name, &mut used_names)
                        } else {
                            // Generate name based on aggregate function
                            let base_name = match &agg_func.function {
                                AggregateType::Count => {
                                    if agg_func.column.column == "*" {
                                        "count_star".to_string()
                                    } else {
                                        // Add table suffix in join case
                                        if self.has_join {
                                            let table = agg_func.column.table.as_ref()
                                                .expect("Column reference must have table name in JOIN query");
                                            let suffix =
                                                self.table_to_alias.get(table).unwrap_or(table);
                                            format!("count_{}_{}", agg_func.column.column, suffix)
                                        } else {
                                            format!("count_{}", agg_func.column.column)
                                        }
                                    }
                                }
                                other_agg => {
                                    if self.has_join {
                                        let table = agg_func.column.table.as_ref().expect(
                                            "Column reference must have table name in JOIN query",
                                        );
                                        let suffix =
                                            self.table_to_alias.get(table).unwrap_or(table);
                                        format!(
                                            "{}_{}_{}",
                                            other_agg.to_string().to_lowercase(),
                                            agg_func.column.column,
                                            suffix
                                        )
                                    } else {
                                        format!(
                                            "{}_{}",
                                            other_agg.to_string().to_lowercase(),
                                            agg_func.column.column
                                        )
                                    }
                                }
                            };
                            self.get_unique_name(&base_name, &mut used_names)
                        };

                        let col_type = match agg_func.function {
                            AggregateType::Count => "usize".to_string(),
                            AggregateType::Avg => "f64".to_string(),
                            _ => self.get_type(&agg_func.column),
                        };

                        self.result_column_types.insert(col_name, col_type);
                    }

                    SelectColumn::ComplexValue(col_ref, alias) => {
                        let result_type = self.get_complex_field_type(col_ref);
                        let col_name = if let Some(alias_name) = alias {
                            self.get_unique_name(alias_name, &mut used_names)
                        } else {
                            if self.has_join {
                                // Try to construct a meaningful name from the complex expression
                                let base_name = if let Some(ref col) = col_ref.column_ref {
                                    let table = col.table.as_ref().expect(
                                        "Column reference must have table name in JOIN query",
                                    );
                                    let suffix = self.table_to_alias.get(table).unwrap_or(table);
                                    format!("expr_{}_{}", col.column, suffix)
                                } else {
                                    format!("expr_{}", used_names.len())
                                };
                                self.get_unique_name(&base_name, &mut used_names)
                            } else {
                                let base_name = format!("expr_{}", used_names.len());
                                self.get_unique_name(&base_name, &mut used_names)
                            }
                        };

                        self.result_column_types.insert(col_name, result_type);
                    }
                }
            }
        }

        self
    }

    // Helper method to generate unique column names
    fn get_unique_name(
        &self,
        base_name: &str,
        used_names: &mut std::collections::HashSet<String>,
    ) -> String {
        let mut name = base_name.to_string();
        let mut counter = 1;

        while used_names.contains(&name) {
            name = format!("{}_{}", base_name, counter);
            counter += 1;
        }

        used_names.insert(name.clone());
        name
    }

    pub fn get_complex_field_type(&self, field: &ComplexField) -> String {
        if let Some(ref col) = field.column_ref {
            //check if the column is valid
            self.check_column_validity(col, &String::new());
            self.get_type(col)
        } else if let Some(ref lit) = field.literal {
            match lit {
                IrLiteral::Integer(_) => "i64".to_string(),
                IrLiteral::Float(_) => "f64".to_string(),
                IrLiteral::String(_) => "String".to_string(),
                IrLiteral::Boolean(_) => "bool".to_string(),
                IrLiteral::ColumnRef(col) => self.get_type(col),
            }
        } else if let Some(ref nested) = field.nested_expr {
            let (left, op, right) = &**nested;
            let left_type = self.get_complex_field_type(left);
            let right_type = self.get_complex_field_type(right);

            // If either operand is f64 or operation is division, result is f64
            if left_type == "f64" || right_type == "f64" || op == "/" {
                "f64".to_string()
            } else {
                left_type
            }
        } else if let Some(ref agg) = field.aggregate {
            //check if the column is valid
            if &agg.column.column != "*" {
            self.check_column_validity(&agg.column, &String::new());}
            match agg.function {
                AggregateType::Count => "usize".to_string(),
                AggregateType::Avg => "f64".to_string(),
                _ => self.get_type(&agg.column),
            }
        } else {
            panic!("Invalid complex field - no valid content")
        }
    }

    pub fn check_column_validity(&self, col_ref: &ColumnRef, known_table: &String) {
        //check if the col ref corresponds to a real column
        let col_to_check = col_ref.column.clone();
        if col_ref.table.is_some() {
            let mut table = col_ref.table.as_ref().unwrap();

            //check if the table is an alias. If it is, get the real table name
            if self.get_table_from_alias(&table).is_some() {
                table = self.get_table_from_alias(&table).unwrap();
            }

            //get the struct map for the table
            let struct_map = self.table_to_struct.get(table).unwrap_or_else(|| {
                panic!("Error in retrieving struct_map for table {}.", table);
            });
            if !struct_map.contains_key(&col_to_check) {
                panic!("Column {} does not exist in table {}", col_to_check, table);
            }
        } else {
            let mut found = false;
            if !known_table.is_empty() {
                let struct_map = self.table_to_struct.get(known_table).unwrap();
                if struct_map.contains_key(&col_to_check) {
                    found = true;
                }
            } else {
                for table in &self.table_names_list {
                    let struct_map = self.table_to_struct.get(table).unwrap();
                    if struct_map.contains_key(&col_to_check) {
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                panic!("Column {} does not exist in any table", col_to_check);
            }
        }
    }
}
