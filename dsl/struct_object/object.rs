use crate::dsl::ir::aqua::{
    ast_structure::{AggregateType, ComplexField, JoinClause, SelectClause},
    literal::LiteralParser,
    AquaAST, ColumnRef,
};
use indexmap::IndexMap;

#[derive(Clone)]
pub struct QueryObject {
    pub has_join: bool,                           // true if the query has a join
    pub joined_tables: Vec<String>,               // list of joined tables
    pub table_names_list: Vec<String>,            // list of table names
    pub projections: Vec<(ComplexField, String)>, // list of projections (column reference, operation)

    pub table_to_alias: IndexMap<String, String>, // key: table name, value: alias
    pub table_to_csv: IndexMap<String, String>,   // key: table name, value: csv file path
    pub table_to_struct: IndexMap<String, IndexMap<String, String>>, // key: table name, value: IndexMap of column name and data type
    pub table_to_struct_name: IndexMap<String, String>, // key: table name, value: struct name
    pub table_to_tuple_access: IndexMap<String, String>, // key: table name, value: tuple field access

    //IndexMap to store the result column name and its corresponding type, input column and table name
    pub result_column_to_input: IndexMap<String, (String, String, String)>,
    // key: result column name, value: tuple (result type, input column, table name)

    //ex. SELECT power * total_km AS product FROM table1
    //this indexMap will be filled with:
    //"product" -> ("f64", "power, "table1")

    //ex. SELECT SUM(total_km) AS total_distance FROM table1
    //this indexMap will be filled with:
    //"total_distance" -> ("f64", "total_km", "table1")

    //ex. SELECT SUM(total_km) FROM table1
    //this indexMap will be filled with:
    //"total_km" -> ("f64", "total_km", "table1")

    // Renoir final string
    pub renoir_string: String,
    //output path
    pub output_path: String,
}

// vehicle_count, (u64, *, table1)
// total_distance, (f64, total_km, table1)

impl QueryObject {
    pub fn new() -> Self {
        QueryObject {
            has_join: false,
            joined_tables: Vec::new(),
            table_names_list: Vec::new(),
            projections: Vec::new(),

            table_to_alias: IndexMap::new(),
            table_to_csv: IndexMap::new(),
            table_to_struct: IndexMap::new(),
            table_to_struct_name: IndexMap::new(),
            table_to_tuple_access: IndexMap::new(),
            result_column_to_input: IndexMap::new(),
            renoir_string: String::new(),
            output_path: String::new(),
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

    pub fn insert_result_col(
        &mut self,
        result_col: &str,
        result_type: &str,
        input_col: &str,
        table: &str,
    ) {
        self.result_column_to_input.insert(
            result_col.to_string(),
            (
                result_type.to_string(),
                input_col.to_string(),
                table.to_string(),
            ),
        );
    }

    pub fn insert_projection(&mut self, field: &ComplexField, agg_type: &str) {
        self.projections.push((field.clone(), agg_type.to_string()));
    }

    pub fn populate(
        mut self,
        aqua_ast: &AquaAST,
        csv_paths: &Vec<String>,
        hash_maps: &Vec<IndexMap<String, String>>,
    ) -> Self {
        let mut joins_vec: Vec<JoinClause> = Vec::new();

        // Check if query has join
        match &aqua_ast.from.joins {
            Some(joins) => {
                self.has_join = true;
                joins_vec = joins.clone();
            }
            None => {
                self.has_join = false;
            }
        }

        // Add main table
        let main_table = aqua_ast.from.scan.stream_name.clone();
        self.table_names_list.push(main_table.clone());

        if let Some(alias) = &aqua_ast.from.scan.alias {
            self.table_to_alias
                .insert(main_table.clone(), alias.to_string());
        }

        // Add all joined tables
        for join in &joins_vec {
            let join_table = join.scan.stream_name.clone();
            self.joined_tables.push(join_table.clone());
            self.table_names_list.push(join_table.clone());
            if let Some(join_alias) = &join.scan.alias {
                self.table_to_alias
                    .insert(join_table.clone(), join_alias.clone());
            }
        }

        // Collect all table names in order
        let mut table_names = vec![main_table.clone()];
        for join in &joins_vec {
            table_names.push(join.scan.stream_name.clone());
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

        //println!("table to struct name: {:?}", self.table_to_struct_name);

        //populate the result column to input column mapping

        for select_clause in &aqua_ast.select {
            match select_clause {
                SelectClause::Column(col_ref, alias) => {
                    if col_ref.column == "*" {
                        if self.has_join {
                            for table in &self.table_names_list {
                                let struct_map = self.get_struct(table).unwrap().clone();
                                // Use the alias if it exists, otherwise use the table name
                                let suffix = self.get_alias(table).unwrap_or(table).clone();
                                for (field_name, field_type) in struct_map {
                                    let result_col = format!("{}_{}", field_name, suffix);
                                    self.result_column_to_input
                                        .insert(result_col, (field_type, field_name, table.clone()));
                                }
                            }
                        } else {
                            let struct_map =
                                self.get_struct(&self.table_names_list[0]).unwrap().clone();
                            for (field_name, field_type) in struct_map {
                                let result_col = field_name.clone();
                                self.result_column_to_input.insert(
                                    result_col,
                                    (
                                        field_type.clone(),
                                        field_name.clone(),
                                        self.table_names_list[0].clone(),
                                    ),
                                );
                            }
                        }
                    } else {
                        let result_col = if self.has_join {
                            let table = match &col_ref.table {
                                Some(t) => {
                                    // If the table name is actually an alias, use it directly
                                    if let Some(_) = self.get_table_from_alias(t) {
                                        t.clone()
                                    } else {
                                        // If it's a table name, get its alias if it exists, otherwise use table name
                                        self.get_alias(t).unwrap_or(t).clone()
                                    }
                                }
                                None => self
                                    .get_alias(&self.table_names_list[0])
                                    .unwrap_or(&self.table_names_list[0])
                                    .clone(),
                            };
                            format!("{}_{}", col_ref.column, table)
                        } else {
                            col_ref.column.clone()
                        };
                        let input_col = col_ref.column.clone();
                        let table = match &col_ref.table {
                            Some(t) => {
                                if let Some(actual_table) = self.get_table_from_alias(t) {
                                    actual_table.clone()
                                } else {
                                    t.clone()
                                }
                            }
                            None => self.table_names_list[0].clone(),
                        };
                        let result_type = self.get_type(&col_ref);
                        self.result_column_to_input
                            .insert(result_col, (result_type, input_col, table));
                    }
                }
                SelectClause::Aggregate(agg_func, alias) => {
                    let (result_col, input_col) =
                        match (&agg_func.function, &agg_func.column.column) {
                            (AggregateType::Count, s) if s == "*" => {
                                // For COUNT(*), use the first column from the table's struct
                                let table = match &agg_func.column.table {
                                    Some(t) => {
                                        if let Some(actual_table) = self.get_table_from_alias(t) {
                                            actual_table.clone()
                                        } else {
                                            t.clone()
                                        }
                                    }
                                    None => self.table_names_list[0].clone(),
                                };
                                let first_col = self
                                    .get_struct(&table)
                                    .unwrap()
                                    .keys()
                                    .next()
                                    .unwrap()
                                    .clone();
                                (
                                    alias.clone().unwrap_or_else(|| "count".to_string()),
                                    first_col,
                                )
                            }
                            // For COUNT(column), use the column name as the result column
                            _ => (
                                alias
                                    .clone()
                                    .unwrap_or_else(|| agg_func.column.column.clone()),
                                agg_func.column.column.clone(),
                            ),
                        };

                    // this is the case of MAX(), MIN(), SUM(), AVG()
                    let table = match &agg_func.column.table {
                        Some(t) => {
                            if let Some(actual_table) = self.get_table_from_alias(t) {
                                actual_table.clone()
                            } else {
                                t.clone()
                            }
                        }
                        None => self.table_names_list[0].clone(),
                    };

                    // For aggregates, we always use usize for COUNT, use f64 for AVG and respect original type for others
                    let result_type = if matches!(agg_func.function, AggregateType::Count) {
                        "usize".to_string()
                    } else if matches!(agg_func.function, AggregateType::Avg) {
                        "f64".to_string()
                    } else {
                        self.get_type(&agg_func.column)
                    };
                    self.result_column_to_input
                        .insert(result_col, (result_type, input_col, table));
                }
                SelectClause::ComplexValue(left_field, _op, right_field, alias) => {
                    //parse left field to check if it is a ColumnRef or a Literal or an aggregate expr
                    let mut left_is_literal = false;
                    let mut right_is_literal = false;

                    let mut left_col = String::new();
                    let mut left_table = String::new();
                    let mut right_col = String::new();
                    let mut right_table = String::new();

                    let mut left_type = String::new();
                    let mut right_type = String::new();
                    let result_type;

                    //check left field type
                    if left_field.column.is_some() {
                        left_col = left_field.column.clone().unwrap().column;
                        if left_field.column.clone().unwrap().table.is_some() {
                            left_table = left_field.column.clone().unwrap().table.clone().unwrap();
                        }
                        left_type = self.get_type(&left_field.column.clone().unwrap());
                    } else if left_field.literal.is_some() {
                        left_is_literal = true;
                        left_type =
                            LiteralParser::get_literal_type(&left_field.literal.clone().unwrap());
                    } else if left_field.aggregate.is_some() {
                        left_col = left_field.aggregate.clone().unwrap().column.column;
                        if left_field.aggregate.clone().unwrap().column.table.is_some() {
                            left_table = left_field
                                .aggregate
                                .clone()
                                .unwrap()
                                .column
                                .table
                                .clone()
                                .unwrap();
                        }
                        left_type = self.get_type(&left_field.aggregate.clone().unwrap().column);
                    }

                    //check right field type
                    if right_field.column.is_some() {
                        right_col = right_field.column.clone().unwrap().column;
                        if right_field.column.clone().unwrap().table.is_some() {
                            right_table =
                                right_field.column.clone().unwrap().table.clone().unwrap();
                        }
                        right_type = self.get_type(&right_field.column.clone().unwrap());
                    } else if right_field.literal.is_some() {
                        right_is_literal = true;
                        right_type =
                            LiteralParser::get_literal_type(&right_field.literal.clone().unwrap());
                    } else if right_field.aggregate.is_some() {
                        right_col = right_field.aggregate.clone().unwrap().column.column;
                        if right_field
                            .aggregate
                            .clone()
                            .unwrap()
                            .column
                            .table
                            .is_some()
                        {
                            right_table = right_field
                                .aggregate
                                .clone()
                                .unwrap()
                                .column
                                .table
                                .clone()
                                .unwrap();
                        }
                        right_type = self.get_type(&right_field.aggregate.clone().unwrap().column);
                    }

                    //safety check
                    if left_is_literal && right_is_literal {
                        panic!(
                            "Literal functions as both field of the expression are not supported"
                        );
                    }

                    //safety check on types
                    if left_type != right_type {
                        panic!("Type mismatch in expression");
                    }

                    //set result type
                    result_type = left_type.clone();

                    //set result column
                    let result_col = alias.clone().unwrap_or_else(|| {
                        if !left_is_literal {
                            left_col.clone()
                        } else {
                            right_col.clone()
                        }
                    });
                    let input_col = if !left_is_literal {
                        left_col.clone()
                    } else {
                        right_col.clone()
                    };
                    let table;
                    if !left_is_literal {
                        table = if !left_table.is_empty() {
                            if let Some(actual_table) = self.get_table_from_alias(&left_table) {
                                actual_table.clone()
                            } else {
                                left_table.clone()
                            }
                        } else {
                            self.table_names_list[0].clone()
                        };
                    } else {
                        table = if !right_table.is_empty() {
                            if let Some(actual_table) = self.get_table_from_alias(&right_table) {
                                actual_table.clone()
                            } else {
                                right_table.clone()
                            }
                        } else {
                            self.table_names_list[0].clone()
                        };
                    }

                    self.result_column_to_input
                        .insert(result_col, (result_type, input_col, table));
                }
            }
        }

        println!("result column to input: {:?}", self.result_column_to_input);

        self
    }
}
