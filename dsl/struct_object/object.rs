use crate::dsl::ir::aqua::{
    ir_ast_structure::{AggregateType, JoinClause, SelectClause},
    literal::LiteralParser,
    AquaAST, ColumnRef,
};
use indexmap::IndexMap;

#[derive(Clone, Debug)]
pub struct Operation {
    pub input_column: String,
    pub table: String,
    pub current_op: String, 
    pub next_op: String,
}

#[derive(Clone, Debug)]
pub struct ResultColumn {
    pub name: String,
    pub r_type: String,
    pub operations: Vec<Operation>,
}
#[derive(Clone, Debug)]
pub struct QueryObject {
    pub has_join: bool, // true if the query has a join

    pub renoir_string: String, //Renoir final string

    pub output_path: String, //output path

    pub ir_ast: Option<AquaAST>,                  //ir ast
    pub joined_tables: Vec<String>,               // list of joined tables
    pub table_names_list: Vec<String>,            // list of table names

    pub table_to_alias: IndexMap<String, String>, // key: table name, value: alias
    pub table_to_csv: IndexMap<String, String>,   // key: table name, value: csv file path
    pub table_to_struct: IndexMap<String, IndexMap<String, String>>, // key: table name, value: IndexMap of column name and data type
    pub table_to_struct_name: IndexMap<String, String>, // key: table name, value: struct name
    pub table_to_tuple_access: IndexMap<String, String>, // key: table name, value: tuple field access

    //IndexMap to store the result column name and its corresponding ResultColumn struct
    pub result_column_to_input: IndexMap<String, ResultColumn>,
    // key: result column name, value: ResultColumn

    //ex. SELECT power * total_km AS product FROM table1
    //this indexMap will be filled with:
    //"product" -> ResultColumn { name: "product", r_type: "f64", operations: [Operation { input_column: "power", table: "table1", operation: "*" }, Operation { input_column: "total_km", table: "table1", operation: "" }] }

    //ex. SELECT SUM(total_km) AS total_distance FROM table1
    //this indexMap will be filled with:
    //"total_distance" -> ResultColumn { name: "total_distance", r_type: "f64", operations: [Operation { input_column: "total_km", table: "table1", operation: "SUM" }] }

    //ex. SELECT SUM(total_km) FROM table1
    //this indexMap will be filled with:
    //"total_km" -> ResultColumn { name: "total_km", r_type: "f64", operations: [Operation { input_column: "total_km", table: "table1", operation: "SUM" }] }
}

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
            result_column_to_input: IndexMap::new(),
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

    pub fn insert_result_col(
        &mut self,
        result_col: &str,
        result_type: &str,
        input: Vec<Operation>,
    ) {
        self.result_column_to_input.insert(
            result_col.to_string(),
            ResultColumn {
                name: result_col.to_string(),
                r_type: result_type.to_string(),
                operations: input,
            },
            );
    }

    pub fn populate(
        mut self,
        aqua_ast: &AquaAST,
        csv_paths: &Vec<String>,
        hash_maps: &Vec<IndexMap<String, String>>,
    ) -> Self {
        //insert the ir ast
        self.ir_ast = Some(aqua_ast.clone());
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
        //currently WIP

        /*
        for select_clause in &aqua_ast.select {
            match select_clause {
                SelectClause::Column(col_ref, _alias) => {
                    //case SELECT *
                    if col_ref.column == "*" {
                        if self.has_join {
                            let mut operations = Vec::new();
                            for table in &self.table_names_list {
                                let struct_map = self.get_struct(table).unwrap().clone();
                                // Use the alias if it exists, otherwise use the table name
                                let suffix = self.get_alias(table).unwrap_or(table).clone();
                                for (field_name, field_type) in struct_map {
                                    let result_col = format!("{}_{}", field_name, suffix);

                                    //insert the result column to input column mapping
                                    operations.push((
                                        result_col,
                                        field_type,
                                        vec![Operation {
                                            input_column: field_name.clone(),
                                            table: table.clone(),
                                            current_op: "".to_string(),
                                            next_op: "".to_string(),
                                        }]
                                    ));
                                }
                            }
                            // Insert all the columns from all tables
                            for (result_col, result_type, operation_chain) in operations {
                                self.insert_result_col(&result_col, &result_type, operation_chain);
                            }
                        } else {
                            let struct_map =
                                self.get_struct(&self.table_names_list[0]).unwrap().clone();
                            for (field_name, field_type) in struct_map {
                                let result_col = field_name.clone();

                                //insert the result column to input column mapping
                                self.insert_result_col(&result_col, &field_type, vec![Operation {
                                    input_column: field_name.clone(),
                                    table: self.table_names_list[0].clone(),
                                    current_op: "".to_string(),
                                    next_op: "".to_string(),
                                }],);
                            }
                        }
                    } 
                    // other
                    else {
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

                        //insert the result column to input column mapping
                        self.insert_result_col(&result_col, &result_type, vec![Operation {
                            input_column: input_col,
                            table: table,
                            current_op: "".to_string(),
                            next_op: "".to_string(),
                        }]);
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
                                    alias.clone().unwrap_or_else(|| "count(*)".to_string()),
                                    "*".to_string(),
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

                    let operation_chain = vec![Operation {
                        input_column: input_col.clone(),
                        table: table.clone(),
                        current_op: match agg_func.function {
                            AggregateType::Count => "count".to_string(),
                            AggregateType::Sum => "sum".to_string(),
                            AggregateType::Avg => "avg".to_string(),
                            AggregateType::Max => "max".to_string(),
                            AggregateType::Min => "min".to_string(),
                        },
                        next_op: "".to_string(),
                    }];

                    //insert the result column to input column mapping
                    self.insert_result_col(&result_col, &result_type, 
                        operation_chain);
                }
                SelectClause::ComplexValue(left_field, op, right_field, alias) => {
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

                    let operation_chain = vec![
                        Operation {
                            input_column: left_col,
                            table: left_table,
                            current_op: 
                            if left_field.aggregate.is_some() {
                                let fun = match left_field.aggregate.clone().unwrap().function{
                                    AggregateType::Count => "count",
                                    AggregateType::Sum => "sum",
                                    AggregateType::Avg => "avg",
                                    AggregateType::Max => "max",
                                    AggregateType::Min => "min",
                                };
                                format!("{}", fun )
                                } 
                                 else {
                                "".to_string()
                            },
                            next_op: op.to_string(), 
                        
                        },
                        Operation {
                            input_column: right_col,
                            table: right_table,
                            current_op: 
                            if right_field.aggregate.is_some() {
                                let fun = match right_field.aggregate.clone().unwrap().function{
                                    AggregateType::Count => "count",
                                    AggregateType::Sum => "sum",
                                    AggregateType::Avg => "avg",
                                    AggregateType::Max => "max",
                                    AggregateType::Min => "min",
                                };
                                format!("{}", fun )
                                } 
                                 else {
                                "".to_string()
                            },
                            next_op: "".to_string(),
                        },
                    ];

                    //insert the result column to input column mapping
                    self.insert_result_col(&result_col, &result_type, operation_chain);
                }
            }
        }*/

        self
    }
}
