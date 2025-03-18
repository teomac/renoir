use super::support_structs::StreamInfo;
use crate::dsl::ir::{
    ir_ast_structure::{AggregateType, ComplexField},
    ColumnRef, IrLiteral, IrPlan, ProjectionColumn,
};
use crate::dsl::struct_object::utils::check_column_validity;
use core::panic;
use indexmap::IndexMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct QueryObject {
    // Tables references
    pub tables_info: IndexMap<String, IndexMap<String, String>>, // key: table name, value: IndexMap of column name and data type

    pub table_to_csv: IndexMap<String, String>, // key: table name, value: csv file path

    pub table_to_struct_name: IndexMap<String, String>, // key: tuple (table name, alias), value: struct name

    pub alias_to_stream: IndexMap<String, String>, // key: alias, value: stream name. Please note that, if the table has no alias and the query has multiple tables, the alias will be the table name.

    pub streams: IndexMap<String, StreamInfo>, // key: stream name, value: StreamInfo

    pub has_join: bool,              // true if the query has a join
    pub output_path: String,         //output path
    pub ir_ast: Option<Arc<IrPlan>>, //ir ast
    pub result_column_types: IndexMap<String, String>, // key: result column name, value: data type

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
    pub order_by_string: String, //order by string
    pub limit_string: String,    //limit string
    pub distinct_string: String, //distinct string

    pub projection_agg: Vec<ProjectionColumn>, //projection aggregates.
                                               //Here we store ONLY the aggregates in the final projection, that we will need to generate the fold in case of a group by
}

impl Default for QueryObject {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryObject {
    pub fn new() -> Self {
        QueryObject {
            streams: IndexMap::new(),
            alias_to_stream: IndexMap::new(),
            has_join: false,
            tables_info: IndexMap::new(),
            table_to_csv: IndexMap::new(),
            table_to_struct_name: IndexMap::new(),
            result_column_types: IndexMap::new(),
            output_path: String::new(),
            ir_ast: None,
            order_by_string: String::new(),
            limit_string: String::new(),
            projection_agg: Vec::new(),
            distinct_string: String::new(),
        }
    }

    //getter and setter methods for tables_info
    pub fn set_tables_info(&mut self, tables_info: IndexMap<String, IndexMap<String, String>>) {
        self.tables_info = tables_info;
    }

    //method to create a new stream
    pub fn create_new_stream(
        &mut self,
        stream_name: &String,
        source_table: &String,
        alias: &str,
    ) {
        //create the StreamInfo object
        let mut stream = StreamInfo::new(stream_name.clone(), source_table.clone(), alias.to_owned());

        //check if the stream already exists
        if self.check_stream(&stream) {
            panic!("Stream {} already exists.", stream_name);
        }

        if !alias.is_empty() {
            self.alias_to_stream
                .insert(alias.to_owned(), stream_name.clone());
        }

        stream.update_columns(self.tables_info.get(source_table).unwrap().clone());

        self.streams.insert(stream_name.clone(), stream);
    }

    //method to insert a new stream operator in the chain
    pub fn insert_stream_op_chain(&mut self, stream_name: &String, op: &str) {
        self.streams
            .get_mut(stream_name)
            .unwrap()
            .insert_op(op.to_owned());
    }

    //method to check the validity of an alias
    pub fn is_alias_valid(&self, alias: &String) -> bool {
        //first check if the alias is already in the list of aliases
        if self.alias_to_stream.get(alias).is_some() {
            return false
        }

        true
    }

    //setter method for output_path
    pub fn set_output_path(&mut self, output_path: &str) {
        self.output_path = output_path.to_string();
    }

    //setter method for table_to_csv
    pub fn set_table_to_csv(&mut self, table_to_csv: IndexMap<String, String>) {
        self.table_to_csv = table_to_csv;
    }

    // setter for ir_ast
    pub fn set_ir_ast(&mut self, ir_ast: &Arc<IrPlan>) {
        self.ir_ast = Some(ir_ast.clone());
    }

    //getter for result_column_types
    pub fn get_result_column_types(&self) -> &IndexMap<String, String> {
        &self.result_column_types
    }

    //getter for single stream
    pub fn get_stream(&self, stream_name: &String) -> &StreamInfo {
        self.streams
            .get(stream_name)
            .unwrap_or_else(|| panic!("Stream {} does not exist.", stream_name))
    }

    //getter for single stream mutable
    pub fn get_mut_stream(&mut self, stream_name: &String) -> &mut StreamInfo {
        self.streams
            .get_mut(stream_name)
            .unwrap_or_else(|| panic!("Stream {} does not exist.", stream_name))
    }

    //method to check if a stream already exists
    pub fn check_stream(&self, stream: &StreamInfo) -> bool {
        let mut exists = false;

        for (_, s) in self.streams.iter() {
            if s.equals(stream) || s.source_equals(stream) {
                exists = true;
                break;
            }
        }

        exists
    }

    //get stream from alias
    pub fn get_stream_from_alias(&self, alias: &str) -> Option<&String> {
        self.alias_to_stream.get(alias)
    }

    //method to insert final result columns types
    pub fn insert_final_result_col(&mut self, result_col: &str, result_type: &str) {
        self.result_column_types
            .insert(result_col.to_string(), result_type.to_string());
    }

    //get csv from table
    pub fn get_csv(&self, table: &str) -> Option<&String> {
        self.table_to_csv.get(table)
    }

    //get struct from table
    pub fn get_struct(&self, table: &str) -> Option<&IndexMap<String, String>> {
        self.tables_info.get(table)
    }

    //get field from struct
    pub fn get_struct_field(&self, table: &str, field: &str) -> Option<&String> {
        self.tables_info.get(table).and_then(|s| s.get(field))
    }

    //get struct name from table
    pub fn get_struct_name(&self, table: &str) -> Option<&String> {
        self.table_to_struct_name.get(&(table.to_string()))
    }

    //method to get all the structs
    pub fn get_all_structs(&self) -> Vec<String> {
        self.table_to_struct_name.values().cloned().collect()
    }

    //method to get all the table names
    pub fn get_all_table_names(&self) -> Vec<String> {
        self.tables_info.keys().cloned().collect()
    }

    //method to get the type of a column ref
    pub fn get_type(&self, column: &ColumnRef) -> String {
        let stream_name: String = if column.table.is_some() {
            self.get_stream_from_alias(column.table.as_ref().unwrap())
                .unwrap()
                .clone()
        } else {
            let all_streams = self.streams.keys().cloned().collect::<Vec<_>>();
            if all_streams.len() == 1 {
                all_streams[0].clone()
            } else {
                panic!("Column reference must have table name in JOIN query");
            }
        };

        let table_name = self.get_stream(&stream_name).source_table.clone();

        let field = &column.column;
        let str = if self.get_struct_field(&table_name, field).is_none() {
            "f64".to_string()
        } else {
            self.get_struct_field(&table_name, field)
                .unwrap()
                .to_string()
        };

        str
    }

    //method to insert the result column and its type in the result_column_types
    pub fn insert_result_col(&mut self, result_col: &str, result_type: &str) {
        self.result_column_types
            .insert(result_col.to_string(), result_type.to_string());
    }

    //method to populate the QueryObject with the necessary information
    pub fn populate(mut self, ir_ast: &Arc<IrPlan>) -> Self {
        self.set_ir_ast(ir_ast);
        // Collect all Scan and Join nodes
        let mut scans = Self::collect_scan_nodes(ir_ast);
        scans.reverse();

        //////////////////////////////
        // main table focus

        // Add main table
        let main_scan: &Arc<IrPlan> = scans.first().unwrap();

        let (main_table_arc, main_stream, main_alias) = match &**main_scan {
            IrPlan::Scan {
                input: input_source,
                stream_name,
                alias,
            } => (input_source, stream_name, alias),
            _ => panic!("Error: this is not a scan node"),
        };


        //check if main table is a table name or a subquery
        let main_table = match &**main_table_arc {
            IrPlan::Table { table_name } => {
                table_name.clone()
            }
            _ => panic!("Main table is not a table name."),
        };

        //check if the table is present in the list
        if self.tables_info.get(&main_table).is_none() {
            panic!("Table {} is not present in the list of tables.", main_table);
        }

        //create the first stream
        self.create_new_stream(
            main_stream,
            &main_table,
            &main_alias.clone().unwrap_or(String::new()),
        );

        //////////////////////////////////////////////

        //now let's start processing the joins
        for scan in scans.iter().skip(1) {
            let (join_table_arc, join_stream, join_alias) = match &**scan {
                IrPlan::Scan {
                    input: input_source,
                    stream_name,
                    alias,
                } => (input_source, stream_name, alias),
                _ => panic!("Error: this is not a scan node"),
            };

            //check if main table is a table name or a subquery
            let join_table = match &**join_table_arc {
                IrPlan::Table { table_name } => {
                    table_name.clone()
                }
                _ => panic!("Main table is not a table name."),
            };


            //check if the table is in the tables_info
            if self.tables_info.get(&join_table).is_none() {
                panic!("Table {} is not present in the list of tables.", &join_table);
            }

            //create the stream
            self.create_new_stream(
                join_stream,
                &join_table,
                &join_alias
                    .clone()
                    .unwrap_or_else(|| panic!("Alias not found for table {}", &join_table)),
            );
        }

        //////////////////////////////////////////////
        //manipulate the tables_info object.
        //if a table is not the main one and is not in the joined_tables, remove it from the tables_info object.
        let tables_info_keys = self.get_all_table_names();
        let mut temp_tables_info = self.tables_info.clone();

        //now collect in a vec all the table_names from the streams
        let stream_tables = self
            .streams
            .values()
            .map(|stream| stream.source_table.clone())
            .collect::<Vec<_>>();

        for table in tables_info_keys.iter() {
            //if the table is not in the stream_tables, remove it from the tables_info object
            if table != &main_table && !stream_tables.contains(table) {
                temp_tables_info.shift_remove(table);
            }
        }

        //we also update the table_to_csv object
        let mut temp_table_to_csv = self.table_to_csv.clone();
        for (table, _) in self.table_to_csv.iter() {
            if !stream_tables.contains(table) {
                temp_table_to_csv.shift_remove(table);
            }
        }

        //now we update the table_to_csv object and the tables_info object
        self.set_table_to_csv(temp_table_to_csv.clone());
        self.set_tables_info(temp_tables_info.clone());

        let all_tables = self.get_all_table_names();

        // Process paths
        let paths: Vec<String> = self
            .table_to_csv
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .iter()
            .map(|path| {
                std::env::current_dir()
                    .unwrap()
                    .join(path)
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();

        //Replace all the paths in table_to_csv with the processed paths
        for (table, path) in self.table_to_csv.iter_mut() {
            *path = paths[all_tables.iter().position(|x| x == table).unwrap()].clone();
        }

        // Set up mappings for each table
        for i in 0..all_tables.len() {
            let name = &all_tables.get(i).unwrap();
            self.table_to_struct_name
                .insert(name.to_string(), format!("StructVar{}", i));
        }

        //table_to_csv and tables_info are already updated now.

        //let's update every stream with the struct name and first op
        let all_stream_names = self.streams.keys().cloned().collect::<Vec<String>>();
        let all_structs = self.table_to_struct_name.clone();
        let csvs = self.table_to_csv.clone();

        for stream in all_stream_names.iter() {
            let stream_obj = self.get_mut_stream(stream);
            let table_name = stream_obj.source_table.clone();
            let struct_name = all_structs.get(&table_name).unwrap();
            stream_obj.insert_op(format!(
                "ctx.stream_csv::<{}>(\"{}\")",
                struct_name,
                csvs.get(&table_name).unwrap()
            ));
        }

        self
    }

    /// Collects ONLY aggregates from the final projection
    /// This is Phase 1 of result mapping population, focusing only on gathering aggregates
    /// before AST parsing for GROUP BY processing
    pub fn collect_projection_aggregates(&mut self, ir_ast: &Arc<IrPlan>) {
        match &**ir_ast {
            IrPlan::Project { columns, .. } => {
                self.projection_agg.clear(); // Ensure we start with empty vec

                // If this is a SELECT *, we don't need to process as it won't contain aggregates
                if columns.len() == 1 {
                    if let ProjectionColumn::Column(col_ref, _) = &columns[0] {
                        if col_ref.column == "*" {
                            return;
                        }
                    }
                }

                // Process each projection to find and collect aggregates
                for projection in columns {
                    match projection {
                        ProjectionColumn::Aggregate(ref agg, ref alias) => {
                            // Direct aggregate in projection - add it
                            self.projection_agg
                                .push(ProjectionColumn::Aggregate(agg.clone(), alias.clone()));
                        }
                        ProjectionColumn::ComplexValue(ref field, ref alias) => {
                            // Find all aggregates in complex expressions
                            self.collect_aggregates_from_complex_field(field, alias.clone());
                        }
                        _ => continue, // Other projection types don't contain aggregates
                    }
                }
            }
            _ => panic!("Expected Project node at the root of the AST"),
        }
    }

    // Helper function to collect aggregates from complex fields
    fn collect_aggregates_from_complex_field(
        &mut self,
        field: &ComplexField,
        alias: Option<String>,
    ) {
        if let Some(ref agg) = field.aggregate {
            // Found an aggregate, add it
            self.projection_agg
                .push(ProjectionColumn::Aggregate(agg.clone(), alias.clone()));
        }

        // Check nested expressions recursively
        if let Some(ref nested) = field.nested_expr {
            let (left, _, right) = &**nested;
            self.collect_aggregates_from_complex_field(left, alias.clone());
            self.collect_aggregates_from_complex_field(right, alias.clone());
        }
    }

    pub fn populate_result_mappings(
        &mut self,
        columns: &Vec<ProjectionColumn>,
        stream_name: &String,
    ) {
        let mut used_names = std::collections::HashSet::new();
        let all_streams = self.streams.keys().cloned().collect::<Vec<String>>();
        let stream = self.get_stream(stream_name).clone();

        for clause in columns {
            match clause {
                ProjectionColumn::Column(col_ref, alias) => {
                    // Handle SELECT * case
                    if col_ref.column == "*" {
                        

                        if stream.is_keyed {
                            // If stream is keyed, only include GROUP BY keys
                            for key_col in &stream.key_columns {
                                let stream_name = if key_col.table.is_some() {
                                    self.get_stream_from_alias(key_col.table.as_ref().unwrap())
                                        .unwrap()
                                } else if all_streams.len() == 1 {
                                    &all_streams[0]
                                } else {
                                    panic!(
                                        "Column reference must have table name in JOIN query"
                                    );
                                };

                                let table = self.get_stream(stream_name).source_table.clone();
                                let col_type = self
                                    .get_struct_field(&table, &key_col.column)
                                    .expect("Column not found in table structure")
                                    .clone();

                                let suffix = if self.has_join {
                                    if key_col.table.is_some() {
                                        key_col.table.as_ref().unwrap().clone()
                                    } else {
                                        stream_name.clone()
                                    }
                                } else {
                                    stream_name.clone()
                                };

                                let col_name = format!("{}_{}", key_col.column, suffix);
                                self.result_column_types.insert(col_name, col_type);
                            }
                        } else {
                            // Not keyed - include all columns from all streams
                            let final_tables = if let Some(ref join_tree) = stream.join_tree {
                                // Use join tree to get all involved streams
                                join_tree.get_involved_streams()
                            } else {
                                vec![stream_name.clone()]
                            };

                            for stream_name in final_tables {
                                let stream = self.get_stream(&stream_name);
                                let table = &stream.source_table;
                                let alias = if stream.alias.is_empty() {
                                    table.clone()
                                } else {
                                    stream.alias.clone()
                                };

                                if let Some(struct_map) = self.tables_info.get(table) {
                                    for (col_name, col_type) in struct_map {
                                        let full_col_name = format!("{}_{}", col_name, alias);
                                        self.result_column_types
                                            .insert(full_col_name, col_type.clone());
                                    }
                                }
                            }
                        }
                    } else {
                        // Regular column
                        let col_name = alias.clone().unwrap_or_else(|| {
                            if self.has_join {
                                let table = col_ref
                                    .table
                                    .as_ref()
                                    .expect("Column reference must have table name in JOIN query");
                                format!("{}_{}", col_ref.column, table)
                            } else {
                                col_ref.column.clone()
                            }
                        });

                        let col_name = self.get_unique_name(&col_name, &mut used_names);
                        let col_type = self.get_type(col_ref);
                        self.result_column_types.insert(col_name, col_type);
                    }
                }
                ProjectionColumn::Aggregate(agg_func, alias) => {
                    let col_name = if let Some(alias_name) = alias {
                        self.get_unique_name(alias_name, &mut used_names)
                    } else {
                        let base_name = match &agg_func.function {
                            AggregateType::Count => {
                                if agg_func.column.column == "*" {
                                    "count_star".to_string()
                                } else if self.has_join {
                                    let table = agg_func.column.table.as_ref().expect(
                                        "Column reference must have table name in JOIN query",
                                    );
                                    format!("count_{}_{}", agg_func.column.column, table)
                                } else {
                                    format!("count_{}", agg_func.column.column)
                                }
                            }
                            other_agg => {
                                if self.has_join {
                                    let table = agg_func.column.table.as_ref().expect(
                                        "Column reference must have table name in JOIN query",
                                    );
                                    format!(
                                        "{}_{}_{}",
                                        other_agg.to_string().to_lowercase(),
                                        agg_func.column.column,
                                        table
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
                ProjectionColumn::ComplexValue(col_ref, alias) => {
                    let result_type = self.get_complex_field_type(col_ref);
                    let col_name = if let Some(alias_name) = alias {
                        self.get_unique_name(alias_name, &mut used_names)
                    } else {
                        let base_name = if self.has_join {
                            if let Some(ref col) = col_ref.column_ref {
                                let table = col
                                    .table
                                    .as_ref()
                                    .expect("Column reference must have table name in JOIN query");
                                format!("expr_{}_{}", col.column, table)
                            } else {
                                format!("expr_{}", used_names.len())
                            }
                        } else {
                            format!("expr_{}", used_names.len())
                        };
                        self.get_unique_name(&base_name, &mut used_names)
                    };

                    self.result_column_types.insert(col_name, result_type);
                }
                ProjectionColumn::StringLiteral(value, alias) => {
                    let col_name = self.get_unique_name(alias.as_ref().unwrap_or(value), &mut used_names);
                    self.result_column_types
                        .insert(col_name, "String".to_string());
                }
                _ => panic!("Invalid projection column type"),
            }
        }
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
            let stream_name = if col.table.is_some() {
                self.get_stream_from_alias(col.table.as_ref().unwrap())
                    .unwrap()
                    .clone()
            } else {
                let all_streams = self.streams.keys().cloned().collect::<Vec<String>>();
                if all_streams.len() == 1 {
                    all_streams[0].clone()
                } else {
                    panic!("Column reference must have table name in JOIN query");
                }
            };
            //check if the column is valid
            check_column_validity(col, &stream_name, self);
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
            //check if the column is valid (not when it's count(*))
            if !(agg.function == AggregateType::Count  && agg.column.column == "*") {
                let stream_name = if agg.column.table.is_some() {
                    self.get_stream_from_alias(agg.column.table.as_ref().unwrap())
                        .unwrap()
                        .clone()
                } else {
                    let all_streams = self.streams.keys().cloned().collect::<Vec<String>>();
                    if all_streams.len() == 1 {
                        all_streams[0].clone()
                    } else {
                        panic!("Column reference must have table name in JOIN query");
                    }
                };
                check_column_validity(&agg.column, &stream_name, self);
            }
            match agg.function {
                AggregateType::Count => "usize".to_string(),
                AggregateType::Avg => "f64".to_string(),
                _ => self.get_type(&agg.column),
            }
        } else {
            panic!("Invalid complex field - no valid content")
        }
    }

    fn collect_scan_nodes(plan: &Arc<IrPlan>) -> Vec<Arc<IrPlan>> {
        let mut scans = Vec::new();

        match &**plan {
            IrPlan::Scan { .. } => {
                scans.push(plan.clone());
            }
            IrPlan::Join { left, right, .. } => {
                // Recursively check both sides of join
                let left_scans = Self::collect_scan_nodes(left);
                let right_scans = Self::collect_scan_nodes(right);
                scans.extend(left_scans);
                scans.extend(right_scans);
            }
            IrPlan::Filter { input, .. }
            | IrPlan::Project { input, .. }
            | IrPlan::GroupBy { input, .. }
            | IrPlan::OrderBy { input, .. }
            | IrPlan::Limit { input, .. } => {
                // Recursively check input
                let input_scans = Self::collect_scan_nodes(input);
                scans.extend(input_scans);
            }
            _ => {}
        }
        scans
    }
}
