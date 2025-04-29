pub(crate) mod subquery_process;
pub(crate) mod subquery_utils;

use indexmap::IndexMap;
use subquery_utils::manage_subqueries;

use super::binary_generation::creation;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::csv_utils::csv_parsers::*;
use crate::dsl::ir::*;
use crate::dsl::languages::sql::sql_parser::sql_to_ir;
use crate::dsl::struct_object::object::*;
use core::panic;
use std::io;
use std::sync::Arc;

/// Executes an SQL query on CSV files and generates a Rust binary containing the corresponding Renoir code.
///
/// # Arguments
///
/// * `sql_query` - A string that holds the SQL query to be executed.
/// * `output_path` - A string that holds the path where the output binary will be saved.
/// * `input_tables` - An `IndexMap` that holds the table name as the key and a tuple of CSV path and user-defined types as the value.
///
/// # Returns
///
/// * `io::Result<String>` - Returns an `Ok` variant with a success message if the operation is successful,
///   or an `Err` variant with an `io::Error` if an error occurs.
///
/// # Errors
///
/// This function will return an error if:
/// * The number of CSV files does not match the number of user-defined types.
/// * There is an error in parsing the user-defined types.
/// * There is an error in creating the Rust project.
/// * There is an error in reading the CSV columns.
/// * There is an error in combining the CSV columns with user-defined types.
/// * There is an error in parsing the SQL query.
/// * There is an error in generating the main.rs file.
/// * There is an error in compiling the binary.
///
/// # Steps
///
/// 1. Safety checks on inputs to ensure that for every defined table, there is a CSV path and user-defined types.
/// 2. Parses the SQL query to IR and builds the IR AST.
/// 3. Processes the IR AST and generates the corresponding Rust binary with Renoir code.
pub fn renoir_sql(
    sql_query: &str,
    output_path: &String,
    input_tables: &IndexMap<String, (String, String)>,
) -> io::Result<String> {
    //step 1: Safety checks on inputs
    //checks if the query contains "SELECT" and "FROM"
    if !sql_query.to_uppercase().contains("SELECT") && sql_query.to_uppercase().contains("FROM") {
        panic!("Invalid SQL query syntax");
    }
    //checks if the input_tables is empty
    if input_tables.is_empty() {
        panic!("No input tables provided");
    }
    //checks if no input table name contains an underscore
    for key in input_tables.keys() {
        if key.contains('_') {
            panic!("Table names cannot contain an underscore. {} .", key);
        }
    }
    //checks if every key of input_tables has a value
    for (key, (csv, types)) in input_tables.iter() {
        if csv.is_empty() {
            panic!("No CSV path provided for table {}", key);
        }
        if types.is_empty() {
            panic!("No user-defined types provided for table {}", key);
        }
    }

    //step 2: Parses the SQL query to IR. It builds the IR AST.
    let ir_query = sql_to_ir(sql_query);
    let ir_ast = query_ir_to_ast(&ir_query);

    //step 3: Processes the ast calling the process_ir_ast function
    process_ir_ast(ir_ast, output_path, input_tables)
}

/// Executes an IR query on CSV files and generates a Rust binary containing the corresponding Renoir code.
///
/// # Arguments
///
/// * `ir_query` - A string that holds the IR query to be executed.
/// * `output_path` - A string that holds the path where the output binary will be saved.
/// * `input_tables` - An `IndexMap` that holds the table name as the key and a tuple of CSV path and user-defined types as the value.
///
/// # Returns
///
/// * `io::Result<String>` - Returns an `Ok` variant with a success message if the operation is successful,
///   or an `Err` variant with an `io::Error` if an error occurs.
///
/// # Errors
///
/// This function will return an error if:
/// * The number of CSV files does not match the number of user-defined types.
/// * Fails in parsing the user-defined types.
/// * Fails in creating the Rust project.
/// * Fails in reading the CSV columns.
/// * Fails in combining the CSV columns with user-defined types.
/// * Fails in parsing the IR query.
/// * Fails in generating the main.rs file.
/// * Fails in compiling the binary.
///
/// # Steps
///
/// 1. Safety checks on inputs to ensure that for every defined table, there is a CSV path and user-defined types.
/// 2. Parses the IR query and builds the IR AST.
/// 3. Processes the IR AST and generates the corresponding Rust binary with Renoir code.
pub fn renoir_ir(
    ir_query: &str,
    output_path: &String,
    input_tables: &IndexMap<String, (String, String)>,
) -> io::Result<String> {
    //step 1: Safety checks on inputs
    //checks if the query contains "SELECT" and "FROM"
    if !ir_query.to_uppercase().contains("select") && ir_query.to_uppercase().contains("from") {
        panic!("Invalid IR query syntax");
    }
    //checks if the input_tables is empty
    if input_tables.is_empty() {
        panic!("No input tables provided");
    }
    //checks if no input table name contains an underscore
    for key in input_tables.keys() {
        if key.contains('_') {
            panic!("Table names cannot contain an underscore. {} .", key);
        }
    }
    //checks if every key of input_tables has a value
    for (key, (csv, types)) in input_tables.iter() {
        if csv.is_empty() {
            panic!("No CSV path provided for table {}", key);
        }
        if types.is_empty() {
            panic!("No user-defined types provided for table {}", key);
        }
    }

    //step 2: Parses the IR query and builds the IR AST.
    let ir_ast = query_ir_to_ast(ir_query);

    //step 3: Processes the ast calling the process_ir_ast function
    process_ir_ast(ir_ast, output_path, input_tables)
}

/// Processes the IR AST and generates a Rust binary containing the corresponding Renoir code.
pub(crate) fn process_ir_ast(
    ir_ast: Arc<IrPlan>,
    output_path: &String,
    input_tables: &IndexMap<String, (String, String)>,
) -> io::Result<String> {
    //creates a new QueryObject and sets the output path
    let mut query_object = QueryObject::new();
    query_object.set_output_path(output_path);

    //creates a new Rust project if it doesn't exist
    let rust_project = creation::RustProject::create_empty_project(output_path)?;

    //opens csvs input, reads column names and data types and creates the struct for each csv file
    let mut tables_info: IndexMap<String, IndexMap<String, String>> = IndexMap::new();
    let mut tables_csv: IndexMap<String, String> = IndexMap::new();

    for (key, (csv, type_list)) in input_tables.iter() {
        tables_csv.insert(key.to_string(), csv.to_string());
        let user_types: Vec<String> = parse_type_string(type_list).unwrap();
        let csv_columns: Vec<String> = get_csv_columns(csv);
        let temp: IndexMap<String, String> = csv_columns
            .into_iter()
            .zip(user_types.into_iter())
            .map(|(c, t)| (c.to_string(), t.to_string()))
            .collect();
        tables_info.insert(key.to_string(), temp);
    }

    //sets the tables info and csv paths in the query object
    query_object.set_tables_info(tables_info);
    query_object.set_table_to_csv(tables_csv);

    //calls the manage_subqueries function to handle any nested subqueries
    let ir_ast = manage_subqueries(&ir_ast, &mut query_object).unwrap();

    //calls the populate function to fill the query object with the IR AST
    query_object = query_object.populate(&ir_ast);

    //calls the collect_projection_aggregates function to collect the aggregates from the final projection clause
    query_object.collect_projection_aggregates(&ir_ast);

    //converts Ir AST to renoir string
    ir_ast_to_renoir(&mut query_object);

    //updates the fields object with the final structs and streams
    let structs = query_object.structs.clone();
    let streams = query_object.streams.clone();
    let fields = query_object.get_mut_fields();
    fields.output_path = output_path.clone();
    fields.fill(structs, streams);

    //generates main.rs and updates it in the Rust project
    fields.fill_main();
    rust_project.update_main_rs(&fields.main.clone())?;

    //finally compiles the generated binary
    binary_execution(output_path, rust_project)
}
