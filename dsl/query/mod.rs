use indexmap::IndexMap;
use subquery_utils::manage_subqueries;

use super::binary_generation::creation;
use crate::dsl::binary_generation::creation::*;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::csv_utils::csv_parsers::*;
use crate::dsl::ir::*;
use crate::dsl::languages::sql::sql_parser::sql_to_ir;
use crate::dsl::struct_object::object::*;
use core::panic;
use std::io;
use std::sync::Arc;

pub mod subquery_utils;

/// Executes a query on CSV files and generates a Rust binary to process the query.
///
/// # Arguments
///
/// * `query_str` - A string that holds the query to be executed.
/// * `output_path` - A string that holds the path where the output binary will be saved.
/// *  `input_tables` - An `IndexMap` that holds the table name as the key and a tuple of CSV path and user-defined types as the value.
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
/// 0. Safety check on inputs to ensure that for every defined table, there is a CSV path and user-defined types.
/// 1. Create an empty Rust project.
/// 2. Open CSV input, read column names and data types, and create the struct for each CSV file.
/// 3. Parse the query and convert it to an intermediate representation.
/// 4. Convert the Ir AST to a valid Renoir query.
/// 5. Generate the main.rs file and update it in the Rust project.
/// 6. Compile the binary and save it to the specified output path.

pub fn query_csv(
    query_str: &str,
    output_path: &String,
    input_tables: &IndexMap<String, (String, String)>, // key: table name, value: (csv_path, user_defined_types)
) -> io::Result<String> {
    // step 0: safety checks
    if input_tables.is_empty() {
        panic!("No input tables provided");
    }

    // check if every key of input_tables has a value
    for (key, (csv, types)) in input_tables.iter() {
        if csv.is_empty() {
            panic!("No CSV path provided for table {}", key);
        }
        if types.is_empty() {
            panic!("No user-defined types provided for table {}", key);
        }
    }

    let mut query_object = QueryObject::new();

    query_object.set_output_path(output_path);

    // step 1: if not existing, create a Rust project
    let rust_project = creation::RustProject::create_empty_project(output_path)?;

    // step 2: open csv input, read column names and data types, create the struct for each csv file

    let mut tables_info: IndexMap<String, IndexMap<String, String>> = IndexMap::new();
    let mut tables_csv: IndexMap<String, String> = IndexMap::new();

    for (key, (csv, type_list)) in input_tables.iter() {
        tables_csv.insert(key.to_string(), csv.to_string());
        let user_types = parse_type_string(type_list).unwrap();
        let csv_columns = get_csv_columns(csv);
        let temp: IndexMap<String, String> = csv_columns
            .into_iter()
            .zip(user_types.into_iter())
            .map(|(c, t)| (c.to_string(), t.to_string()))
            .collect();
        tables_info.insert(key.to_string(), temp);
    }

    query_object.set_tables_info(tables_info);
    query_object.set_table_to_csv(tables_csv);

    // step 3: parse the query
    let ir_query = sql_to_ir(query_str);
    let mut ir_ast = query_ir_to_ast(&ir_query);
    // step 3.5: manage subqueries
    ir_ast = manage_subqueries(&ir_ast, &output_path.to_string(), &query_object).unwrap();

    query_object = query_object.populate(&ir_ast);
    println!("Ir AST: {:?}", query_object.ir_ast);
    query_object.collect_projection_aggregates(&ir_ast);

    // step 4: convert Ir AST to renoir string
    let result = ir_ast_to_renoir(&mut query_object);
    if result.is_err() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Error converting IR AST to Renoir",
        ));
    }

    // step 5: generate main.rs and update it in the Rust project
    let main = create_template(&query_object, false);
    rust_project.update_main_rs(&main)?;

    // step 6: compile the binary
    binary_execution(output_path, rust_project)
}

pub fn subquery_csv(
    ir_ast: Arc<IrPlan>,
    output_path: &String,
    tables_info: IndexMap<String, IndexMap<String, String>>,
    tables_csv: IndexMap<String, String>,
) -> String {
    
    // step 1: create query_object
    let mut query_object = QueryObject::new();
    query_object.set_output_path(output_path);
    query_object.set_tables_info(tables_info);
    query_object.set_table_to_csv(tables_csv);

    // step2: create new temporary project
    let rust_project = creation::RustProject::create_empty_project(output_path).unwrap();

    // step 3: check if there is a subquery
    let ir_ast = manage_subqueries(&ir_ast, &output_path.to_string(), &query_object).unwrap();

    // step 3.5: populate query_object with ir_ast
    query_object = query_object.populate(&ir_ast);
    query_object.collect_projection_aggregates(&ir_ast);

    // step 4: convert Ir AST to renoir string
    let result = ir_ast_to_renoir(&mut query_object);
    if result.is_err() {
        panic!("Error converting IR AST to Renoir");
    }

    // step 5: generate main.rs
    let main = create_template(&query_object, true);
    let _ = rust_project.update_main_rs(&main);

    // step 6: compile the binary and return the output as string
    let output = binary_execution(output_path, rust_project);
    if let Ok(output) = output {
        output
    } else {
        panic!("Error compiling the binary");
    }
}
