use indexmap::IndexMap;
use old_subquery_utils::old_manage_subqueries;
use subquery_utils::manage_subqueries;

use super::binary_generation::creation;
use super::binary_generation::fields::Fields;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::csv_utils::csv_parsers::*;
use crate::dsl::ir::*;
use crate::dsl::languages::dataframe::dataframe_to_ir;
use crate::dsl::languages::sql::sql_parser::sql_to_ir;
use crate::dsl::struct_object::object::*;
use core::panic;
use std::io;
use std::sync::Arc;

pub mod old_subquery_utils;
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
pub fn query(
    query_str: &str,
    output_path: &String,
    input_tables: &IndexMap<String, (String, String)>, // key: table name, value: (csv_path, user_defined_types)
) -> io::Result<String> {
    // Determine query type based on syntax
    let is_sql_query =
        query_str.to_uppercase().contains("SELECT") && query_str.to_uppercase().contains("FROM");

    let ir_ast = if !is_sql_query && query_str.contains('.') {
        // Parse as DataFrame query
        println!("Detected DataFrame query syntax");
        match dataframe_to_ir(query_str) {
            Ok(ast) => ast,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error parsing DataFrame query: {}", e),
                ))
            }
        }
    } else {
        // Parse as SQL query
        println!("Detected SQL query syntax");
        let ir_query = sql_to_ir(query_str);
        query_ir_to_ast(&ir_query)
    };

    // Common processing logic for both query types
    process_query(ir_ast, output_path, input_tables)
}

pub fn query_ir_input(query_ir: &str,
    output_path: &String,
    input_tables: &IndexMap<String, (String, String)>, // key: table name, value: (csv_path, user_defined types)
) -> io::Result<String> {

    let ir_ast = query_ir_to_ast(query_ir);
    //println!("IR AST: {:?}", ir_ast);

    process_query(ir_ast, output_path, input_tables)
} 

pub fn process_query(
    ir_ast: Arc<IrPlan>,
    output_path: &String,
    input_tables: &IndexMap<String, (String, String)>, // key: table name, value: (csv_path, user_defined_types)
) -> io::Result<String> {
    // step 0: safety checks
    if input_tables.is_empty() {
        panic!("No input tables provided");
    }

    //check if no input table name contains an underscore
    for key in input_tables.keys() {
        if key.contains('_') {
            panic!("Table names cannot contain an underscore. {} .", key);
        }
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

    // step 3: manage subqueries
    //ir_ast = manage_subqueries(&ir_ast, &output_path.to_string(), &mut query_object).unwrap();
    let ir_ast = manage_subqueries(&ir_ast, &output_path.to_string(), &mut query_object).unwrap();

    //println!("IR AST: {:?}", ir_ast);

    query_object = query_object.populate(&ir_ast);
    //println!("Ir AST: {:?}", query_object.ir_ast);
    query_object.collect_projection_aggregates(&ir_ast);

    // step 4: convert Ir AST to renoir string
    let result = ir_ast_to_renoir(&mut query_object);
    if result.is_err() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Error converting IR AST to Renoir",
        ));
    }

    let structs = query_object.structs.clone();
    let streams = query_object.streams.clone();

    //step 4.5: update fields
    let fields = query_object.get_mut_fields();
    fields.output_path = output_path.clone();
    fields.fill(structs, streams);
    fields.fill_main();

    // step 5: generate main.rs and update it in the Rust project
    //let main = create_template(&query_object, false);
    let main = fields.main.clone();
    rust_project.update_main_rs(&main)?;

    // step 6: compile the binary
    binary_execution(output_path, rust_project)
}

//method that appends the generated substream to the unique main.rs file
pub fn subquery_csv(
    ir_ast: Arc<IrPlan>,
    output_path: &String,
    tables_info: IndexMap<String, IndexMap<String, String>>,
    tables_csv: IndexMap<String, String>,
    is_single_result: bool,
) -> (String, String, Fields) {
    // step 1: create query_object
    let mut query_object = QueryObject::new();
    query_object.set_output_path(output_path);
    query_object.set_tables_info(tables_info);
    query_object.set_table_to_csv(tables_csv);

    // step2: -----------

    // step 3: check if there is a subquery
    let ir_ast = manage_subqueries(&ir_ast, &output_path.to_string(), &mut query_object).unwrap();

    // step 3.5: populate query_object with ir_ast
    query_object = query_object.populate(&ir_ast);
    query_object.collect_projection_aggregates(&ir_ast);

    // step 4: convert Ir AST to renoir string
    let result = ir_ast_to_renoir(&mut query_object);
    if result.is_err() {
        panic!("Error converting IR AST to Renoir");
    }

    let structs = query_object.structs.clone();
    let streams = query_object.streams.clone();
    let _result_columns = query_object.result_column_types.clone();

    //step 4.5: update fields
    let fields = query_object.get_mut_fields();
    fields.output_path = output_path.clone();
    fields.fill(structs, streams);

    let (subquery_result, subquery_result_type) = fields.collect_subquery_result(is_single_result);

    // step 5: ----------------

    // return the name of the result vec
    (subquery_result, subquery_result_type, fields.clone())
}

//old method that executes an entire different main.rs file for each subquery
pub fn old_subquery_csv(
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
    let ir_ast =
        old_manage_subqueries(&ir_ast, &output_path.to_string(), &mut query_object).unwrap();

    // step 3.5: populate query_object with ir_ast
    query_object = query_object.populate(&ir_ast);
    query_object.collect_projection_aggregates(&ir_ast);

    // step 4: convert Ir AST to renoir string
    let result = ir_ast_to_renoir(&mut query_object);
    if result.is_err() {
        panic!("Error converting IR AST to Renoir");
    }

    let structs = query_object.structs.clone();
    let streams = query_object.streams.clone();
    let result_columns = query_object.result_column_types.clone();

    //step 4.5: update fields
    let fields = query_object.get_mut_fields();
    fields.output_path = output_path.clone();
    fields.fill(structs, streams);
    fields.fill_subquery_main(result_columns);

    // step 5: generate main.rs and update it in the Rust project
    let main = fields.main.clone();
    let _ = rust_project.update_main_rs(&main);

    fields.main.clear();
    fields.streams.clear();
    fields.structs.clear();

    // step 6: compile the binary and return the output as string
    let output = binary_execution(output_path, rust_project);
    if let Ok(output) = output {
        output
    } else {
        panic!("Error compiling the binary");
    }
}

//method that creates a stream that has no sink, that will be used for scan operations
//method that appends the generated substream to the unique main.rs file
pub fn subquery_sink(
    ir_ast: Arc<IrPlan>,
    output_path: &String,
    tables_info: IndexMap<String, IndexMap<String, String>>,
    tables_csv: IndexMap<String, String>,
) -> Fields {
    // step 1: create query_object
    let mut query_object = QueryObject::new();
    query_object.set_output_path(output_path);
    query_object.set_tables_info(tables_info);
    query_object.set_table_to_csv(tables_csv);

    // step2: -----------

    // step 3: check if there is a subquery
    let ir_ast = manage_subqueries(&ir_ast, &output_path.to_string(), &mut query_object).unwrap();

    // step 3.5: populate query_object with ir_ast
    query_object = query_object.populate(&ir_ast);
    query_object.collect_projection_aggregates(&ir_ast);

    // step 4: convert Ir AST to renoir string
    let result = ir_ast_to_renoir(&mut query_object);
    if result.is_err() {
        panic!("Error converting IR AST to Renoir");
    }

    let structs = query_object.structs.clone();
    let streams = query_object.streams.clone();
    let _result_columns = query_object.result_column_types.clone();

    //step 4.5: update fields
    let fields = query_object.get_mut_fields();
    fields.output_path = output_path.clone();
    fields.fill(structs, streams);

    // step 5: ----------------
    // return the name of the alias stream
    fields.clone()
}
