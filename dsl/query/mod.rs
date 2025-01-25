use indexmap::IndexMap;

use crate::dsl::csv_utils::csv_parsers::*;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::binary_generation::creation::*;
use crate::dsl::ir::aqua::*;
use std::io;
use super::binary_generation::creation;
use crate::dsl::languages::sql::sql_parser::sql_to_aqua;
use crate::dsl::struct_object::object::*;

/// Executes a query on CSV files and generates a Rust binary to process the query.
///
/// # Arguments
///
/// * `query_str` - A string that holds the query to be executed.
/// * `output_path` - A string that holds the path where the output binary will be saved.
/// * `csv_path` - A vector of strings that holds the paths to the CSV files that refers to the tables in the query
/// * `user_defined_types` - A vector of strings that holds the user-defined types for each CSV file.
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
/// 0. Safety check on inputs to ensure the number of CSV files matches the number of user-defined types strings.
/// 1. Create an empty Rust project.
/// 2. Open CSV input, read column names and data types, and create the struct for each CSV file.
/// 3. Parse the query and convert it to an intermediate representation (Aqua).
/// 4. Convert the Aqua AST to a valid Renoir query.
/// 5. Generate the main.rs file and update it in the Rust project.
/// 6. Compile the binary and save it to the specified output path.

pub fn query_csv(query_str: &String, output_path: &str, csv_path: &Vec<String>, user_defined_types: &Vec<String>) -> io::Result<String>
{

    //step 0: safety check on inputs
    if csv_path.len() != user_defined_types.len()
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Number of csv files and user defined types do not match"));
    }

    let mut query_object = QueryObject::new();

    // step 1: if not existing, create a Rust project
    let rust_project = creation::RustProject::create_empty_project()?;

    // step 2: open csv input, read column names and data types, create the struct for each csv file
    let user_types: Vec<Vec<String>> = user_defined_types
    .iter()
    .map(|types| parse_type_string(types)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)))
    .collect::<Result<Vec<_>, _>>()?;

    let mut field_list: Vec<Vec<(String, String)>> = Vec::new();
    for i in 0..user_types.len()
    {
        let var = generate_field_list(&user_types[i]);
        field_list.push(var.clone());
        query_object.add_field_list(var);
    }

    let csv_structs: Vec<String> = field_list
        .iter()
        .enumerate()
        .map(|(index, types)| create_struct(&types, index.to_string()))
        .collect();

    println!("Parsed structs: {:?}", csv_structs);


    // step 2.1: Get CSV columns and combine with user defined types
    let columns: Vec<Vec<String>> = csv_path
    .iter()
    .map(|path| get_csv_columns(path))
    .collect();

    let hash_maps: Vec<IndexMap<String, String>> = columns
    .iter()
    .zip(user_types.iter())
    .map(|(cols, types)| {
        cols.iter()
            .zip(types.iter())
            .map(|(c, t)| (c.to_string(), t.to_string()))
            .collect()
    })
    .collect();

    // debug print hash_maps
    for hash_map in &hash_maps {
        println!("AAAAAAAAAAAAAAAAAAAAAAAAAA{:?}", hash_map);
    }
    
    // step 3: parse the query
    let aqua_query = sql_to_aqua(query_str);
    let aqua_ast = query_aqua_to_ast(&aqua_query);
    query_object = query_object.populate(&aqua_ast, &csv_path, &hash_maps);
    
    // step 4: convert aqua AST to renoir string
    let renoir_string = aqua_ast_to_renoir(&aqua_ast, &query_object);
    query_object.set_renoir_string(&renoir_string);


    // step 5: generate main.rs and update it in the Rust project
    let main = create_template(&query_object);
    rust_project.update_main_rs(&main)?;


    // step 6: compile the binary
    binary_execution(output_path, rust_project)

}