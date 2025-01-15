use crate::dsl::csv_utils::csv_parsers::*;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::binary_generation::creation::*;
use crate::dsl::ir::aqua::*;
use std::io;
use std::collections::HashMap;
use super::binary_generation::creation;
use crate::dsl::languages::sql::sql_parser::sql_to_aqua;

pub fn query_csv(query_str: &Vec<String>, output_path: &str, csv_path: &Vec<String>, user_defined_types: &Vec<String>) -> io::Result<String>
{
    //step 0: safety check on inputs
    if csv_path.len() != user_defined_types.len()
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Number of csv files and user defined types do not match"));
    }
    if query_str.len() != csv_path.len()
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Number of queries and csv files do not match"));
    }


    // step 1: if not existing, create a Rust project
    let rust_project = creation::RustProject::create_empty_project()?;

    // step 2: open csv input, read column names and data types, create the struct for each csv file
    let user_types: Vec<Vec<String>> = user_defined_types
    .iter()
    .map(|types| parse_type_string(types)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)))
    .collect::<Result<Vec<_>, _>>()?;

    let csv_structs: Vec<String> = user_types
    .iter()
    .enumerate()
    .map(|(index, types)| create_struct(types, index.to_string()))
    .collect();


    // step 3: Get CSV columns and combine with user defined types
    let columns: Vec<Vec<String>> = csv_path
    .iter()
    .map(|path| get_csv_columns(path))
    .collect();

    let hash_maps: Vec<HashMap<String, String>> = columns
    .iter()
    .zip(user_types.iter())
    .map(|(cols, types)| combine_arrays(cols, types))
    .collect();
    
   //TODO FIX query_to_string_aqua method
    // step 4: parse the queries
    let queries: Vec<String> = query_str
        .iter()
        .zip(hash_maps.iter())
        .map(|(query, hash_map)| query_aqua_to_renoir(&sql_to_aqua(query), hash_map))
        .collect();

    // step 5: generate main.rs and update it in the Rust project
    let main = create_template(&queries, csv_path, &csv_structs);
    rust_project.update_main_rs(&main)?;

    // step 6: compile the binary
    binary_execution(output_path, rust_project)

}