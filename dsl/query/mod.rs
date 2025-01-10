use csv_parsers::*;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::binary_generation::creation::*;
use crate::dsl::parsers::csv_utils::*;
use crate::dsl::ir::aqua::*;
use std::io;
use std::collections::HashMap;
use super::binary_generation::creation;

pub fn query_csv(query_str: &Vec<String>, output_path: &str, csv_path: &Vec<String>, user_defined_types: &Vec<String>) -> io::Result<String>
{
    let mut index = 0;
    //safety check on inputs
    //TODO


    // step 1: if not existing, create a Rust project
    let rust_project = creation::RustProject::create_empty_project()?;

    // step 2: open csv input, read column names and data types, create the struct for each csv file

    //for each string in user_defined_types, parse it and return a vector of strings
    let mut user_types = Vec::<Vec<String>>::new();

    let mut csv_structs = Vec::<String>::new();

    for types in user_defined_types {
        let parsed_types = parse_type_string(types)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        user_types.push(parsed_types);
        //for each parsed type, create a struct
        let csv_struct = create_struct(&user_types[index], index.to_string());
        csv_structs.push(csv_struct);
        index += 1;
    }

    //for each csv file in csv_path, get the columns and combine them with the user_defined_types
    let mut columns = Vec::<Vec<String>>::new();
    let mut hash_maps = Vec::<HashMap<String, String>>::new();
    
    //reset index
    index = 0;
    for path in csv_path {
        let csv_columns = get_csv_columns(path);
        columns.push(csv_columns);
        let hash_map = combine_arrays(&columns[index], &user_types[index]);
        hash_maps.push(hash_map);
        index += 1;
    }
    
   
    // step 4: parse the queries
    let mut queries = Vec::<String>::new();
    //reset index
    index = 0;
    for query in query_str {
        let query = query_to_string_aqua(query, &hash_maps[index]);
        queries.push(query);
        index += 1;
    }

    // step 5: generate main.rs and update it in the Rust project
    let main = create_template(&queries, csv_path, &csv_structs);
    rust_project.update_main_rs(&main)?;

    // step 6: compile the binary
    let result = binary_execution(output_path, rust_project);

    result

}