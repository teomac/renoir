use crate::dsl::csv_utils::csv_parsers::*;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::binary_generation::creation::*;
use crate::dsl::ir::aqua::*;
use std::io;
use std::collections::HashMap;
use super::binary_generation::creation;
use crate::dsl::languages::sql::sql_parser::sql_to_aqua;
use crate::dsl::struct_object::object::*;

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

    query_object.initialize(csv_path, &user_types);

    let csv_structs: Vec<String> = user_types
    .iter()
    .enumerate()
    .map(|(index, types)| create_struct(types, index.to_string()))
    .collect();

    for i in 0..user_types.len()
    {
        query_object.add_field_list(generate_field_list(&user_types[i]));
    }

    query_object.parsed_structs = csv_structs.clone();
    println!("Parsed structs: {:?}", csv_structs);


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
    // step 4: parse the query
    let aqua_query = sql_to_aqua(query_str);
    let aqua_ast = query_aqua_to_ast(&aqua_query);
    query_object = query_object.populate(&aqua_ast, &csv_path, &hash_maps);
    let renoir_string = aqua_ast_to_renoir(&aqua_ast, &query_object);
    query_object.set_renoir_string(&renoir_string);


    // step 5: generate main.rs and update it in the Rust project
    let main = create_template(&query_object);
    rust_project.update_main_rs(&main)?;


    // step 6: compile the binary
    binary_execution(output_path, rust_project)

}