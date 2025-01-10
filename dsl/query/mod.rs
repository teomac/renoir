use csv_parsers::*;
use crate::dsl::binary_generation::execution::*;
use crate::dsl::binary_generation::creation::*;
use crate::dsl::parsers::csv_utils::*;
use crate::dsl::ir::aqua::*;
use std::io;
use super::binary_generation::creation;

pub fn query_csv(query_str: &str, output_path: &str, csv_path: &str, user_defined_types: &str) -> io::Result<String>
{
    // step 1: if not existing, create a Rust project
    let rust_project = creation::RustProject::create_empty_project()?;

    // step 2: open csv input, read column names and data types
    let user_defined_types = parse_type_string(user_defined_types)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    let columns = get_csv_columns(csv_path);
    let hash_map = combine_arrays(&columns, &user_defined_types);

    // step 3: create the struct
    let struct_string = create_struct(&user_defined_types);

    // step 4: parse the query
    let query = query_to_string_aqua(query_str, hash_map);

    // step 5: generate main.rs and update it in the Rust project
    let main = create_template(&query, csv_path, &struct_string);
    rust_project.update_main_rs(&main)?;

    // step 6: compile the binary
    let result = binary_execution(output_path, rust_project);

    result

}