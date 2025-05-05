use std::io;

use metadata::{extract_expr_ids, extract_metadata};
use serde_json::Value;

use crate::dsl::query::process_ir_ast;

pub(crate) mod converter;
pub(crate) mod conversion_error;
pub(crate) mod metadata;


pub fn renoir_dataframe(metadata_list: Vec<String>, csv_paths: Vec<String>, catalyst_plan: &[Value], output_path: &str) -> io::Result<String> {
    //step 1: convert metadata_list to a IndexMap<String, (String, String)> containing the input tables

    //safety checks on metadata_list

    let input_tables = extract_metadata(metadata_list, csv_paths)?;

    println!("Input tables: {:?}", input_tables);

    //step 2: retireve an object from the catalyst plan that contains the mapping expr_id to table name

    let expr_ids = extract_expr_ids(catalyst_plan, &input_tables);

    println!("Expr IDs: {:?}", expr_ids);

    //step 2: Generate the IR Plan from the catalyst plan

    //let ir_ast = CatalystConverter::convert(catalyst_plan)?;

    //step 3: Process the IR AST and generate the Rust binary with Renoir code
    //process_ir_ast(&ir_ast, &output_path.to_string(), &input_tables)
    Ok("".to_string())
}