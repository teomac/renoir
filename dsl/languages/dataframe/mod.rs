use std::io;
use std::sync::Arc;

use ast_builder::df_utils::ConverterObject;
use converter::build_ir_ast_df;
use indexmap::IndexMap;
use metadata::extract_metadata;
use serde_json::Value;

use crate::dsl::{
    binary_generation::{creation, execution::binary_execution},
    ir::{ir_ast_to_renoir, IrPlan},
    query::subquery_utils::manage_subqueries,
    struct_object::object::QueryObject,
};

pub(crate) mod ast_builder;
pub(crate) mod conversion_error;
pub(crate) mod converter;
pub(crate) mod metadata;

pub fn renoir_dataframe(
    metadata_list: Vec<String>,
    csv_paths: Vec<String>,
    expr_ids_mapping: IndexMap<usize, (String, String)>,
    catalyst_plan: &[Value],
    output_path: &str,
    renoir_path: &Option<String>,
) -> io::Result<String> {
    //step 1: convert metadata_list to a IndexMap<String, (String, String)> containing the input tables
    let input_tables = extract_metadata(metadata_list, csv_paths)?;

    //step 2: retrieve an object from the catalyst plan that contains the mapping expr_id to table name

    let mut conv_object = ConverterObject::new(expr_ids_mapping.clone());

    //step 3: Generate the IR Plan from the catalyst plan

    let ir_ast = build_ir_ast_df(catalyst_plan, &mut conv_object).unwrap();

    //step 4: Process the IR AST and generate the Rust binary with Renoir code
    process_ir_ast_for_df(ir_ast, &output_path.to_string(), renoir_path, &input_tables)
}

/// Processes the IR AST and generates a Rust binary containing the corresponding Renoir code.
fn process_ir_ast_for_df(
    ir_ast: Arc<IrPlan>,
    output_path: &String,
    renoir_path: &Option<String>,
    input_tables: &IndexMap<String, (String, IndexMap<String, String>)>,
) -> io::Result<String> {
    //creates a new QueryObject and sets the output path
    let mut query_object = QueryObject::new();
    query_object.set_output_path(output_path);

    //creates a new Rust project if it doesn't exist
    let rust_project = creation::RustProject::create_empty_project(output_path, renoir_path)?;

    //opens csvs input, reads column names and data types and creates the struct for each csv file
    let mut tables_info: IndexMap<String, IndexMap<String, String>> = IndexMap::new();
    let mut tables_csv: IndexMap<String, String> = IndexMap::new();

    for (key, (csv, type_list)) in input_tables.iter() {
        tables_csv.insert(key.to_string(), csv.to_string());
        tables_info.insert(key.to_string(), type_list.clone());
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
