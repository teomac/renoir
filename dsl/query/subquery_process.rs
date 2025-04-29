use crate::dsl::query::subquery_utils::manage_subqueries;
use indexmap::IndexMap;

use crate::dsl::binary_generation::fields::Fields;
use crate::dsl::ir::*;
use crate::dsl::struct_object::object::*;
use std::sync::Arc;

// Executes and parses a specific subquery provided in input. Generates renoir code for the subquery and
// returns an updated instance of 'Field' object.
//
// # Arguments
//
// * `ir_ast` - An `Arc<IrPlan>` that holds the intermediate representation of the subquery.
// * `output_path` - A string that holds the path where the output binary will be saved.
// * `tables_info` - An `IndexMap` that holds the table name as the key and a tuple of column names and user-defined types as the value.
// * `tables_csv` - An `IndexMap` that holds the table name as the key and a string that holds the CSV path as the value.
// * `is_single_result` - A boolean that indicates if the subquery is expected to return a single result.
//
// # Returns
//
// * `Fields` - An updated 'Fields' object.
//
// # Errors
//
// This function will return an error if the conversion of the IR AST to Renoir fails.
//
// # Steps
//
// 1. Creates a new `QueryObject` and sets the output path, tables info, and table to CSV mappings.
// 2. Checks if there are any nested subqueries in the IR AST and manages them accordingly.
/// 3. Populates the `QueryObject` with the IR AST and collects projection aggregates.
/// 4. Converts the IR AST to a Renoir string.
/// 5. Updates the fields with the structs and streams from the `QueryObject`.
/// 6. Returns the updated `Fields` object.
pub(crate) fn subquery_renoir(
    ir_ast: Arc<IrPlan>,
    output_path: &str,
    tables_info: IndexMap<String, IndexMap<String, String>>,
    tables_csv: IndexMap<String, String>,
) -> Fields {
    // step 1: creates query_object
    let mut query_object = QueryObject::new();
    query_object.set_output_path(output_path);
    query_object.set_tables_info(tables_info);
    query_object.set_table_to_csv(tables_csv);

    // step 2: manages any nested subqueries
    let ir_ast = manage_subqueries(&ir_ast, &mut query_object).unwrap();

    // step 3: populates query_object with ir_ast
    query_object = query_object.populate(&ir_ast);
    query_object.collect_projection_aggregates(&ir_ast);

    // step 4: converts Ir AST to renoir string
    ir_ast_to_renoir(&mut query_object);

    //step 5: updates 'Field' object
    let structs = query_object.structs.clone();
    let streams = query_object.streams.clone();
    let fields = query_object.get_mut_fields();
    fields.output_path = output_path.to_owned();
    fields.fill(structs, streams);

    // step 6: returns the updated 'Field' object
    fields.clone()
}

// Executes and parses a specific subquery provided in input. Generates renoir code for the subquery and
// returns the name of the subquery result, its type, and an updated instance of 'Field' object.
//
// # Arguments
//
// * `ir_ast` - An `Arc<IrPlan>` that holds the intermediate representation of the subquery.
// * `output_path` - A string that holds the path where the output binary will be saved.
// * `tables_info` - An `IndexMap` that holds the table name as the key and a tuple of column names and user-defined types as the value.
// * `tables_csv` - An `IndexMap` that holds the table name as the key and a string that holds the CSV path as the value.
// * `is_single_result` - A boolean that indicates if the subquery is expected to return a single result.
//
// # Returns
//
// * `(String, String, Fields)` - A tuple containing the name of the subquery result, its type, and an updated 'Fields' object.
//
// # Steps
//
// 1. Calls the `subquery_renoir` function to generate the Renoir code for the subquery and get the updated 'Fields' object.
// 2. Calls the `collect_subquery_result` method on the 'Fields' object to get the name and type of the subquery result.
// 3. Returns the name of the subquery result, its type, and the updated 'Fields' object.

pub(crate) fn subquery_result(
    ir_ast: Arc<IrPlan>,
    output_path: &str,
    tables_info: IndexMap<String, IndexMap<String, String>>,
    tables_csv: IndexMap<String, String>,
    is_single_result: bool,
) -> (String, String, Fields) {
    // step 1: generates renoir code for the subquery and gets the updated 'Fields' object
    let mut fields = subquery_renoir(ir_ast, output_path, tables_info, tables_csv);

    // step 2: collects the subquery result name and type
    let (subquery_result, subquery_result_type) = fields.collect_subquery_result(is_single_result);

    //step 3: returns the vec name, type and fields object
    (subquery_result, subquery_result_type, fields.clone())
}
