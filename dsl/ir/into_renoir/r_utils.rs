use crate::dsl::ir::QueryObject;
use crate::dsl::ir::IrLiteral;

// helper function to convert literal to string
pub(crate) fn convert_literal(literal: &IrLiteral) -> String {
    match literal {
        IrLiteral::Integer(val) => format!("{}", val),
        IrLiteral::Float(val) => format!("{:.2}", val),
        IrLiteral::String(val) => format!("\"{}\"", val),
        IrLiteral::Boolean(val) => format!("{}", val),
        IrLiteral::ColumnRef(_val) => "".to_string(),
    }
}

// method to check if a table is an alias and return the stream name
pub(crate) fn check_alias(table_to_check: &str, query_object: &QueryObject) -> String {
    //case if table is an alias
    if query_object.alias_to_stream.contains_key(table_to_check) {
        query_object
            .alias_to_stream
            .get(table_to_check)
            .unwrap()
            .to_string()
    }
    //case if table is not an alias
    else {
        //the table is actual a table name. Let's check if the table exists in the tables_info hashmap
        if query_object.tables_info.contains_key(table_to_check) {
            query_object
                .streams
                .keys()
                .cloned()
                .collect::<Vec<String>>()
                .first()
                .unwrap()
                .to_string()
        } else {
            //throw error
            panic!("Table {} does not exist", table_to_check);
        }
    }
}