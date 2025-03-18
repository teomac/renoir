use crate::dsl::ir::ColumnRef;
use crate::dsl::ir::QueryObject;
use crate::dsl::ir::{AggregateFunction, AggregateType, IrLiteral};

// helper function to convert column reference to string
pub fn convert_column_ref(column_ref: &ColumnRef, query_object: &QueryObject) -> String {
    if column_ref.column == "*" {
        if !query_object.has_join {
            return "x".to_string();
        } else
        //case join
        {
            let val = column_ref.table.as_ref().unwrap();
            let stream_name = check_alias(val, query_object);

            let stream = query_object.get_stream(&stream_name);

            let access = stream.get_access().base_path.clone();
            return format!("x{}", access);
        }
    }

    if !query_object.has_join {
        let all_streams = query_object
            .streams
            .keys()
            .cloned()
            .collect::<Vec<String>>();
        let stream_name = all_streams.first().unwrap();
        let stream = query_object.get_stream(stream_name);

        let col = if stream.check_if_column_exists(&column_ref.column) {
            column_ref.column.to_string()
        } else {
            //throw error
            panic!(
                "Column {} does not exist in stream {}",
                column_ref.column, stream_name
            );
        };
        format!("x{}.{}", stream.get_access().get_base_path(), col)
    } else
    //case join
    {
        // take value from column_ref.table
        let val = column_ref.table.as_ref().unwrap();
        // check if value is an alias in the query object hashmap
        let stream_name = check_alias(val, query_object);

        let stream = query_object.get_stream(&stream_name);

        let col = if stream.check_if_column_exists(&column_ref.column) {
            column_ref.column.to_string()
        } else {
            //throw error
            panic!(
                "Column {} does not exist in stream {}",
                column_ref.column, stream_name
            );
        };

        format!("x{}.{}", stream.get_access().get_base_path(), col)
    }
}

// helper function to convert literal to string
pub fn convert_literal(literal: &IrLiteral) -> String {
    match literal {
        IrLiteral::Integer(val) => format!("{}", val),
        IrLiteral::Float(val) => format!("{:.2}", val),
        IrLiteral::String(val) => format!("\"{}\"", val),
        IrLiteral::Boolean(val) => format!("{}", val),
        IrLiteral::ColumnRef(_val) => "".to_string(),
    }
}

// helper function to get the type of a literal
pub fn get_type_from_literal(literal: &IrLiteral) -> String {
    match literal {
        IrLiteral::Integer(_) => "i64".to_string(),
        IrLiteral::Float(_) => "f64".to_string(),
        IrLiteral::String(_) => "String".to_string(),
        IrLiteral::Boolean(_) => "bool".to_string(),
        IrLiteral::ColumnRef(_) => "".to_string(),
    }
}

pub fn convert_aggregate(aggregate: &AggregateFunction, query_object: &QueryObject) -> String {
    let func = match aggregate.function {
        AggregateType::Max => "max",
        AggregateType::Min => "min",
        AggregateType::Avg => "avg",
        AggregateType::Sum => "sum",
        AggregateType::Count => "count",
    };

    let col = convert_column_ref(&aggregate.column, query_object);

    format!("{}.{}", func, col)
}

// method to check if a table is an alias and return the stream name
pub fn check_alias(table_to_check: &str, query_object: &QueryObject) -> String {
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

// Helper function to find the exact matching result column for an ORDER BY column
pub fn find_matching_result_column(
    column_name: &str,
    table_name: Option<&str>,
    query_object: &QueryObject,
) -> Option<String> {
    let result_column_keys: Vec<&String> = query_object.result_column_types.keys().collect();
    // If the column name is already an exact match in the result columns, return it
    if result_column_keys.contains(&&column_name.to_string()) {
        return Some(column_name.to_string());
    }

    // Case 1: With table name specified
    if let Some(table) = table_name {
        // Check if table is an alias and get the actual table name if needed
        let actual_table = check_alias(table_name.unwrap(), query_object);

        //if actual_table is equal to table_name, table_name is not an alias
        let is_alias = actual_table != table_name.unwrap();

        // Build the expected pattern for the column name
        let table_suffix = if is_alias {
            table_name.unwrap().to_string()
        } else {
            actual_table.to_string()
        };
        let expected_pattern = format!("{}_{}", column_name, table_suffix);

        // First check for exact match with the pattern
        if let Some(key) = result_column_keys.iter().find(|k| ***k == expected_pattern) {
            return Some(key.to_string());
        }

        // If not found, check for any result column that starts with column name and matches the table
        for key in result_column_keys {
            // Split by underscore to check if the last part matches the table/alias
            let parts: Vec<&str> = key.split('_').collect();
            if parts.len() >= 2 {
                let potential_col = parts[0];
                let potential_table = parts.last().unwrap();

                if potential_col == column_name
                    && (potential_table == &table || **potential_table == table_suffix)
                {
                    return Some(key.clone());
                }
            }
        }
    }
    // Case 2: No table specified - try to find any matching column
    else {
        for key in result_column_keys {
            if key.starts_with(column_name)
                && (key.len() == column_name.len()
                    || key.chars().nth(column_name.len()) == Some('_'))
            {
                return Some(key.clone());
            }
        }
    }
    None
}
