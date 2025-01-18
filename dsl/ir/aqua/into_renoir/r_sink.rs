use crate::dsl::ir::aqua::{AggregateType, AquaLiteral, QueryObject, SelectClause};
use crate::dsl::ir::aqua::r_utils::convert_column_ref;

/// Processes a `SelectClause` and generates a corresponding string representation
/// of the query operation.
///
/// # Arguments
///
/// * `ast` - A reference to the `SelectClause` which represents the selection clause in the query.
/// * `query_object` - A reference to the `QueryObject` which contains metadata and type information for the query.
///
/// # Returns
///
/// A `String` that represents the query operation based on the provided `SelectClause`.
///
/// # Panics
///
/// This function will panic if:
/// - The data type for aggregation is not `f64` or `i64`.
/// - The data type for power operation is not `f64` or `i64`.
///

pub fn process_select_clause (ast: &SelectClause, query_object: &QueryObject) -> String {
    let mut final_string = String::new();
    match &ast {
        SelectClause::Aggregate(agg) => {
            let data_type = query_object.get_type(&agg.column);
            if data_type != "f64" || data_type != "i64" {
                panic!("Invalid type for aggregation");
            }
            let agg_str = match agg.function {
                AggregateType::Max => "max",
                AggregateType::Min => "min",
                AggregateType::Avg => "avg",
            };

            if agg_str == "max" {
                
                final_string.push_str(&format!("
                .map(|x| x.{}.unwrap())
                .fold(
                    None,
                    |acc: &mut Option<{}>, x| {{
                        match acc {{
                            None => *acc = x,
                            Some(curr) => {{
                                if x.unwrap() > curr.clone() {{
                                    *acc = x;
                            }}
                        }}
                    }}
                }}
                )",
                convert_column_ref(&agg.column, &query_object),
                data_type));
            } else {
                unreachable!(); // TODO
            }
        }
        SelectClause::ComplexValue(col, char ,val  ) => {
            let value = match &val {
                AquaLiteral::Float(val) => format!("{:.2}", val),
                AquaLiteral::Integer(val) => val.to_string(),
                AquaLiteral::Boolean(val) => val.to_string(),
                AquaLiteral::String(val) => val.to_string(),
                AquaLiteral::ColumnRef(column_ref) => convert_column_ref(&column_ref, &query_object),
            };
            if char == "^" {
                let data_type = query_object.get_type(&col);
                if data_type != "f64" || data_type != "i64" {
                    panic!("Invalid type for power operation");
                }
                final_string.push_str(&format!(".map(|x| {}.pow({}))", convert_column_ref(&col, &query_object), value));
            } else {
                final_string.push_str(&format!(".map(|x| {} {} {})", convert_column_ref(&col, &query_object), char, value));
            }
        }
        SelectClause::Column(col) => {
            if col.column != "*" {
                final_string.push_str(&format!(".map(|x| {})", convert_column_ref(&col, &query_object)));
            } else {
                final_string.push_str(".map(|x| x)");
            }
        }
    }
    final_string
}