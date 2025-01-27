use crate::dsl::ir::aqua::{AggregateType, AquaLiteral, QueryObject, SelectClause};
use crate::dsl::ir::aqua::r_utils::convert_column_ref;

/// Processes a `SelectClause` and generates a corresponding string representation
/// of the query operation.
///
/// # Arguments
///
/// * 'select_clauses` - A reference to the Vec<SelectClause> which represents all selections in the query.
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

pub fn process_select_clauses(select_clauses: &Vec<SelectClause>, query_object: &QueryObject) -> String {
    // If there's only one column and it's an asterisk, return the identity map
    if select_clauses.len() == 1 {
        if let SelectClause::Column(col) = &select_clauses[0] {
            if col.column == "*" {
                return ".map(|x| x)".to_string();
            }
        }
    }

    // Start building the map expression
    let mut map_internals = String::new();
    map_internals.push_str("|x| (");

    // Process each select clause
    for (i, clause) in select_clauses.iter().enumerate() {
        if i > 0 {
            map_internals.push_str(", ");
        }

        match clause {
            SelectClause::Column(col_ref) => {
                map_internals.push_str(&convert_column_ref(&col_ref, query_object));
            }
            SelectClause::Aggregate(agg) => {
                let data_type = query_object.get_type(&agg.column);
                if data_type != "f64" && data_type != "i64" {
                    panic!("Invalid type for aggregation");
                }

                match agg.function {
                    AggregateType::Max => {
                        map_internals.push_str(&format!(
                            "{}.unwrap()",
                            convert_column_ref(&agg.column, query_object)
                        ));
                    }
                    AggregateType::Min => {
                        map_internals.push_str(&format!(
                            "{}.unwrap()",
                            convert_column_ref(&agg.column, query_object)
                        ));
                    }
                    AggregateType::Avg => {
                        map_internals.push_str(&format!(
                            "{}.unwrap()",
                            convert_column_ref(&agg.column, query_object)
                        ));
                    }
                }
            }
            SelectClause::ComplexValue(col_ref, op, val) => {
                let value = match val {
                    AquaLiteral::Float(val) => format!("{:.2}", val),
                    AquaLiteral::Integer(val) => val.to_string(),
                    AquaLiteral::Boolean(val) => val.to_string(),
                    AquaLiteral::String(val) => val.clone(),
                    AquaLiteral::ColumnRef(column_ref) => convert_column_ref(&column_ref, query_object),
                };

                if op == "^" {
                    let data_type = query_object.get_type(&col_ref);
                    if data_type != "f64" && data_type != "i64" {
                        panic!("Invalid type for power operation");
                    }
                    map_internals.push_str(&format!("{}.pow({})", convert_column_ref(&col_ref, query_object), value));
                } else {
                    map_internals.push_str(&format!("{} {} {}", convert_column_ref(&col_ref, query_object), op, value));
                }
            }
        }
    }

    map_internals.push_str(")");
    format!(".map({})", map_internals)
}