use indexmap::IndexMap;
use crate::dsl::ir::aqua::ir_ast_structure::ComplexField;
use crate::dsl::ir::aqua::{
    AggregateType, AquaLiteral, ColumnRef, SelectClause,
};
use crate::dsl::struct_object::object::QueryObject;


// struct to store the accumulator value
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum AccumulatorValue {
    Aggregate(AggregateType, ColumnRef),
    Column(ColumnRef),
}

#[derive(Debug)]
struct AccumulatorInfo {
    value_positions: IndexMap<AccumulatorValue, (usize, String)>, // (position, type)
}

impl AccumulatorInfo {
    fn new() -> Self {
        AccumulatorInfo {
            value_positions: IndexMap::new(),
        }
    }

    fn add_value(&mut self, value: AccumulatorValue, val_type: String) -> usize {
        if let Some((pos, _)) = self.value_positions.get(&value) {
            *pos
        } else {
            let pos = self.value_positions.len();
            self.value_positions.insert(value, (pos, val_type));
            pos
        }
    }

    fn add_avg(&mut self, column: ColumnRef, val_type: String) -> (usize, usize) {
        let sum_pos = self.add_value(AccumulatorValue::Aggregate(AggregateType::Sum, column.clone()), val_type);
        let count_pos = self.add_value(AccumulatorValue::Aggregate(AggregateType::Count, column), "usize".to_string());
        (sum_pos, count_pos)
    }
}


/// Processes a `SelectClause` and generates a corresponding string representation
/// of the query operation.
///
/// # Arguments
///
/// * `select_clauses` - A reference to a/// * `query_object` - A reference to the `QueryObject` which contains metadata and type information for the query.
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
/// 
/// 
/// 
/// 

pub fn process_select_clauses(
    select_clauses: &Vec<SelectClause>,
    query_object: &mut QueryObject,
) -> String {

    // Check for SELECT * case
    if select_clauses.len() == 1 {
        match &select_clauses[0] {
            SelectClause::Column(col_ref, _) if col_ref.column == "*" => {
                return create_select_star_map(query_object);
            }
            _ => {}
        }
    }
    // Check if any aggregations are present using recursive traversal
    let has_aggregates: bool = select_clauses.iter().any(|clause| {
        match clause {
            SelectClause::Aggregate(_, _) => true,
            SelectClause::ComplexValue(field, _) => has_aggregate_in_complex_field(field),
            _ => false
        }
    });

    if has_aggregates {
        create_aggregate_map(select_clauses, query_object)
    } else {
        create_simple_map(select_clauses, query_object)
    }
}
   
// function to create aggregate fold and map
fn create_aggregate_map(select_clauses: &Vec<SelectClause>, query_object: &QueryObject) -> String {
    let mut acc_info = AccumulatorInfo::new();
    let mut result = String::new();

    // First analyze all clauses to build accumulator info
    for (i, clause) in select_clauses.iter().enumerate() {
        let result_type = query_object.result_column_types.get_index(i).unwrap().1;
        match clause {
            SelectClause::Aggregate(agg, _) => {
                match agg.function {
                    AggregateType::Avg => {
                        acc_info.add_avg(agg.column.clone(), result_type.clone());
                    },
                    _ => {
                        acc_info.add_value(
                            AccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                            result_type.clone()
                        );
                    }
                }
            },
            SelectClause::ComplexValue(field, _) => {
                process_complex_field_for_accumulator(field, &mut acc_info, query_object);
            },
            SelectClause::Column(col, _) => {
                acc_info.add_value(
                    AccumulatorValue::Column(col.clone()),
                    result_type.clone()
                );
            }
        }
    }

     // Initialize the fold accumulator with correct types and initial values
     let mut tuple_types = Vec::new();
     let mut tuple_inits = Vec::new();
 
     for (_, (_, val_type)) in acc_info.value_positions.iter() {
         tuple_types.push(val_type.clone());
         
         // Set appropriate initial values based on type and aggregation
         match val_type.as_str() {
             "f64" => tuple_inits.push("0.0".to_string()),
             "i64" => tuple_inits.push("0".to_string()),
             "usize" => tuple_inits.push("0".to_string()),
             _ => panic!("Unsupported type: {}", val_type)
         }
     }
 
     // Override initial values for MIN/MAX aggregates
     for (value, (pos, val_type)) in acc_info.value_positions.iter() {
         if let AccumulatorValue::Aggregate(agg_type, _) = value {
             match agg_type {
                 AggregateType::Max => {
                     tuple_inits[*pos] = match val_type.as_str() {
                         "f64" => "f64::MIN".to_string(),
                         "i64" => "i64::MIN".to_string(),
                         _ => panic!("Invalid type for MAX: {}", val_type)
                     };
                 },
                 AggregateType::Min => {
                     tuple_inits[*pos] = match val_type.as_str() {
                         "f64" => "f64::MAX".to_string(),
                         "i64" => "i64::MAX".to_string(),
                         _ => panic!("Invalid type for MIN: {}", val_type)
                     };
                 },
                 _ => {}
             }
         }
     }
 
     let tuple_type = format!("({},)", tuple_types.join(", "));
     let tuple_init = format!("({},)", tuple_inits.join(", "));
 
     // Start fold operation
     result.push_str(&format!(".fold({}, |acc: &mut {}, x| {{\n", tuple_init, tuple_type));
 
     // Generate fold accumulator updates
     let mut update_code = String::new();

   for (value, (pos, _)) in acc_info.value_positions.iter() {
       match value {
           AccumulatorValue::Aggregate(agg_type, col) => {
               let col_access = if query_object.has_join {
                   let table = col.table.as_ref().unwrap();
                   let table_name = query_object.get_alias(table).unwrap_or_else(|| table);
                   format!("x{}.{}", 
                       query_object.table_to_tuple_access.get(table_name).unwrap(),
                       col.column)
               } else {
                   format!("x.{}", col.column)
               };

               match agg_type {
                   AggregateType::Count => {
                       if col.column == "*" {
                           update_code.push_str(&format!("    acc.{} += 1;\n", pos));
                       } else {
                           update_code.push_str(&format!(
                               "    if {}.is_some() {{ acc.{} += 1.0; }}\n",
                               col_access, pos
                           ));
                       }
                   },
                   AggregateType::Sum => {
                       update_code.push_str(&format!(
                           "    if let Some(val) = {} {{ acc.{} += val; }}\n",
                           col_access, pos
                       ));
                   },
                   AggregateType::Max => {
                       update_code.push_str(&format!(
                           "    if let Some(val) = {} {{ acc.{} = acc.{}.max(val); }}\n",
                           col_access, pos, pos
                       ));
                   },
                   AggregateType::Min => {
                       update_code.push_str(&format!(
                           "    if let Some(val) = {} {{ acc.{} = acc.{}.min(val); }}\n",
                           col_access, pos, pos
                       ));
                   },
                   AggregateType::Avg => {} // Handled through Sum and Count
               }
           },
           AccumulatorValue::Column(col) => {
               let col_access = if query_object.has_join {
                   let table = col.table.as_ref().unwrap();
                   let table_name = query_object.get_alias(table).unwrap_or_else(|| table);
                   format!("x{}.{}", 
                       query_object.table_to_tuple_access.get(table_name).unwrap(),
                       col.column)
               } else {
                   format!("x.{}", col.column)
               };

               update_code.push_str(&format!(
                   "    if let Some(val) = {} {{ acc.{} = val; }}\n",
                   col_access, pos
               ));
           }
       }
   }

   result.push_str(&update_code);
   result.push_str("})\n");

   // Generate final map to OutputStruct
   result.push_str(".map(|acc| OutputStruct {\n");

   for (i, clause) in select_clauses.iter().enumerate() {
       let field_name = query_object.result_column_types.get_index(i).unwrap().0;
       let value = match clause {
           SelectClause::Aggregate(agg, _) => {
               match agg.function {
                   AggregateType::Avg => {
                       let (sum_pos, count_pos) = (
                           acc_info.value_positions.get(&AccumulatorValue::Aggregate(
                               AggregateType::Sum, agg.column.clone())).unwrap().0,
                           acc_info.value_positions.get(&AccumulatorValue::Aggregate(
                               AggregateType::Count, agg.column.clone())).unwrap().0
                       );
                       format!("Some(acc.{} as f64 / acc.{} as f64)", sum_pos, count_pos)
                   },
                   _ => {
                       let pos = acc_info.value_positions.get(&AccumulatorValue::Aggregate(
                           agg.function.clone(), agg.column.clone())).unwrap().0;
                       format!("Some(acc.{})", pos)
                   }
               }
           },
           SelectClause::ComplexValue(field, _) => {
               format!("Some({})", process_complex_field_for_accumulator(field, &mut acc_info, query_object))
           },
           SelectClause::Column(col, _) => {
               let pos = acc_info.value_positions.get(&AccumulatorValue::Column(col.clone())).unwrap().0;
               format!("Some(acc.{})", pos)
           }
       };
       result.push_str(&format!("    {}: {},\n", field_name, value));
   }

   result.push_str("})");
   result
}

fn process_complex_field_for_accumulator(
    field: &ComplexField, 
    acc_info: &mut AccumulatorInfo,
    query_object: &QueryObject
) -> String {
    if let Some(ref nested) = field.nested_expr {
        // Handle nested expression (left_field OP right_field)
        let (left, op, right) = &**nested;
        
        // Special handling for power operation
        if op == "^" {
            let left_type = query_object.get_complex_field_type(left);
            let right_type = query_object.get_complex_field_type(right);
            
            // If either operand is f64, use powf
            if left_type == "f64" || right_type == "f64" {
                format!("({} as f64).powf({} as f64)", 
                    process_complex_field_for_accumulator(left, acc_info, query_object),
                    process_complex_field_for_accumulator(right, acc_info, query_object)
                )
            } else {
                // Both are integers, use pow
                // Note: pow expects u32 for exponent
                format!("({}).pow({} as u32)", 
                    process_complex_field_for_accumulator(left, acc_info, query_object),
                    process_complex_field_for_accumulator(right, acc_info, query_object)
                )
            }
        } else {
            format!("({} {} {})", 
                process_complex_field_for_accumulator(left, acc_info, query_object),
                op,
                process_complex_field_for_accumulator(right, acc_info, query_object)
            )
        }
    } else if let Some(ref col) = field.column_ref {
        // Handle regular column reference
        let pos = acc_info.add_value(
            AccumulatorValue::Column(col.clone()),
            query_object.get_type(col)
        );
        format!("acc.{}", pos)
    } else if let Some(ref lit) = field.literal {
        // Handle literal values
        match lit {
            AquaLiteral::Integer(i) => i.to_string(),
            AquaLiteral::Float(f) => format!("{:.2}", f),
            AquaLiteral::String(s) => format!("\"{}\"", s),
            AquaLiteral::Boolean(b) => b.to_string(),
            AquaLiteral::ColumnRef(col) => {
                let pos = acc_info.add_value(
                    AccumulatorValue::Column(col.clone()),
                    query_object.get_type(col)
                );
                format!("acc.{}", pos)
            }
        }
    } else if let Some(ref agg) = field.aggregate {
        // Handle aggregate functions
        match agg.function {
            AggregateType::Avg => {
                let (sum_pos, count_pos) = acc_info.add_avg(
                    agg.column.clone(),
                    query_object.get_type(&agg.column)
                );
                format!("(acc.{} as f64 / acc.{} as f64)", sum_pos, count_pos)
            },
            _ => {
                let pos = acc_info.add_value(
                    AccumulatorValue::Aggregate(agg.function.clone(), agg.column.clone()),
                    query_object.get_type(&agg.column)
                );
                format!("acc.{}", pos)
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}

fn create_select_star_map(query_object: &QueryObject) -> String {
    let mut result = String::from(".map(|x| OutputStruct { ");


    if query_object.has_join {
        // Handle joined case - need to use tuple access
        let tables = query_object.get_all_table_names();
        let empty_string = "".to_string();

        //for table in tables, build all the columns mapping in the .map

        for table_index in 0..tables.len() {
            let table = &tables[table_index];
            let tuple_access = query_object.table_to_tuple_access.get(table).unwrap_or_else(|| &empty_string);
            let table_struct = query_object.table_to_struct.get(table).unwrap();

            for (column_index, field_name) in table_struct.iter().enumerate() {
                result.push_str(&format!("{}: x{}.{}, ", query_object.result_column_types.get_index(table_index + column_index).unwrap().0, tuple_access, field_name.0));
            }
        }
    } else {
        // Simple case - direct access
        // retrieve the column list of the first table
        let columns = query_object.table_to_struct.get(&query_object.get_all_table_names()[0]).unwrap();
        
        //zip the column list with the result_column_types
        let zip = columns.iter().zip(query_object.result_column_types.iter());

        //iterate over the zip and build the mapping
        let fields: Vec<String> = zip.collect::<Vec<_>>().iter()
            .map(|(column,  result_column)| format!("{}: x.{}", result_column.0, column.0))
            .collect();

        result.push_str(&fields.join(", "));
    }

    result.push_str(" })");
    result
}

fn create_simple_map(select_clauses: &Vec<SelectClause>, query_object: &QueryObject) -> String {
    let mut map_string = String::from(".map(|x| OutputStruct { ");
    let empty_string = "".to_string();

    let fields: Vec<String> = select_clauses.iter()
        .enumerate()  // Add enumerate to track position
        .map(|(i, clause)| {
            match clause {
                SelectClause::Column(col_ref, _) => {
                    let field_name = query_object.result_column_types.get_index(i).unwrap_or_else(|| (&empty_string, &empty_string)).0;
                    let value = if query_object.has_join {
                        let table = col_ref.table.as_ref().unwrap();
                        let tuple_access = query_object.table_to_tuple_access
                            .get(table)
                            .expect("Table not found in tuple access map");
                        format!("x{}.{}", tuple_access, col_ref.column)
                    } else {
                        format!("x.{}", col_ref.column)
                    };
                    format!("{}: {}", field_name, value)
                },
                SelectClause::ComplexValue(complex_field, alias) => {
                    let field_name = alias.as_ref()
                        .unwrap_or_else(|| {
                            query_object.result_column_types.iter()
                                .nth(i)  // Use i from enumerate instead
                                .map(|(name, _)| name)
                                .unwrap()
                        });
                    let value = process_complex_field(complex_field, query_object);
                    format!("{}: Some({})", field_name, value)
                },
                _ => unreachable!("Should not have aggregates in simple map")
            }
        })
        .collect();

    map_string.push_str(&fields.join(", "));
    map_string.push_str(" })");
    map_string
}

fn process_complex_field(field: &ComplexField, query_object: &QueryObject) -> String {
    if let Some(ref nested) = field.nested_expr {
        // Handle nested expression (left_field OP right_field)
        let (left, op, right) = &**nested;
        
        // Special handling for power operation
        if op == "^" {
            let left_type = query_object.get_complex_field_type(left);
            let right_type = query_object.get_complex_field_type(right);
            
            // If either operand is f64, use powf
            if left_type == "f64" || right_type == "f64" {
                format!("({} as f64).powf({} as f64)", 
                    process_complex_field(left, query_object),
                    process_complex_field(right, query_object)
                )
            } else {
                // Both are integers, use pow
                // Note: pow expects u32 for exponent
                format!("({}).pow({} as u32)", 
                    process_complex_field(left, query_object),
                    process_complex_field(right, query_object)
                )
            }
        } else {
            format!("({} {} {})", 
                process_complex_field(left, query_object),
                op,
                process_complex_field(right, query_object)
            )
        }
    } else if let Some(ref col) = field.column_ref {
        // Handle column reference
        if query_object.has_join {
            let table = col.table.as_ref().unwrap();
            let tuple_access = query_object.table_to_tuple_access
                .get(table)
                .expect("Table not found in tuple access map");
            format!("x{}.{}.unwrap()", tuple_access, col.column)
        } else {
            format!("x.{}.unwrap()", col.column)
        }
    } else if let Some(ref lit) = field.literal {
        // Handle literal value
        match lit {
            AquaLiteral::Integer(i) => i.to_string(),
            AquaLiteral::Float(f) => format!("{:.2}", f),
            AquaLiteral::String(s) => format!("\"{}\"", s),
            AquaLiteral::Boolean(b) => b.to_string(),
            AquaLiteral::ColumnRef(col_ref) => {
                if query_object.has_join {
                    let table = col_ref.table.as_ref().unwrap();
                    let tuple_access = query_object.table_to_tuple_access
                        .get(table)
                        .expect("Table not found in tuple access map");
                    format!("x{}.{}.unwrap()", tuple_access, col_ref.column)
                } else {
                    format!("x.{}.unwrap()", col_ref.column)
                }
            }
        }
    } else {
        panic!("Invalid ComplexField - no valid content");
    }
}
// Recursive function to check for aggregates in ComplexField
fn has_aggregate_in_complex_field(field: &ComplexField) -> bool {
    // Check if this field has an aggregate
    if field.aggregate.is_some() {
        return true;
    }

    // Recursively check nested expressions
    if let Some(nested) = &field.nested_expr {
        let (left, _, right) = &**nested;
        // Check both sides of the nested expression
        return has_aggregate_in_complex_field(left) || has_aggregate_in_complex_field(right);
    }

    false
}