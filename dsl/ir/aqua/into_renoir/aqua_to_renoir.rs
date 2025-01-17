use core::panic;

use crate::dsl::{ir::aqua::{ast_parser::ast_structure::AquaAST, AggregateType, AquaLiteral, BinaryOp, ColumnRef, ComparisonOp, Condition, FromClause, SelectClause, WhereClause}, struct_object::object::QueryObject};

pub struct AquaToRenoir;

impl AquaToRenoir {
    pub fn convert(ast: &AquaAST, query_object: &QueryObject) -> String {


        let mut final_string = String::new();

        let from_clause = &ast.from; 
        final_string.push_str(&format!(
            "{}",
            Self::process_from_clause(&from_clause, &query_object)
        ));

        if let Some(where_clause) = &ast.filter {
            final_string.push_str(&format!(
                ".filter(|x| {})",
                Self::process_where_clause(&where_clause, &query_object)
            ));
        }
        
        // Add aggregation or column selection
        match &ast.select {
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
                    Self::convert_column_ref(&agg.column, query_object),
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
                    AquaLiteral::ColumnRef(column_ref) => Self::convert_column_ref(&column_ref, query_object),
                };
                if char == "^" {
                    let data_type = query_object.get_type(&col);
                    if data_type != "f64" || data_type != "i64" {
                        panic!("Invalid type for power operation");
                    }
                    final_string.push_str(&format!(".map(|x| {}.pow({}))", Self::convert_column_ref(&col, query_object), value));
                } else {
                    final_string.push_str(&format!(".map(|x| {} {} {})", Self::convert_column_ref(&col, query_object), char, value));
                }
            }
            SelectClause::Column(col) => {
                if col.column != "*" {
                    final_string.push_str(&format!(".map(|x| {})", Self::convert_column_ref(&col, query_object)));
                } else {
                    final_string.push_str(".map(|x| x)");
                }
            }
    }
    
        println!("Final string: {}", final_string);
        final_string
    }

    // Helper function to recursively process where conditions
    fn process_where_clause(clause: &WhereClause, query_object: &QueryObject) -> String {
        let mut current = clause;
        let mut conditions = Vec::new();
        
        // Process first condition
        conditions.push(Self::process_condition(&current.condition, query_object));
        
        // Process remaining conditions
        while let (Some(op), Some(next)) = (&current.binary_op, &current.next) {
            let op_str = match op {
                BinaryOp::And => "&&",
                BinaryOp::Or => "||",
            };
            conditions.push(op_str.to_string());
            conditions.push(Self::process_condition(&next.condition, query_object));
            current = next;
        }
        
        conditions.join(" ")
    }

    // Helper function to process a single condition
    fn process_condition(condition: &Condition, query_object: &QueryObject) -> String {
        let operator_str = match condition.operator {
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::LessThan => "<",
            ComparisonOp::Equal => "==",
            ComparisonOp::GreaterThanEquals => ">=",
            ComparisonOp::LessThanEquals => "<=",
            ComparisonOp::NotEqual => "!=",
        };

        let value = match &condition.value {
            AquaLiteral::Float(val) => format!("{:.2}", val),
            AquaLiteral::Integer(val) => val.to_string(),
            AquaLiteral::String(val) => val.to_string(),
            AquaLiteral::Boolean(val) => val.to_string(),
            AquaLiteral::ColumnRef(column_ref) => Self::convert_column_ref(&column_ref, query_object),
        };


        let table_names = query_object.get_all_table_names();

        if !query_object.has_join {
            return format!(
                "x.{}.unwrap() {} {}",
                query_object.table_to_struct.get(table_names.first().unwrap()).unwrap().get(&condition.variable.column).unwrap(),
                operator_str,
                value
            );
        }
        else {
            let table_name = Self::check_alias(&condition.variable.table.clone().unwrap(), query_object);
            return format!(
                "x.{}.{}.unwrap() {} {}",
                query_object.table_to_struct_name.get(&table_name).unwrap().chars().last().unwrap(),
                query_object.table_to_struct.get(&table_name).unwrap().get(&condition.variable.column).unwrap(),
                operator_str,
                value
            );
        }

    }

    fn process_from_clause(from_clause: &FromClause, query_object: &QueryObject) -> String {
        if !query_object.has_join {
            "".to_string();
        }
    
        if let Some(join) = &from_clause.join {
            let joined_table = &join.scan.stream_name;
    
            let left_col = &join.condition.left_col;
            let right_col = &join.condition.right_col;
    
            let first_struct = query_object.get_struct_name(&joined_table).unwrap();
            let first_index = first_struct.chars().last().unwrap();

            // check if left_col.table is not an alias in the query object hashmap
            let left_table_name = Self::check_alias(&left_col.table.clone().unwrap(), query_object);

            // same for right_col
            let right_table_name = Self::check_alias(&right_col.table.clone().unwrap(), query_object);


            let left_field = query_object
                .get_struct_field(
                    &left_table_name, 
                    &left_col.column
                )
                .unwrap();
            
            let right_field = query_object
                .get_struct_field(
                    &right_table_name, 
                    &right_col.column
                )
                .unwrap();
    
            
            format!(
                ".join(stream{}, |x| x.{}.clone(), |y| y.{}.clone()).drop_key()",
                first_index,
                left_field,
                right_field
            )
        } else {
            String::new()
        }
    }

    // helper function to convert column reference to string
    fn convert_column_ref(column_ref: &ColumnRef, query_object: &QueryObject) -> String {
        let table_names = query_object.get_all_table_names();

        if !query_object.has_join {
            let table_name = table_names.first().unwrap();
            let col = query_object.table_to_struct.get(table_name).unwrap().get(&column_ref.column).unwrap();
            format!("x.{}", col)
        } else {
            // take value from column_ref.table
            let val = column_ref.table.clone().unwrap();
            // check if value is an alias in the query object hashmap
            let mut table_name = String::new();
            if query_object.table_to_alias.contains_key(&val) {
                table_name = val;
            }
            // else it's a table name
            else {
                table_name = query_object.table_to_alias.iter().find(|&x| x.1 == &val).unwrap().0.clone();
            }

            let col = query_object.table_to_struct.get(&table_name).unwrap().get(&column_ref.column).unwrap();
            let i = query_object.table_to_struct_name.get(&table_name).unwrap().chars().last().unwrap();
            format!("x.{}.{}.unwrap()", i, col)
        }
        
    }

    // method to check if a table is an alias and return the table name
    fn check_alias(table: &str, query_object: &QueryObject) -> String {
        if query_object.table_to_alias.contains_key(table) {
            table.to_string()
        } else {
            query_object.table_to_alias.iter().find(|&x| x.1 == table).unwrap().0.clone()
        }
    }
} 