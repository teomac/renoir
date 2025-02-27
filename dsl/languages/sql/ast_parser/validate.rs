
use super::sql_ast_structure::*;
use super::error::SqlParseError;

pub fn validate_ast(ast: &SqlAST) -> Result<(), SqlParseError> {
    validate_limit_offset(&ast.limit)?;
    validate_no_aggregates_in_where(&ast.filter)?;
    validate_having_columns_in_group_by(ast)?;
    validate_order_by(ast)?;


    Ok(())
}

fn validate_order_by(ast: &SqlAST) -> Result<(), SqlParseError> {
    // If there's no ORDER BY clause, nothing to validate
    if let Some(order_by) = &ast.order_by {
        // Extract column references from SELECT clause
        let mut select_columns = Vec::new();
        
        // Extract column references and also collect any referenceable columns from complex expressions
        for select_clause in &ast.select.select {
            match &select_clause.selection {
                SelectType::Simple(col_ref) => {
                    select_columns.push(col_ref.clone());
                },
                SelectType::Aggregate(_, col_ref) => {
                    select_columns.push(col_ref.clone());
                },
                SelectType::ComplexValue(left, _, right) => {
                    // For complex expressions, extract any direct column references
                    extract_columns_from_complex(left, &mut select_columns);
                    extract_columns_from_complex(right, &mut select_columns);
                }
            }
        }

        // Also get column aliases from SELECT clause for matching
        let select_aliases: Vec<String> = ast.select.select.iter()
            .filter_map(|s| s.alias.clone())
            .collect();

        // Check if SELECT contains asterisk (SELECT *) - in that case any column is valid for ORDER BY
        let has_asterisk = ast.select.select.iter().any(|s| {
            match &s.selection {
                SelectType::Simple(col_ref) => col_ref.column == "*",
                _ => false
            }
        });
        
        // If we have SELECT *, all columns are available for ORDER BY
        if has_asterisk {
            return Ok(());
        }
        
        // Check each ORDER BY item
        for item in &order_by.items {
            let column_name = &item.column.column;
            let table_name = &item.column.table;
            
            // Check if ORDER BY column is in SELECT columns
            let in_select = select_columns.iter().any(|col| {
                // Match on column name
                col.column == *column_name &&
                // Match on table name if both are specified, or if ORDER BY doesn't specify table
                (col.table == *table_name || table_name.is_none())
            });

            // Check if ORDER BY column matches a SELECT alias
            let matches_alias = table_name.is_none() && select_aliases.iter().any(|alias| alias == column_name);

            // If it's neither in SELECT columns nor matches an alias, it's invalid
            if !in_select && !matches_alias {
                return Err(SqlParseError::InvalidInput(
                    format!("ORDER BY column '{}' must appear in the SELECT list", 
                        if let Some(table) = table_name {
                            format!("{}.{}", table, column_name)
                        } else {
                            column_name.clone()
                        }
                    )
                ));
            }
        }
    }
    
    Ok(())
}

// Helper method to extract column references from complex expressions
fn extract_columns_from_complex(field: &ComplexField, columns: &mut Vec<ColumnRef>) {
    if let Some(col_ref) = &field.column_ref {
        columns.push(col_ref.clone());
    }
    
    if let Some(ref nested) = field.nested_expr {
        let (left, _, right) = &**nested;
        extract_columns_from_complex(left, columns);
        extract_columns_from_complex(right, columns);
    }
    
    if let Some((_, col_ref)) = &field.aggregate {
        columns.push(col_ref.clone());
    }
}


// check if where has aggregates
fn validate_no_aggregates_in_where(clause: &Option<WhereClause>) -> Result<(), SqlParseError> {
    if let Some(where_clause) = clause {
        match where_clause {
            WhereClause::Base(base_condition) => {
                match base_condition {
                    WhereBaseCondition::Comparison(cond) => {
                        check_where_field_for_aggregates(&cond.left_field)?;
                        check_where_field_for_aggregates(&cond.right_field)?;
                    },
                    WhereBaseCondition::NullCheck(null_cond) => {
                        check_where_field_for_aggregates(&null_cond.field)?;
                    }
                }
            },
            WhereClause::Expression { left, op: _, right } => {
                validate_no_aggregates_in_where(&Some(*left.clone()))?;
                validate_no_aggregates_in_where(&Some(*right.clone()))?;
            }
        }
    }
    Ok(())
}

fn check_where_field_for_aggregates(field: &WhereField) -> Result<(), SqlParseError> {
    if let Some(ref arithmetic) = field.arithmetic {
        check_arithmetic_for_aggregates(arithmetic)?;
    }
    Ok(())
}

fn check_arithmetic_for_aggregates(expr: &ArithmeticExpr) -> Result<(), SqlParseError> {
    match expr {
        ArithmeticExpr::Aggregate(func, col_ref) => {
            // Found aggregate function in WHERE clause which is invalid
            return Err(SqlParseError::InvalidInput(
                format!("Aggregate function '{}({})' cannot be used in WHERE clause", 
                    match func {
                        AggregateFunction::Max => "MAX",
                        AggregateFunction::Min => "MIN",
                        AggregateFunction::Avg => "AVG",
                        AggregateFunction::Sum => "SUM",
                        AggregateFunction::Count => "COUNT",
                    },
                    col_ref.to_string()
                )
            ));
        },
        ArithmeticExpr::BinaryOp(left, _, right) => {
            check_arithmetic_for_aggregates(left)?;
            check_arithmetic_for_aggregates(right)?;
        },
        // Other cases (Column, Literal) don't contain aggregates
        _ => {}
    }
    Ok(())
}

fn validate_having_columns_in_group_by(ast: &SqlAST) -> Result<(), SqlParseError> {
    if let (Some(group_by), Some(having)) = (&ast.group_by, &ast.group_by.as_ref().and_then(|gb| gb.having.as_ref())) {
        let group_by_columns = &group_by.columns;
        validate_having_expr_columns(having, group_by_columns)?;
    }
    Ok(())
}

fn validate_having_expr_columns(having: &HavingClause, group_by_columns: &[ColumnRef]) -> Result<(), SqlParseError> {
    match having {
        HavingClause::Base(base_condition) => {
            match base_condition {
                HavingBaseCondition::Comparison(cond) => {
                    validate_having_field(&cond.left_field, group_by_columns)?;
                    validate_having_field(&cond.right_field, group_by_columns)?;
                },
                HavingBaseCondition::NullCheck(null_cond) => {
                    validate_having_field(&null_cond.field, group_by_columns)?;
                }
            }
        },
        HavingClause::Expression { left, op: _, right } => {
            validate_having_expr_columns(left, group_by_columns)?;
            validate_having_expr_columns(right, group_by_columns)?;
        }
    }
    Ok(())
}

fn validate_having_field(field: &HavingField, group_by_columns: &[ColumnRef]) -> Result<(), SqlParseError> {
    // Skip validation for literals or aggregates (they're always allowed)
    if field.value.is_some() || field.aggregate.is_some() {
        return Ok(());
    }
    
    // Check column references
    if let Some(ref col) = field.column {
        let is_in_group_by = group_by_columns.iter().any(|gb_col| {
            gb_col.column == col.column && 
            (gb_col.table == col.table || col.table.is_none())
        });
        
        if !is_in_group_by {
            return Err(SqlParseError::InvalidInput(
                format!("Column '{}' in HAVING clause must be in GROUP BY or used in an aggregate function", 
                    if let Some(table) = &col.table {
                        format!("{}.{}", table, col.column)
                    } else {
                        col.column.clone()
                    }
                )
            ));
        }
    }
    
    // Check arithmetic expressions
    if let Some(ref arithmetic) = field.arithmetic {
        validate_arithmetic_columns(arithmetic, group_by_columns)?;
    }
    
    Ok(())
}

fn validate_arithmetic_columns(expr: &ArithmeticExpr, group_by_columns: &[ColumnRef]) -> Result<(), SqlParseError> {
    match expr {
        ArithmeticExpr::Column(col_ref) => {
            // Check if this column is in the GROUP BY
            let is_in_group_by = group_by_columns.iter().any(|gb_col| {
                gb_col.column == col_ref.column && 
                (gb_col.table == col_ref.table || col_ref.table.is_none())
            });
            
            Ok(if !is_in_group_by {
                return Err(SqlParseError::InvalidInput(
                    format!("Column '{}' in HAVING clause must be in GROUP BY or used in an aggregate function", 
                        col_ref.to_string()
                    )
                ));
            })
        },
        ArithmeticExpr::Aggregate(_, _) => {
            Ok(())
        
            // Aggregates are always allowed in HAVING
        },
        ArithmeticExpr::Literal(_) => {
            Ok(())
        },
        ArithmeticExpr::BinaryOp(left, _, right) => {
            validate_arithmetic_columns(left, group_by_columns)?;
            validate_arithmetic_columns(right, group_by_columns)?;
            Ok(())
        }
    }
}

fn validate_limit_offset(clause: &Option<LimitClause>) -> Result<(), SqlParseError> {
    if let Some(limit_clause) = clause {
        // Check that LIMIT is non-negative
        println!("Limit: {:?}", limit_clause.limit);
        if limit_clause.limit < 0 {
            return Err(SqlParseError::InvalidInput(
                format!("LIMIT value must be non-negative, got {}", limit_clause.limit)
            ));
        }
        
        // Check that OFFSET (if present) is non-negative
        if let Some(offset) = limit_clause.offset {
            if offset < 0 {
                return Err(SqlParseError::InvalidInput(
                    format!("OFFSET value must be non-negative, got {}", offset)
                ));
            }
        }
    }
    Ok(())
}

