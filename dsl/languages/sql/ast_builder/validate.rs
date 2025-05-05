use super::error::SqlParseError;
use super::sql_ast_structure::*;

pub(crate) fn validate_ast(ast: &SqlAST) -> Result<(), Box<SqlParseError>> {
    validate_limit_offset(&ast.limit)?;
    validate_no_aggregates_in_where(&ast.filter)?;
    validate_having_columns_in_group_by(ast)?;
    validate_order_by(ast)?;

    for select_column in &ast.select.select {
        validate_select_subquery(&select_column.selection)?;
    }

    Ok(())
}

fn validate_order_by(ast: &SqlAST) -> Result<(), Box<SqlParseError>> {
    if let Some(order_by) = &ast.order_by {
        let mut select_columns = Vec::new();

        // Extract column references and also collect any referenceable columns from expressions
        for select_clause in &ast.select.select {
            match &select_clause.selection {
                SelectType::Simple(col_ref) => {
                    select_columns.push(col_ref.clone());
                }
                SelectType::Aggregate(_, col_ref) => {
                    select_columns.push(col_ref.clone());
                }
                // Updated to handle ArithmeticExpr instead of ComplexValue
                SelectType::ArithmeticExpr(expr) => {
                    extract_columns_from_arithmetic(expr, &mut select_columns);
                }
                _ => { /* Ignore literals and subqueries */ }
            }
        }

        // Rest of validation logic remains the same
        let select_aliases: Vec<String> = ast
            .select
            .select
            .iter()
            .filter_map(|s| s.alias.clone())
            .collect();

        let has_asterisk = ast.select.select.iter().any(|s| match &s.selection {
            SelectType::Simple(col_ref) => col_ref.column == "*",
            _ => false,
        });

        if has_asterisk {
            return Ok(());
        }

        for item in &order_by.items {
            let column_name = &item.column.column;
            let table_name = &item.column.table;

            let in_select = select_columns.iter().any(|col| {
                col.column == *column_name && (col.table == *table_name || table_name.is_none())
            });

            let matches_alias =
                table_name.is_none() && select_aliases.iter().any(|alias| alias == column_name);

            if !in_select && !matches_alias {
                return Err(Box::new(SqlParseError::InvalidInput(format!(
                    "ORDER BY column '{}' must appear in the SELECT list",
                    if let Some(table) = table_name {
                        format!("{}.{}", table, column_name)
                    } else {
                        column_name.clone()
                    }
                ))));
            }
        }
    }

    Ok(())
}

// Replace extract_columns_from_complex with this new function
fn extract_columns_from_arithmetic(expr: &ArithmeticExpr, columns: &mut Vec<ColumnRef>) {
    match expr {
        ArithmeticExpr::Column(col_ref) => {
            columns.push(col_ref.clone());
        }
        ArithmeticExpr::NestedExpr(left, _, right, _) => {
            extract_columns_from_arithmetic(left, columns);
            extract_columns_from_arithmetic(right, columns);
        }
        ArithmeticExpr::Aggregate(_, col_ref) => {
            columns.push(col_ref.clone());
        }
        ArithmeticExpr::Subquery(_) => {
            // Subqueries are handled separately
        }
        ArithmeticExpr::Literal(_) => {
            // Literals don't contain column references
        }
    }
}

// check if where has aggregates
fn validate_no_aggregates_in_where(clause: &Option<WhereClause>) -> Result<(), Box<SqlParseError>> {
    if let Some(where_clause) = clause {
        match where_clause {
            WhereClause::Base(base_condition) => match base_condition {
                WhereBaseCondition::Comparison(cond) => {
                    check_where_field_for_aggregates(&cond.left_field)?;
                    check_where_field_for_aggregates(&cond.right_field)?;
                }
                WhereBaseCondition::NullCheck(null_cond) => {
                    check_where_field_for_aggregates(&null_cond.field)?;
                }
                WhereBaseCondition::Exists(_, _) => { /*TODO */ }
                WhereBaseCondition::In(_) => { /*TODO */ }
                WhereBaseCondition::Boolean(_) => { /*TODO */ }
            },
            WhereClause::Expression { left, op: _, right } => {
                validate_no_aggregates_in_where(&Some(*left.clone()))?;
                validate_no_aggregates_in_where(&Some(*right.clone()))?;
            }
        }
    }
    Ok(())
}

fn check_where_field_for_aggregates(field: &WhereField) -> Result<(), Box<SqlParseError>> {
    if let Some(ref arithmetic) = field.arithmetic {
        check_arithmetic_for_aggregates(arithmetic)?;
    }
    Ok(())
}

fn check_arithmetic_for_aggregates(expr: &ArithmeticExpr) -> Result<(), Box<SqlParseError>> {
    match expr {
        ArithmeticExpr::Aggregate(func, col_ref) => {
            // Found aggregate function in WHERE clause which is invalid
            return Err(Box::new(SqlParseError::InvalidInput(format!(
                "Aggregate function '{}({})' cannot be used in WHERE clause",
                match func {
                    AggregateFunction::Max => "MAX",
                    AggregateFunction::Min => "MIN",
                    AggregateFunction::Avg => "AVG",
                    AggregateFunction::Sum => "SUM",
                    AggregateFunction::Count => "COUNT",
                },
                col_ref
            ))));
        }
        ArithmeticExpr::NestedExpr(left, _, right, _) => {
            check_arithmetic_for_aggregates(left)?;
            check_arithmetic_for_aggregates(right)?;
        }
        // Other cases (Column, Literal) don't contain aggregates
        _ => {}
    }
    Ok(())
}

fn validate_having_columns_in_group_by(ast: &SqlAST) -> Result<(), Box<SqlParseError>> {
    if let (Some(group_by), Some(having)) = (
        &ast.group_by,
        &ast.group_by.as_ref().and_then(|gb| gb.having.as_ref()),
    ) {
        let group_by_columns = &group_by.columns;
        validate_having_expr_columns(having, group_by_columns)?;
    }
    Ok(())
}

fn validate_having_expr_columns(
    having: &HavingClause,
    group_by_columns: &[ColumnRef],
) -> Result<(), Box<SqlParseError>> {
    match having {
        HavingClause::Base(base_condition) => match base_condition {
            HavingBaseCondition::Comparison(cond) => {
                validate_having_field(&cond.left_field, group_by_columns)?;
                validate_having_field(&cond.right_field, group_by_columns)?;
            }
            HavingBaseCondition::NullCheck(null_cond) => {
                validate_having_field(&null_cond.field, group_by_columns)?;
            }
            HavingBaseCondition::Exists(..) => { /*TODO */ }
            HavingBaseCondition::In(..) => { /*TODO */ }
            HavingBaseCondition::Boolean(_) => { /*TODO */ }
        },
        HavingClause::Expression { left, op: _, right } => {
            validate_having_expr_columns(left, group_by_columns)?;
            validate_having_expr_columns(right, group_by_columns)?;
        }
    }
    Ok(())
}

fn validate_having_field(
    field: &HavingField,
    group_by_columns: &[ColumnRef],
) -> Result<(), Box<SqlParseError>> {
    // Skip validation for literals or aggregates (they're always allowed)
    if field.value.is_some() || field.aggregate.is_some() {
        return Ok(());
    }

    // Check column references
    if let Some(ref col) = field.column {
        let is_in_group_by = group_by_columns.iter().any(|gb_col| {
            gb_col.column == col.column && (gb_col.table == col.table || col.table.is_none())
        });

        if !is_in_group_by {
            return Err(Box::new(SqlParseError::InvalidInput(format!(
                "Column '{}' in HAVING clause must be in GROUP BY or used in an aggregate function",
                if let Some(table) = &col.table {
                    format!("{}.{}", table, col.column)
                } else {
                    col.column.clone()
                }
            ))));
        }
    }

    // Check arithmetic expressions
    if let Some(ref arithmetic) = field.arithmetic {
        validate_arithmetic_columns(arithmetic, group_by_columns)?;
    }

    Ok(())
}

fn validate_arithmetic_columns(
    expr: &ArithmeticExpr,
    group_by_columns: &[ColumnRef],
) -> Result<(), Box<SqlParseError>> {
    match expr {
        ArithmeticExpr::Column(col_ref) => {
            // Check if this column is in the GROUP BY
            let is_in_group_by = group_by_columns.iter().any(|gb_col| {
                gb_col.column == col_ref.column
                    && (gb_col.table == col_ref.table || col_ref.table.is_none())
            });

            if !is_in_group_by {
                return Err(Box::new(SqlParseError::InvalidInput(
                               format!("Column '{}' in HAVING clause must be in GROUP BY or used in an aggregate function",
                                   col_ref
                              )
                           )));
            };
            Ok(())
        }
        ArithmeticExpr::Aggregate(_, _) => {
            Ok(())

            // Aggregates are always allowed in HAVING
        }
        ArithmeticExpr::Literal(_) => Ok(()),
        ArithmeticExpr::NestedExpr(left, _, right, _) => {
            validate_arithmetic_columns(left, group_by_columns)?;
            validate_arithmetic_columns(right, group_by_columns)?;
            Ok(())
        }
        ArithmeticExpr::Subquery(_) => {
            Ok(())
            /*TODO */
        }
    }
}

fn validate_limit_offset(clause: &Option<LimitClause>) -> Result<(), Box<SqlParseError>> {
    if let Some(limit_clause) = clause {
        // Check that LIMIT is non-negative
        if limit_clause.limit < 0 {
            return Err(Box::new(SqlParseError::InvalidInput(format!(
                "LIMIT value must be non-negative, got {}",
                limit_clause.limit
            ))));
        }

        // Check that OFFSET (if present) is non-negative
        if let Some(offset) = limit_clause.offset {
            if offset < 0 {
                return Err(Box::new(SqlParseError::InvalidInput(format!(
                    "OFFSET value must be non-negative, got {}",
                    offset
                ))));
            }
        }
    }
    Ok(())
}

// check that the subquery contains only one column in the select clause. other checks are made at runtime
fn validate_select_subquery(select_type: &SelectType) -> Result<(), Box<SqlParseError>> {
    match select_type {
        SelectType::ArithmeticExpr(expr) => {
            // Recursively validate any subqueries in arithmetic expressions
            match expr {
                ArithmeticExpr::Subquery(subquery) => {
                    if subquery.select.select.len() != 1 {
                        return Err(Box::new(SqlParseError::InvalidInput(
                            "Subquery in SELECT clause must return exactly one column".to_string(),
                        )));
                    }
                }
                ArithmeticExpr::NestedExpr(left, _, right, _) => {
                    // Check both sides of the expression for subqueries
                    validate_arithmetic_subquery(left)?;
                    validate_arithmetic_subquery(right)?;
                }
                _ => {}
            }
        }
        SelectType::Subquery(subquery) => {
            if subquery.select.select.len() != 1 {
                return Err(Box::new(SqlParseError::InvalidInput(
                    "Subquery in SELECT clause must return exactly one column".to_string(),
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_arithmetic_subquery(expr: &ArithmeticExpr) -> Result<(), Box<SqlParseError>> {
    match expr {
        ArithmeticExpr::Subquery(subquery) => {
            if subquery.select.select.len() != 1 {
                return Err(Box::new(SqlParseError::InvalidInput(
                    "Subquery in arithmetic expression must return exactly one column".to_string(),
                )));
            }
            Ok(())
        }
        ArithmeticExpr::NestedExpr(left, _, right, _) => {
            validate_arithmetic_subquery(left)?;
            validate_arithmetic_subquery(right)
        }
        _ => Ok(()),
    }
}
