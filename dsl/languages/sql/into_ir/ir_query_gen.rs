use crate::dsl::languages::sql::ast_parser::sql_ast_structure::*;

pub struct SqlToIr;

impl SqlToIr {
    pub fn convert(sql_ast: &SqlAST) -> String {
        let mut parts = Vec::new();

        // FROM clause
        let from_str = Self::from_clause_to_string(&sql_ast.from);
        parts.push(from_str);

        // WHERE clause (if present)
        if let Some(where_clause) = &sql_ast.filter {
            parts.push(format!(
                "where {}",
                Self::where_clause_to_string(where_clause)
            ));
        }

        // GROUP BY clause (if present)
        if let Some(group_by_clause) = &sql_ast.group_by {
            parts.push(format!(
                "group {}",
                Self::group_by_clause_to_string(group_by_clause)
            ));
        }

        // SELECT clause - handle multiple columns
        let select_keyword = if sql_ast.select.distinct {
            "select_distinct"
        } else {
            "select"
        };

        let select_strs: Vec<String> = sql_ast
            .select
            .select
            .iter()
            .map(|select_clause| {
                let selection_str = match &select_clause.selection {
                    SelectType::Simple(col_ref) => col_ref.to_string(),
                    SelectType::Aggregate(func, col_ref) => {
                        let agg = match func {
                            AggregateFunction::Max => "max",
                            AggregateFunction::Min => "min",
                            AggregateFunction::Sum => "sum",
                            AggregateFunction::Avg => "avg",
                            AggregateFunction::Count => "count",
                        };
                        format!("{}({})", agg, col_ref.to_string())
                    }
                    SelectType::ComplexValue(left, op, right) => format!(
                        "{} {} {}",
                        Self::convert_complex_field(left),
                        op,
                        Self::convert_complex_field(right)
                    )
                    .trim()
                    .to_string(),
                    SelectType::StringLiteral(val) => format!("'{}'", val),
                    // Handle subquery in SELECT
                    SelectType::Subquery(subquery) => format!("({})", Self::convert(subquery)),
                };

                // Add alias if present
                if let Some(alias) = &select_clause.alias {
                    format!("{} as {}", selection_str, alias)
                } else {
                    selection_str
                }
            })
            .collect();

        parts.push(format!("{} {}", select_keyword, select_strs.join(", ")));

        if let Some(order_by_clause) = &sql_ast.order_by {
            parts.push(format!(
                "order {}",
                Self::order_by_clause_to_string(order_by_clause)
            ));
        }

        // Add LIMIT clause (if present)
        if let Some(limit_clause) = &sql_ast.limit {
            let mut limit_str = format!("limit {}", limit_clause.limit);
            if let Some(offset) = limit_clause.offset {
                limit_str.push_str(&format!(" offset {}", offset));
            }
            parts.push(limit_str);
        }

        parts.join("\n")
    }

    // Helper method to handle the FROM clause with subqueries
    fn from_clause_to_string(from_clause: &FromClause) -> String {
        let mut from_str = match &from_clause.scan {
            FromSource::Table(scan_clause) => match &scan_clause.alias {
                Some(alias) => format!("from {} as {} in input1", scan_clause.variable, alias),
                None => format!("from {} in input1", scan_clause.variable),
            },
            FromSource::Subquery(subquery, alias) => {
                let subquery_str = Self::convert(subquery);
                match alias {
                    Some(alias_name) => format!("from ({}) as {} in input1", subquery_str, alias_name),
                    None => format!("from ({}) in input1", subquery_str),
                }
            }
        };

        // iterate over join(s)
        if let Some(joins) = &from_clause.joins {
            for (i, join) in joins.iter().enumerate() {
                let input_num = i + 2;
                
                let join_source = match &join.join_scan {
                    FromSource::Table(scan_clause) => match &scan_clause.alias {
                        Some(alias) => format!("{} as {}", scan_clause.variable, alias),
                        None => scan_clause.variable.clone(),
                    },
                    FromSource::Subquery(subquery, alias) => {
                        let subquery_str = Self::convert(subquery);
                        match alias {
                            Some(alias_name) => format!("({}) as {}", subquery_str, alias_name),
                            None => format!("({})", subquery_str),
                        }
                    }
                };

                // Create all join conditions
                let conditions: Vec<String> = join
                    .join_expr
                    .conditions
                    .iter()
                    .map(|cond| format!("{} == {}", cond.left_var, cond.right_var))
                    .collect();

                let join_type_str = match join.join_type {
                    JoinType::Inner => "", // Default join is inner, so no prefix needed
                    JoinType::Left => "left ",
                    JoinType::Outer => "outer ",
                };

                from_str.push_str(&format!(
                    " {}join {} in input{} on {}",
                    join_type_str,
                    join_source,
                    input_num,
                    conditions.join(" && ")
                ));
            }
        }
        
        from_str
    }

    // Converts a WHERE clause from the SQL AST to its equivalent IR string representation.
    fn where_clause_to_string(clause: &WhereClause) -> String {
        match clause {
            WhereClause::Base(base_condition) => match base_condition {
                WhereBaseCondition::Comparison(cond) => {
                    let left = Self::convert_where_field(&cond.left_field);
                    let right = Self::convert_where_field(&cond.right_field);

                    let op = match cond.operator {
                        ComparisonOp::Equal => "==",
                        ComparisonOp::NotEqual => "!=",
                        ComparisonOp::GreaterThan => ">",
                        ComparisonOp::LessThan => "<",
                        ComparisonOp::GreaterOrEqualThan => ">=",
                        ComparisonOp::LessOrEqualThan => "<=",
                    };

                    format!("{} {} {}", left, op, right)
                }
                WhereBaseCondition::NullCheck(null_cond) => {
                    let field = Self::convert_where_field(&null_cond.field);
                    let op = match null_cond.operator {
                        NullOp::IsNull => "is null",
                        NullOp::IsNotNull => "is not null",
                    };
                    format!("{} {}", field, op)
                },
                // Handle EXISTS subquery
                WhereBaseCondition::Exists(subquery, negated) => {
                    let subquery_str = Self::convert(subquery);
                    if *negated {
                        format!("not exists({})", subquery_str)
                    } else {
                        format!("exists({})", subquery_str)
                    }
                },
                // Handle IN subquery
                WhereBaseCondition::In(column, subquery, negated) => {
                    let column_str = column.to_string();
                    let subquery_str = Self::convert(subquery);
                    if *negated {
                        format!("{} not in ({})", column_str, subquery_str)
                    } else {
                        format!("{} in ({})", column_str, subquery_str)
                    }
                }
            },
            WhereClause::Expression { left, op, right } => {
                let op_str = match op {
                    BinaryOp::And => "&&",
                    BinaryOp::Or => "||",
                };

                // Look for the specific patterns that need parentheses
                let left_needs_parens = matches!(
                    **left,
                    WhereClause::Expression {
                        op: BinaryOp::Or,
                        ..
                    }
                );
                let right_needs_parens = matches!(
                    **right,
                    WhereClause::Expression {
                        op: BinaryOp::Or,
                        ..
                    }
                );

                let left_str = if left_needs_parens {
                    format!("({})", Self::where_clause_to_string(left))
                } else {
                    Self::where_clause_to_string(left)
                };

                let right_str = if right_needs_parens {
                    format!("({})", Self::where_clause_to_string(right))
                } else {
                    Self::where_clause_to_string(right)
                };

                format!("{} {} {}", left_str, op_str, right_str)
            }
        }
    }

    /// Converts a WhereField to its string representation in IR format.
    fn convert_where_field(field: &WhereField) -> String {
        if let Some(ref arithmetic) = field.arithmetic {
            match arithmetic {
                ArithmeticExpr::BinaryOp(_left, _op, _right) => {
                    // Add parentheses around binary operations
                    format!("({})", Self::arithmetic_expr_to_string(arithmetic))
                }
                _ => Self::arithmetic_expr_to_string(arithmetic),
            }
        } else if let Some(ref column) = field.column {
            column.to_string()
        } else if let Some(ref value) = field.value {
            match value {
                SqlLiteral::Float(val) => format!("{:.2}", val),
                SqlLiteral::Integer(val) => val.to_string(),
                SqlLiteral::String(val) => format!("'{}'", val),
                SqlLiteral::Boolean(val) => val.to_string(),
            }
        } else if let Some(ref subquery) = field.subquery {
            format!("({})", Self::convert(subquery))
        } else {
            String::new()
        }
    }

    /// Converts an arithmetic expression to its string representation in IR format.
    fn arithmetic_expr_to_string(expr: &ArithmeticExpr) -> String {
        match expr {
            ArithmeticExpr::Column(col_ref) => col_ref.to_string(),
            ArithmeticExpr::Literal(lit) => match lit {
                SqlLiteral::Float(val) => format!("{:.2}", val),
                SqlLiteral::Integer(val) => val.to_string(),
                SqlLiteral::String(val) => format!("'{}'", val),
                SqlLiteral::Boolean(val) => val.to_string(),
            },
            ArithmeticExpr::Aggregate(func, col_ref) => {
                let agg = match func {
                    AggregateFunction::Max => "max",
                    AggregateFunction::Min => "min",
                    AggregateFunction::Sum => "sum",
                    AggregateFunction::Avg => "avg",
                    AggregateFunction::Count => "count",
                };
                format!("{}({})", agg, col_ref.to_string())
            },
            ArithmeticExpr::BinaryOp(left, op, right) => {
                let left_str = Self::arithmetic_expr_to_string(left);
                let right_str = Self::arithmetic_expr_to_string(right);
                format!("{} {} {}", left_str, op, right_str)
            },
            // Handle subquery in arithmetic expression
            ArithmeticExpr::Subquery(subquery) => {
                format!("({})", Self::convert(subquery))
            }
        }
    }

    // Updated group_by_clause_to_string to handle new having structure
    fn group_by_clause_to_string(clause: &GroupByClause) -> String {
        let mut group_by_str = String::new();

        // Handle group by columns
        let group_by_columns = clause.columns.clone();
        for (i, col) in group_by_columns.iter().enumerate() {
            group_by_str.push_str(&col.to_string());
            if i < group_by_columns.len() - 1 {
                group_by_str.push_str(", ");
            }
        }

        // Add having clause if present
        if let Some(having) = &clause.having {
            group_by_str.push_str(" {");
            group_by_str.push_str(&Self::having_clause_to_string(having));
            group_by_str.push('}');
        }

        group_by_str
    }

    // Updated method to handle the recursive having clause structure with subqueries
    fn having_clause_to_string(clause: &HavingClause) -> String {
        match clause {
            HavingClause::Base(base_condition) => match base_condition {
                HavingBaseCondition::Comparison(cond) => {
                    let left = if let Some(ref arithmetic) = cond.left_field.arithmetic {
                        Self::arithmetic_expr_to_string(arithmetic)
                    } else if cond.left_field.column.is_some() {
                        cond.left_field.column.as_ref().unwrap().to_string()
                    } else if cond.left_field.aggregate.is_some() {
                        let aggregate = match cond.left_field.aggregate.as_ref().unwrap().0 {
                            AggregateFunction::Max => "max",
                            AggregateFunction::Min => "min",
                            AggregateFunction::Sum => "sum",
                            AggregateFunction::Avg => "avg",
                            AggregateFunction::Count => "count",
                        };
                        format!(
                            "{}({})",
                            aggregate,
                            cond.left_field.aggregate.as_ref().unwrap().1.to_string()
                        )
                    } else if let Some(ref subquery) = cond.left_field.subquery {
                        format!("({})", Self::convert(subquery))
                    } else {
                        match &cond.left_field.value {
                            Some(SqlLiteral::Float(val)) => format!("{:.2}", val),
                            Some(SqlLiteral::Integer(val)) => val.to_string(),
                            Some(SqlLiteral::String(val)) => format!("'{}'", val),
                            Some(SqlLiteral::Boolean(val)) => val.to_string(),
                            None => String::new(),
                        }
                    };

                    let operator_str = match cond.operator {
                        ComparisonOp::GreaterThan => ">",
                        ComparisonOp::LessThan => "<",
                        ComparisonOp::GreaterOrEqualThan => ">=",
                        ComparisonOp::LessOrEqualThan => "<=",
                        ComparisonOp::Equal => "==",
                        ComparisonOp::NotEqual => "!=",
                    };

                    let right = if let Some(ref arithmetic) = cond.right_field.arithmetic {
                        Self::arithmetic_expr_to_string(arithmetic)
                    } else if cond.right_field.column.is_some() {
                        cond.right_field.column.as_ref().unwrap().to_string()
                    } else if cond.right_field.aggregate.is_some() {
                        let aggregate = match cond.right_field.aggregate.as_ref().unwrap().0 {
                            AggregateFunction::Max => "max",
                            AggregateFunction::Min => "min",
                            AggregateFunction::Sum => "sum",
                            AggregateFunction::Avg => "avg",
                            AggregateFunction::Count => "count",
                        };
                        format!(
                            "{}({})",
                            aggregate,
                            cond.right_field.aggregate.as_ref().unwrap().1.to_string()
                        )
                    } else if let Some(ref subquery) = cond.right_field.subquery {
                        format!("({})", Self::convert(subquery))
                    } else {
                        match &cond.right_field.value {
                            Some(SqlLiteral::Float(val)) => format!("{:.2}", val),
                            Some(SqlLiteral::Integer(val)) => val.to_string(),
                            Some(SqlLiteral::String(val)) => format!("'{}'", val),
                            Some(SqlLiteral::Boolean(val)) => val.to_string(),
                            None => String::new(),
                        }
                    };

                    format!("{} {} {}", left, operator_str, right)
                }
                HavingBaseCondition::NullCheck(null_cond) => {
                    let field = if let Some(ref arithmetic) = null_cond.field.arithmetic {
                        Self::arithmetic_expr_to_string(arithmetic)
                    } else if let Some(ref column) = null_cond.field.column {
                        column.to_string()
                    } else if let Some(ref subquery) = null_cond.field.subquery {
                        format!("({})", Self::convert(subquery))
                    } else {
                        String::new()
                    };

                    match null_cond.operator {
                        NullOp::IsNull => format!("{} is null", field),
                        NullOp::IsNotNull => format!("{} is not null", field),
                    }
                },
                // Handle EXISTS in HAVING
                HavingBaseCondition::Exists(subquery) => {
                    format!("exists({})", Self::convert(subquery))
                },
                // Handle IN in HAVING
                HavingBaseCondition::In(column, subquery) => {
                    format!("{} in ({})", column.to_string(), Self::convert(subquery))
                }
            },
            HavingClause::Expression { left, op, right } => {
                let op_str = match op {
                    BinaryOp::And => "&&",
                    BinaryOp::Or => "||",
                };

                // Handle parentheses for nested expressions
                let left_needs_parens = matches!(
                    **left,
                    HavingClause::Expression {
                        op: BinaryOp::Or,
                        ..
                    }
                );
                let right_needs_parens = matches!(
                    **right,
                    HavingClause::Expression {
                        op: BinaryOp::Or,
                        ..
                    }
                );

                let left_str = if left_needs_parens {
                    format!("({})", Self::having_clause_to_string(left))
                } else {
                    Self::having_clause_to_string(left)
                };

                let right_str = if right_needs_parens {
                    format!("({})", Self::having_clause_to_string(right))
                } else {
                    Self::having_clause_to_string(right)
                };

                format!("{} {} {}", left_str, op_str, right_str)
            }
        }
    }

    fn order_by_clause_to_string(clause: &OrderByClause) -> String {
        let items: Vec<String> = clause
            .items
            .iter()
            .map(|item| {
                let col_str = match &item.column.table {
                    Some(table) => format!("{}.{}", table, item.column.column),
                    None => item.column.column.clone(),
                };

                match item.direction {
                    OrderDirection::Asc => col_str,
                    OrderDirection::Desc => format!("{} desc", col_str),
                }
            })
            .collect();

        items.join(", ")
    }

    //function used to convert complex field to string - updated to handle subqueries
    fn convert_complex_field(field: &ComplexField) -> String {
        if let Some(ref nested) = field.nested_expr {
            // Handle nested expression
            let (left_field, op, right_field) = &**nested;
            return format!(
                "({} {} {})",
                Self::convert_complex_field(left_field),
                op,
                Self::convert_complex_field(right_field)
            );
        }

        if let Some(ref col_ref) = field.column_ref {
            col_ref.to_string()
        } else if let Some(ref lit) = field.literal {
            match lit {
                SqlLiteral::Float(val) => format!("{:.2}", val),
                SqlLiteral::Integer(val) => val.to_string(),
                SqlLiteral::String(val) => format!("'{}'", val),
                SqlLiteral::Boolean(val) => val.to_string(),
            }
        } else if let Some((agg_func, col_ref)) = &field.aggregate {
            let agg = match agg_func {
                AggregateFunction::Max => "max",
                AggregateFunction::Min => "min",
                AggregateFunction::Sum => "sum",
                AggregateFunction::Avg => "avg",
                AggregateFunction::Count => "count",
            };
            format!("{}({})", agg, col_ref.to_string())
        } else if let Some(ref subquery) = field.subquery {
            format!("({})", Self::convert(subquery))
        } else {
            String::new()
        }
    }
}