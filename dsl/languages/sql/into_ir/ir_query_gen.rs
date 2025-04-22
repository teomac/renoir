use crate::dsl::languages::sql::ast_parser::sql_ast_structure::*;

pub struct SqlToIr;

// index is used to propagate the stream number to the subqueries
impl SqlToIr {
    pub fn convert(sql_ast: &SqlAST, index: &mut usize, nested_index: usize) -> String {
        let mut parts = Vec::new();

        // FROM clause
        let from_str = Self::from_clause_to_string(&sql_ast.from, index, nested_index);
        parts.push(from_str);

        // WHERE clause (if present)
        if let Some(where_clause) = &sql_ast.filter {
            parts.push(format!(
                "where {}",
                Self::where_clause_to_string(where_clause, index, nested_index)
            ));
        }

        // GROUP BY clause (if present)
        if let Some(group_by_clause) = &sql_ast.group_by {
            parts.push(format!(
                "group {}",
                Self::group_by_clause_to_string(group_by_clause, index, nested_index)
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
                        format!("{}({})", agg, col_ref)
                    }
                    SelectType::ArithmeticExpr(expr) => {
                        Self::arithmetic_expr_to_string(expr, index, nested_index)
                    }
                    SelectType::StringLiteral(val) => format!("'{}'", val),
                    SelectType::Subquery(subquery) => {
                        format!("({})", Self::convert(subquery, index, nested_index + 1))
                    }
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
    fn from_clause_to_string(
        from_clause: &FromClause,
        stream_index: &mut usize,
        nested_index: usize,
    ) -> String {
        let mut from_str = match &from_clause.scan {
            FromSource::Table(scan_clause) => match &scan_clause.alias {
                Some(alias) => format!(
                    "from {} as {} in {}{}",
                    scan_clause.variable,
                    alias,
                    Self::get_stream_prefix(nested_index),
                    stream_index
                ),
                None => format!(
                    "from {} in {}{}",
                    scan_clause.variable,
                    Self::get_stream_prefix(nested_index),
                    stream_index
                ),
            },
            FromSource::Subquery(subquery, alias) => {
                let subquery_str = Self::convert(subquery, stream_index, nested_index + 1);
                match alias {
                    Some(alias_name) => format!(
                        "from ({}) as {} in {}{}",
                        subquery_str,
                        alias_name,
                        Self::get_stream_prefix(nested_index),
                        stream_index
                    ),
                    None => format!(
                        "from ({}) in {}{}",
                        subquery_str,
                        Self::get_stream_prefix(nested_index),
                        stream_index
                    ),
                }
            }
        };

        *stream_index += 1;

        // iterate over join(s)
        if let Some(joins) = &from_clause.joins {
            for join in joins.iter() {
                let join_source = match &join.join_scan {
                    FromSource::Table(scan_clause) => match &scan_clause.alias {
                        Some(alias) => format!("{} as {}", scan_clause.variable, alias),
                        None => scan_clause.variable.clone(),
                    },
                    FromSource::Subquery(subquery, alias) => {
                        let subquery_str = Self::convert(subquery, stream_index, nested_index + 1);
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
                    " {}join {} in {}{} on {}",
                    join_type_str,
                    join_source,
                    Self::get_stream_prefix(nested_index),
                    stream_index,
                    conditions.join(" && ")
                ));

                *stream_index += 1;
            }
        }

        from_str
    }

    // Converts a WHERE clause from the SQL AST to its equivalent IR string representation.
    fn where_clause_to_string(
        clause: &WhereClause,
        index: &mut usize,
        nested_index: usize,
    ) -> String {
        match clause {
            WhereClause::Base(base_condition) => match base_condition {
                WhereBaseCondition::Comparison(cond) => {
                    let left = Self::convert_where_field(&cond.left_field, index, nested_index);
                    let right = Self::convert_where_field(&cond.right_field, index, nested_index);

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
                    let field = Self::convert_where_field(&null_cond.field, index, nested_index);
                    let op = match null_cond.operator {
                        NullOp::IsNull => "is null",
                        NullOp::IsNotNull => "is not null",
                    };
                    format!("{} {}", field, op)
                }
                WhereBaseCondition::Exists(subquery, negated) => {
                    let subquery_str = Self::convert(subquery, index, nested_index + 1);
                    if *negated {
                        format!("not exists({})", subquery_str)
                    } else {
                        format!("exists({})", subquery_str)
                    }
                }
                WhereBaseCondition::In(condition) => match condition {
                    InCondition::InWhere(field, subquery, negated) => {
                        let field_str = Self::convert_where_field(field, index, nested_index);
                        let subquery_str = Self::convert(subquery, index, nested_index + 1);
                        if *negated {
                            format!("{} not in ({})", field_str, subquery_str)
                        } else {
                            format!("{} in ({})", field_str, subquery_str)
                        }
                    }

                    InCondition::InSubquery(column, subquery, negated) => {
                        let subquery_in = Self::convert(column, index, nested_index + 1);
                        let subquery_str = Self::convert(subquery, index, nested_index + 1);
                        if *negated {
                            format!("({}) not in ({})", subquery_in, subquery_str)
                        } else {
                            format!("({}) in ({})", subquery_in, subquery_str)
                        }
                    }
                    InCondition::InHaving(..) => {
                        // Handle InHaving condition if needed
                        panic!("We cannot have InHaving condition in the WHERE clause")
                    }
                },

                WhereBaseCondition::Boolean(boolean) => boolean.to_string(),
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
                    format!(
                        "({})",
                        Self::where_clause_to_string(left, index, nested_index)
                    )
                } else {
                    Self::where_clause_to_string(left, index, nested_index)
                };

                let right_str = if right_needs_parens {
                    format!(
                        "({})",
                        Self::where_clause_to_string(right, index, nested_index)
                    )
                } else {
                    Self::where_clause_to_string(right, index, nested_index)
                };

                format!("{} {} {}", left_str, op_str, right_str)
            }
        }
    }

    /// Converts a WhereField to its string representation in IR format.
    fn convert_where_field(field: &WhereField, index: &mut usize, nested_index: usize) -> String {
        if let Some(ref arithmetic) = field.arithmetic {
            match arithmetic {
                ArithmeticExpr::NestedExpr(_left, _op, _right, _) => {
                    // Add parentheses around binary operations

                    Self::arithmetic_expr_to_string(arithmetic, index, nested_index)
                }
                _ => Self::arithmetic_expr_to_string(arithmetic, index, nested_index),
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
            format!("({})", Self::convert(subquery, index, nested_index + 1))
        } else {
            String::new()
        }
    }

    /// Converts an arithmetic expression to its string representation in IR format.
    fn arithmetic_expr_to_string(
        expr: &ArithmeticExpr,
        index: &mut usize,
        nested_index: usize,
    ) -> String {
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
                format!("{}({})", agg, col_ref)
            }
            ArithmeticExpr::NestedExpr(left, op, right, is_parenthesized) => {
                let left_str = Self::arithmetic_expr_to_string(left, index, nested_index);
                let right_str = Self::arithmetic_expr_to_string(right, index, nested_index);

                let expr = format!("{} {} {}", left_str, op, right_str);
                if *is_parenthesized {
                    format!("({})", expr)
                } else {
                    expr
                }
            }
            ArithmeticExpr::Subquery(subquery) => {
                format!("({})", Self::convert(subquery, index, nested_index + 1))
            }
        }
    }

    // Updated group_by_clause_to_string to handle new having structure
    fn group_by_clause_to_string(
        clause: &GroupByClause,
        index: &mut usize,
        nested_index: usize,
    ) -> String {
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
            group_by_str.push_str(&Self::having_clause_to_string(having, index, nested_index));
            group_by_str.push('}');
        }

        group_by_str
    }

    // Updated method to handle the recursive having clause structure with subqueries
    fn having_clause_to_string(
        clause: &HavingClause,
        index: &mut usize,
        nested_index: usize,
    ) -> String {
        match clause {
            HavingClause::Base(base_condition) => match base_condition {
                HavingBaseCondition::Comparison(cond) => {
                    let left = if let Some(ref arithmetic) = cond.left_field.arithmetic {
                        Self::arithmetic_expr_to_string(arithmetic, index, nested_index)
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
                            cond.left_field.aggregate.as_ref().unwrap().1
                        )
                    } else if let Some(ref subquery) = cond.left_field.subquery {
                        format!("({})", Self::convert(subquery, index, nested_index + 1))
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
                        Self::arithmetic_expr_to_string(arithmetic, index, nested_index)
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
                            cond.right_field.aggregate.as_ref().unwrap().1
                        )
                    } else if let Some(ref subquery) = cond.right_field.subquery {
                        format!("({})", Self::convert(subquery, index, nested_index + 1))
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
                        Self::arithmetic_expr_to_string(arithmetic, index, nested_index)
                    } else if let Some(ref column) = null_cond.field.column {
                        column.to_string()
                    } else if let Some(ref subquery) = null_cond.field.subquery {
                        format!("({})", Self::convert(subquery, index, nested_index + 1))
                    } else {
                        String::new()
                    };

                    match null_cond.operator {
                        NullOp::IsNull => format!("{} is null", field),
                        NullOp::IsNotNull => format!("{} is not null", field),
                    }
                }
                HavingBaseCondition::Exists(subquery, negated) => {
                    let subquery_str = Self::convert(subquery, index, nested_index + 1);
                    if *negated {
                        format!("not exists({})", subquery_str)
                    } else {
                        format!("exists({})", subquery_str)
                    }
                }
                HavingBaseCondition::In(condition) => match condition {
                    InCondition::InSubquery(in_subquery, subquery, negated) => {
                        let in_subquery_str = Self::convert(in_subquery, index, nested_index + 1);
                        let subquery_str = Self::convert(subquery, index, nested_index + 1);
                        if *negated {
                            format!("({}) not in ({})", in_subquery_str, subquery_str)
                        } else {
                            format!("({}) in ({})", in_subquery_str, subquery_str)
                        }
                    }
                    InCondition::InHaving(field, subquery, negated) => {
                        let field_str = if let Some(ref arithmetic) = field.arithmetic {
                            Self::arithmetic_expr_to_string(arithmetic, index, nested_index)
                        } else if let Some(ref column) = field.column {
                            column.to_string()
                        } else if let Some(ref aggregate) = field.aggregate {
                            let agg_func = match aggregate.0 {
                                AggregateFunction::Max => "max",
                                AggregateFunction::Min => "min",
                                AggregateFunction::Sum => "sum",
                                AggregateFunction::Avg => "avg",
                                AggregateFunction::Count => "count",
                            };
                            format!("{}({})", agg_func, aggregate.1)
                        } else {
                            match &field.value {
                                Some(SqlLiteral::Float(val)) => format!("{:.2}", val),
                                Some(SqlLiteral::Integer(val)) => val.to_string(),
                                Some(SqlLiteral::String(val)) => format!("'{}'", val),
                                Some(SqlLiteral::Boolean(val)) => val.to_string(),
                                None => String::new(),
                            }
                        };

                        let subquery_str = Self::convert(subquery, index, nested_index + 1);
                        if *negated {
                            format!("{} not in ({})", field_str, subquery_str)
                        } else {
                            format!("{} in ({})", field_str, subquery_str)
                        }
                    }
                    InCondition::InWhere(..) => {
                        panic!("We cannot have InWhere condition in the HAVING clause")
                    }
                },
                HavingBaseCondition::Boolean(boolean) => boolean.to_string(),
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
                    format!(
                        "({})",
                        Self::having_clause_to_string(left, index, nested_index)
                    )
                } else {
                    Self::having_clause_to_string(left, index, nested_index)
                };

                let right_str = if right_needs_parens {
                    format!(
                        "({})",
                        Self::having_clause_to_string(right, index, nested_index)
                    )
                } else {
                    Self::having_clause_to_string(right, index, nested_index)
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

    fn get_stream_prefix(index: usize) -> String {
        match index {
            0 => "stream".to_string(),
            1 => "substream".to_string(),
            2 => "subsubstream".to_string(),
            n => format!("sub{}stream", "sub".repeat(n)),
        }
    }
}
