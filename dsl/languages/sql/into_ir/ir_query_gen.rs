use crate::dsl::languages::sql::ast_parser::sql_ast_structure::GroupByClause;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::WhereField;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::WhereBaseCondition;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::HavingConditionType;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::{OrderByClause, OrderDirection};
use crate::dsl::languages::sql::ast_parser::*;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::SqlLiteral;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::BinaryOp;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::ComparisonOp;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::NullOp;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::ComplexField;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::ArithmeticExpr;

pub struct SqlToIr;

impl SqlToIr {
    pub fn convert(sql_ast: &SqlAST) -> String {
        let mut parts = Vec::new();

        // FROM clause
        let mut from_str = match &sql_ast.from.scan.alias {
            Some(alias) => format!("from {} as {} in input1", sql_ast.from.scan.variable, alias),
            None => format!("from {} in input1", sql_ast.from.scan.variable),
        };

        // iterate over join(s)
        for (i, join) in sql_ast.from.joins.clone().unwrap().iter().enumerate() {
            let input_num = i + 2;
            let join_table = match &join.join_scan.alias {
                Some(alias) => format!("{} as {}", join.join_scan.variable, alias),
                None => join.join_scan.variable.clone(),
            };
        
            // Create all join conditions
            let conditions: Vec<String> = join.join_expr.conditions.iter()
                .map(|cond| format!("{} == {}", cond.left_var, cond.right_var))
                .collect();
        
            from_str.push_str(&format!(
                " join {} in input{} on {}",
                join_table, 
                input_num, 
                conditions.join(" && ")
            ));
        }

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
        let select_strs: Vec<String> = sql_ast.select.iter().map(|select_clause| {
            let selection_str = match &select_clause.selection {
                SelectType::Simple(col_ref) => {
                    col_ref.to_string()
                },
                SelectType::Aggregate(func, col_ref) => {
                    let agg = match func {
                        AggregateFunction::Max => "max",
                        AggregateFunction::Min => "min",
                        AggregateFunction::Sum => "sum",
                        AggregateFunction::Avg => "avg",
                        AggregateFunction::Count => "count",
                    };
                    format!("{}({})", agg, col_ref.to_string())
                },
                SelectType::ComplexValue(left, op, right) => {
                    format!("{} {} {}", 
                    Self::convert_complex_field(left),
                    op,
                    Self::convert_complex_field(right)
                ).trim().to_string()
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

        parts.push(format!("select {}", select_strs.join(", ")));

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

// Converts a WHERE clause from the SQL AST to its equivalent IR string representation.
/// This function handles nested expressions, comparison operations, and NULL checks
/// while maintaining proper operator precedence.
///
/// # Arguments
/// * `clause` - The WhereClause AST node to convert
///
/// # Returns
/// A String containing the IR representation of the WHERE clause
///
/// # Examples
/// ```text
/// SQL Input: "WHERE (a > 10 OR b < 20) AND c = 30"
/// IR Output: "a > 10 || b < 20 && c == 30"
///
/// SQL Input: "WHERE x IS NULL AND (y > 100 OR z <= 50)"
/// IR Output: "x is null && (y > 100 || z <= 50)"
/// ```
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
            },
            WhereBaseCondition::NullCheck(null_cond) => {
                let field = Self::convert_where_field(&null_cond.field);
                let op = match null_cond.operator {
                    NullOp::IsNull => "is null",
                    NullOp::IsNotNull => "is not null",
                };
                format!("{} {}", field, op)
            }
        },
        WhereClause::Expression { left, op, right } => {
            let op_str = match op {
                BinaryOp::And => "&&",
                BinaryOp::Or => "||",
            };

            // Look for the specific patterns that need parentheses
            let left_needs_parens = matches!(**left, WhereClause::Expression { op: BinaryOp::Or, .. });
            let right_needs_parens = matches!(**right, WhereClause::Expression { op: BinaryOp::Or, .. });

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
/// This function handles fields that can contain column references,
/// literal values, or arithmetic expressions.
///
/// # Arguments
/// * `field` - The WhereField to convert
///
/// # Returns
/// A String containing the IR representation of the field
///
/// # Examples
/// ```text
/// Column Reference: "table1.column1" -> "table1.column1"
/// Literal Value: 42 -> "42"
/// Arithmetic: "a + b" -> "a + b"
/// ```
fn convert_where_field(field: &WhereField) -> String {
    if let Some(ref arithmetic) = field.arithmetic {
        match arithmetic {
            ArithmeticExpr::BinaryOp(_left, _op, _right) => {
                // Add parentheses around binary operations
                format!("({})", Self::arithmetic_expr_to_string(arithmetic))
            },
            _ => Self::arithmetic_expr_to_string(arithmetic)
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
    } else {
        String::new()
    }
}

/// Converts an arithmetic expression to its string representation in IR format.
/// Handles column references, literals, aggregate functions, and binary operations.
///
/// # Arguments
/// * `expr` - The ArithmeticExpr to convert
///
/// # Returns
/// A String containing the IR representation of the arithmetic expression
///
/// # Examples
/// ```text
/// Column: "table1.column1" -> "table1.column1"
/// Binary Op: "a + b" -> "a + b"
/// Aggregate: "SUM(x)" -> "sum(x)"
/// Complex: "(a + b) * c" -> "(a + b) * c"
/// ```
///
/// # Notes
/// - Maintains operator precedence using parentheses where necessary
/// - Converts aggregate function names to lowercase as required by IR
/// - Preserves spacing around operators for readability
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
        }
    }
}


    //function used to parse the group by clause
    fn group_by_clause_to_string(clause: &GroupByClause) -> String {
        let current = clause;

        let mut group_by_str = String::new();
        let group_by_columns = current.columns.clone();

        // append to group by string all the columns in the group by clause
        for i in 0..group_by_columns.len() {
            group_by_str.push_str(&group_by_columns[i].to_string());
            if i < group_by_columns.len() - 1 {
                group_by_str.push_str(", ");
            }
        }

        //if there is no having clause, return the group by string
        if current.having.is_none() {
            group_by_str
        } else {
            //parse having conditions
            let mut new_current = current.having.clone().unwrap();

            let mut conditions = Vec::new();

            // Process first condition
            conditions.push(Self::having_condition_to_string(&new_current.condition));

            // Process remaining conditions
            while let (Some(op), Some(next)) = (new_current.binary_op, new_current.next) {
                let op_str = match op {
                    BinaryOp::And => "AND",
                    BinaryOp::Or => "OR",
                };
                conditions.push(op_str.to_string());
                conditions.push(Self::having_condition_to_string(&next.condition));
                new_current = *next;
            }

            conditions.join(" ");

            group_by_str.push_str(&format!(" {{{}}}", conditions.join(" ")));

            group_by_str
        }
    }

    fn having_condition_to_string(condition: &HavingConditionType) -> String {
        match condition {
            HavingConditionType::Comparison(cond) => {
                let left = if cond.left_field.column.is_some() {
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

                let right = if cond.right_field.column.is_some() {
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
            HavingConditionType::NullCheck(null_cond) => {
                let field = if null_cond.field.column.is_some() {
                    null_cond.field.column.as_ref().unwrap().to_string()
                } else {
                    String::new()
                };

                match null_cond.operator {
                    NullOp::IsNull => format!("{} is null", field),
                    NullOp::IsNotNull => format!("{} is not null", field),
                }
            }
        }
    }

    fn order_by_clause_to_string(clause: &OrderByClause) -> String {
        let items: Vec<String> = clause.items.iter()
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

    //function used to convert complex field to string
    fn convert_complex_field(field: &ComplexField) -> String {
        if let Some(ref nested) = field.nested_expr {
            // Handle nested expression
            let (left_field, op, right_field) = &**nested;
            return format!("({} {} {})", 
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
        } else {
            String::new()
        }
    }
}
