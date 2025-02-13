use crate::dsl::languages::sql::ast_parser::sql_ast_structure::GroupByClause;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::HavingConditionType;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::WhereConditionType;
use crate::dsl::languages::sql::ast_parser::*;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::SqlLiteral;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::BinaryOp;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::ComparisonOp;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::NullOp;
use crate::dsl::languages::sql::ast_parser::sql_ast_structure::ComplexField;

pub struct SqlToAqua;

impl SqlToAqua {
    pub fn convert(sql_ast: &SqlAST) -> String {
        let mut parts = Vec::new();

        // FROM clause
        let mut from_str = match &sql_ast.from.scan.alias {
            Some(alias) => format!("from {} as {} in input1", sql_ast.from.scan.variable, alias),
            None => format!("from {} in input1", sql_ast.from.scan.variable),
        };

        // iterate over join(s)
        for (i, join) in sql_ast.from.joins.clone().unwrap().iter().enumerate() {
            let input_num = i + 2; // input1 is used by base table, so joins start from input2
            let join_table = match &join.join_scan.alias {
                Some(alias) => format!("{} as {}", join.join_scan.variable, alias),
                None => join.join_scan.variable.clone(),
            };

            from_str.push_str(&format!(
                " join {} in input{} on {} == {}",
                join_table, input_num, join.join_expr.left_var, join.join_expr.right_var
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

        parts.join("\n")
    }

    fn where_clause_to_string(clause: &WhereClause) -> String {
        let mut current = clause;
        let mut conditions = Vec::new();

        // Process first condition
        conditions.push(Self::condition_to_string(&current.condition));

        // Process remaining conditions
        while let (Some(op), Some(next)) = (&current.binary_op, &current.next) {
            let op_str = match op {
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
            };
            conditions.push(op_str.to_string());
            conditions.push(Self::condition_to_string(&next.condition));
            current = &*next;
        }

        conditions.join(" ")
    }

    fn condition_to_string(condition: &WhereConditionType) -> String {
        match condition {
            WhereConditionType::Comparison(cond) => {
                let left = if cond.left_field.column.is_some() {
                    cond.left_field.column.as_ref().unwrap().to_string()
                } else {
                    match &cond.left_field.value {
                        Some(SqlLiteral::Float(val)) => format!("{:.2}", val),
                        Some(SqlLiteral::Integer(val)) => val.to_string(),
                        Some(SqlLiteral::String(val)) => val.clone(),
                        Some(SqlLiteral::Boolean(val)) => val.to_string(),
                        None => String::new(),
                    }
                };

                let operator_str = match cond.operator {
                    ComparisonOp::GreaterThan => ">",
                    ComparisonOp::LessThan => "<",
                    ComparisonOp::GreaterOrEqualThan => ">=",
                    ComparisonOp::LessOrEqualThan => "<=",
                    ComparisonOp::Equal => "=",
                    ComparisonOp::NotEqual => "!=",
                };

                let right = if cond.right_field.column.is_some() {
                    cond.right_field.column.as_ref().unwrap().to_string()
                } else {
                    match &cond.right_field.value {
                        Some(SqlLiteral::Float(val)) => format!("{:.2}", val),
                        Some(SqlLiteral::Integer(val)) => val.to_string(),
                        Some(SqlLiteral::String(val)) => val.clone(),
                        Some(SqlLiteral::Boolean(val)) => val.to_string(),
                        None => String::new(),
                    }
                };

                format!("{} {} {}", left, operator_str, right)
            }
            WhereConditionType::NullCheck(null_cond) => {
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
                        Some(SqlLiteral::String(val)) => val.clone(),
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
                        Some(SqlLiteral::String(val)) => val.clone(),
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
                SqlLiteral::String(val) => val.clone(),
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
