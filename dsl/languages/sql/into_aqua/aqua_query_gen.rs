use crate::dsl::languages::sql::ast_parser::*;
use crate::dsl::languages::sql::ast_parser::ast_structure::SqlLiteral;
use crate::dsl::languages::sql::ast_parser::ast_structure::BinaryOp;
use crate::dsl::languages::sql::ast_parser::ast_structure::ComparisonOp;

pub struct SqlToAqua;

impl SqlToAqua {
    pub fn convert(sql_ast: &SqlAST) -> String {
        let mut parts = Vec::new();
        
        // FROM clause
        let mut from_str = match &sql_ast.from.scan.alias {
            Some(alias) => format!("from {} as {} in input1", sql_ast.from.scan.variable, alias),
            None => format!("from {} in input1", sql_ast.from.scan.variable),
        };
        
        // Add JOIN if present
        if let Some(join) = &sql_ast.from.join {
            let join_table = match &join.join_scan.alias {
                Some(alias) => format!("{} as {}", join.join_scan.variable, alias),
                None => join.join_scan.variable.clone(),
            };
            
            from_str.push_str(&format!(" join {} in input2 on {} == {}", 
                join_table,
                join.join_expr.left_var,
                join.join_expr.right_var
            ));
        }
        
        parts.push(from_str);

        // WHERE clause (if present)
        if let Some(where_clause) = &sql_ast.filter {
            parts.push(format!("where {}", Self::where_clause_to_string(where_clause)));
        }

        // SELECT clause
        let select_str = match &sql_ast.select.selection {
            SelectType::Simple(col_ref) => {
                format!("select {}", col_ref.to_string())
            },
            SelectType::Aggregate(func, col_ref) => {
                let agg = match func {
                    AggregateFunction::Max => "max",
                };
                format!("select {}({})", agg, col_ref.to_string())
            },
            SelectType::ComplexValue(col_ref, op, val) => {
                let value = match val {
                    SqlLiteral::Float(val) => format!("{:.2}", val),
                    SqlLiteral::Integer(val) => val.to_string(),
                    SqlLiteral::String(val) => val.clone(),
                    SqlLiteral::Boolean(val) => val.to_string(),
                };
                format!("select {} {} {}", col_ref.to_string(), op, value)
            }
        };
        parts.push(select_str);

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
            current = next;
        }
        
        conditions.join(" ")
    }

    fn condition_to_string(condition: &Condition) -> String {
        let operator_str = match condition.operator {
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::LessThan => "<",
            ComparisonOp::GreaterOrEqualThan => ">=",
            ComparisonOp::LessOrEqualThan => "<=",
            ComparisonOp::Equal => "==",
            ComparisonOp::NotEqual => "!=",
        };

        let value_str = match &condition.value {
            SqlLiteral::Float(val) => format!("{:.2}", val),
            SqlLiteral::Integer(val) => val.to_string(),
            SqlLiteral::String(val) => val.clone(),
            SqlLiteral::Boolean(val) => val.to_string(),
        };

        format!(
            "{} {} {}",
            condition.variable.to_string(),
            operator_str,
            value_str
        )
    }
}