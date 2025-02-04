use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::SqlParseError;
use super::literal::LiteralParser;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct ConditionParser;

impl ConditionParser {
    pub fn parse(pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut inner = pair.into_inner();
        inner.next(); // Skip WHERE keyword
        
        let conditions = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing where conditions".to_string()))?;
        
        Self::parse_conditions(conditions)
    }

    pub fn parse_conditions(conditions_pair: Pair<Rule>) -> Result<WhereClause, SqlParseError> {
        let mut pairs = conditions_pair.into_inner().peekable();
        
        let first_condition = pairs.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing condition".to_string()))?;
        let mut current = WhereClause {
            condition: Self::parse_single_condition(first_condition)?,
            binary_op: None,
            next: None
        };
        
        let mut last = &mut current;
        
        while let Some(op_pair) = pairs.next() {
            if let Some(condition_pair) = pairs.next() {
                let op = match op_pair.as_str().to_uppercase().as_str() {
                    "AND" => BinaryOp::And,
                    "OR" => BinaryOp::Or,
                    _ => return Err(SqlParseError::InvalidInput("Invalid binary operator".to_string())),
                };
                
                last.binary_op = Some(op);
                last.next = Some(Box::new(WhereClause {
                    condition: Self::parse_single_condition(condition_pair)?,
                    binary_op: None,
                    next: None,
                }));
                
                if let Some(ref mut next) = last.next {
                    last = next;
                }
            }
        }
        
        Ok(current)
    }

    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<Condition, SqlParseError> {
        let mut inner = condition_pair.into_inner();
        
        let col_ref_pair = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing variable in condition".to_string()))?;
        
        // Parse the column reference
        let variable = Self::parse_column_ref(col_ref_pair)?;
            
        let operator = match inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator in condition".to_string()))?
            .as_str() 
        {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            ">=" => ComparisonOp::GreaterOrEqualThan,
            "<=" => ComparisonOp::LessOrEqualThan,
            "=" => ComparisonOp::Equal,
            "!=" | "<>" => ComparisonOp::NotEqual,
            op => return Err(SqlParseError::InvalidInput(format!("Invalid operator: {}", op))),
        };

        let value_str = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing value in condition".to_string()))?
            .as_str();
        let value = LiteralParser::parse(value_str)?;

        Ok(Condition {
            variable,
            operator,
            value,
        })
    }

    // New helper function to parse column references
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, SqlParseError> {
        match pair.as_rule() {
            Rule::table_column => {
                let mut inner = pair.into_inner();
                let table = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }
            Rule::variable => {
                Ok(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                })
            }
            _ => Err(SqlParseError::InvalidInput(format!("Expected column reference, got {:?}", pair.as_rule()))),
        }
    }
}