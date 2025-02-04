use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::AquaParseError;
use super::literal::LiteralParser;
use crate::dsl::ir::aqua::ast_parser::Rule;

pub struct ConditionParser;

impl ConditionParser {
    pub fn parse(pair: Pair<Rule>) -> Result<WhereClause, AquaParseError> {
        let mut inner = pair.into_inner();
        
        // Skip 'where' keyword if present
        if inner.peek().map_or(false, |p| p.as_str() == "where") {
            inner.next();
        }
        
        let conditions = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing where conditions".to_string()))?;
        
        Self::parse_conditions(conditions)
    }

    pub fn parse_conditions(conditions_pair: Pair<Rule>) -> Result<WhereClause, AquaParseError> {
        let mut pairs = conditions_pair.into_inner().peekable();
        
        let first_condition = pairs.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing condition".to_string()))?;
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
                    _ => return Err(AquaParseError::InvalidInput(
                        format!("Invalid binary operator: {}", op_pair.as_str())
                    )),
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

    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<Condition, AquaParseError> {
        let mut inner = condition_pair.into_inner();
        
        // Parse left side (always a column reference in a condition)
        let col_ref_pair = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing field reference in condition".to_string()))?;
        
        // Parse the column reference
        let variable = Self::parse_column_ref(col_ref_pair)?;
            
        // Parse the operator
        let operator = match inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing operator in condition".to_string()))?
            .as_str() 
        {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            ">=" => ComparisonOp::GreaterThanEquals,
            "<=" => ComparisonOp::LessThanEquals,
            "==" => ComparisonOp::Equal,
            "!=" => ComparisonOp::NotEqual,
            "=" => ComparisonOp::Equal,
            op => return Err(AquaParseError::InvalidInput(format!("Invalid operator: {}", op))),
        };

        // Parse the value (right side)
        let value_pair = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing value in condition".to_string()))?;
        
            let value = match value_pair.as_rule() {
                Rule::boolean_keyword => AquaLiteral::Boolean(value_pair.as_str().to_lowercase() == "true" || value_pair.as_str().to_lowercase() =="false"),
                Rule::qualified_column => AquaLiteral::ColumnRef(Self::parse_column_ref(value_pair)?),
                Rule::identifier => AquaLiteral::String(value_pair.as_str().to_string()),
                Rule::number => LiteralParser::parse(value_pair.as_str())?,
                _ => LiteralParser::parse(value_pair.as_str())?
            };

        Ok(Condition {
            variable,
            operator,
            value,
        })
    }

    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, AquaParseError> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let stream = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing stream name".to_string()))?
                    .as_str()
                    .to_string();
                let field = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing field name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(stream),
                    column: field,
                })
            }
            Rule::identifier => {
                Ok(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                })
            }
            _ => Err(AquaParseError::InvalidInput(
                format!("Expected field reference, got {:?}", pair.as_rule())
            )),
        }
    }
}