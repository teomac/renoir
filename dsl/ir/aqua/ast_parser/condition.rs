use pest::iterators::Pair;
use super::ir_ast_structure::*;
use super::error::AquaParseError;
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

    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<WhereConditionType, AquaParseError> {
        let mut inner = condition_pair.into_inner();
        
        // Get the first field
        let left_field_pair = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing variable in condition".to_string()))?;
    
        let left_field = Self::parse_condition_field(left_field_pair)?;
        
        // Get the operator - could be comparison or null
        let operator_pair = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing operator".to_string()))?;
    
        match operator_pair.as_rule() {
            Rule::null_op => {
                // Handle IS NULL / IS NOT NULL
                let operator = match operator_pair.as_str().to_uppercase().as_str() {
                    "IS NULL" => NullOp::IsNull,
                    "IS NOT NULL" => NullOp::IsNotNull,
                    _ => return Err(AquaParseError::InvalidInput(
                        format!("Invalid null operator: {}", operator_pair.as_str())
                    )),
                };
    
                Ok(WhereConditionType::NullCheck(NullCondition {
                    field: left_field,
                    operator,
                }))
            },
            Rule::comparison_op => {
                // Handle regular comparison operators
                let operator = match operator_pair.as_str() {
                    ">" => ComparisonOp::GreaterThan,
                    "<" => ComparisonOp::LessThan,
                    ">=" => ComparisonOp::GreaterThanEquals,
                    "<=" => ComparisonOp::LessThanEquals,
                    "==" | "=" => ComparisonOp::Equal,
                    "!=" => ComparisonOp::NotEqual,
                    op => return Err(AquaParseError::InvalidInput(format!("Invalid operator: {}", op))),
                };
    
                let right_field_pair = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing value in condition".to_string()))?;
    
                let right_field = Self::parse_condition_field(right_field_pair)?;
    
                Ok(WhereConditionType::Comparison(Condition {
                    left_field,
                    operator,
                    right_field,
                }))
            },
            _ => Err(AquaParseError::InvalidInput("Expected operator".to_string())),
        }
    }

    fn parse_condition_field(pair: Pair<Rule>) -> Result<ComplexField, AquaParseError> {
        match pair.as_rule() {
            Rule::value => {
                //we try to parse it as a number
                let value = pair.as_str().parse::<i64>()
                    .map(AquaLiteral::Integer)
                    .unwrap_or_else(|_| {
                        //if it fails, we try to parse as float
                        pair.as_str().parse::<f64>()
                            .map(AquaLiteral::Float)
                            .unwrap_or_else(|_| {
                                //parse as boolean
                                match pair.as_str() {
                                    "true" => AquaLiteral::Boolean(true),
                                    "false" => AquaLiteral::Boolean(false),
                                    _ => {
                                        //if it fails, we return as string
                                        AquaLiteral::String(pair.as_str().to_string())
                                    }
                                }
                            })
                    });
                Ok(ComplexField{
                    column_ref: None,
                    literal: Some(value),
                    aggregate: None,
                    nested_expr: None,
                })

            }
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
                Ok(ComplexField {
                    column_ref: Some(ColumnRef {
                        table: Some(stream),
                        column: field,
                    }),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                })
            }
            Rule::identifier => {
                Ok(ComplexField {
                    column_ref: Some(ColumnRef {
                        table: None,
                        column: pair.as_str().to_string(),
                    }),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                })
            }
            _ => Err(AquaParseError::InvalidInput(
                format!("Expected field reference, got {:?}", pair.as_rule())
            )),
        }
    }
}