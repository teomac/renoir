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
        let first = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing first part of condition".to_string()))?;

        match first.as_rule() {
            Rule::arithmetic_expr => {
                // Handle comparison condition
                let operator_pair = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing operator".to_string()))?;
                let right_expr = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing right expression".to_string()))?;

                let operator = match operator_pair.as_str() {
                    ">" => ComparisonOp::GreaterThan,
                    "<" => ComparisonOp::LessThan,
                    ">=" => ComparisonOp::GreaterThanEquals,
                    "<=" => ComparisonOp::LessThanEquals,
                    "==" | "=" => ComparisonOp::Equal,
                    "!=" => ComparisonOp::NotEqual,
                    op => return Err(AquaParseError::InvalidInput(format!("Invalid operator: {}", op))),
                };

                Ok(WhereConditionType::Comparison(Condition {
                    left_field: Self::parse_arithmetic_expr(first)?,
                    operator,
                    right_field: Self::parse_arithmetic_expr(right_expr)?,
                }))
            },
            Rule::qualified_column | Rule::identifier => {
                // Check if this is a NULL check
                let operator_pair = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing operator".to_string()))?;

                if operator_pair.as_rule() == Rule::null_op {
                    let operator = match operator_pair.as_str() {
                        "is null" => NullOp::IsNull,
                        "is not null" => NullOp::IsNotNull,
                        _ => return Err(AquaParseError::InvalidInput(
                            format!("Invalid null operator: {}", operator_pair.as_str())
                        )),
                    };

                    Ok(WhereConditionType::NullCheck(NullCondition {
                        field: Self::parse_field_reference(first)?,
                        operator,
                    }))
                } else {
                    Err(AquaParseError::InvalidInput("Expected null operator".to_string()))
                }
            },
            _ => Err(AquaParseError::InvalidInput(
                format!("Unexpected token in condition: {:?}", first.as_rule())
            )),
        }
    }

    fn parse_arithmetic_expr(pair: Pair<Rule>) -> Result<ComplexField, AquaParseError> {
        let mut inner = pair.into_inner();
        let first_term = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Empty arithmetic expression".to_string()))?;
        
        let mut result = Self::parse_arithmetic_term(first_term)?;
        
        // Process any additional operations (symbols and terms)
        while let Some(op) = inner.next() {
            if let Some(term) = inner.next() {
                let next_field = Self::parse_arithmetic_term(term)?;
                result = ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: None,
                    nested_expr: Some(Box::new((result, op.as_str().to_string(), next_field))),
                };
            }
        }
        
        Ok(result)
    }

    fn parse_arithmetic_term(pair: Pair<Rule>) -> Result<ComplexField, AquaParseError> {
        let inner = pair.clone().into_inner().next()
            .ok_or_else(|| AquaParseError::InvalidInput("Empty arithmetic term".to_string()))?;
                
        match inner.as_rule() {
            Rule::left_parenthesis => {
                // If we find a left parenthesis, we expect: left_parenthesis ~ arithmetic_expr ~ right_parenthesis
                let expr = pair.into_inner().nth(1) // Get the arithmetic_expr between parentheses
                    .ok_or_else(|| AquaParseError::InvalidInput("Empty parenthesized expression".to_string()))?;
                Self::parse_arithmetic_expr(expr)
            },
            Rule::arithmetic_operand => Self::parse_arithmetic_operand(inner),
            _ => Err(AquaParseError::InvalidInput(
                format!("Unexpected token in arithmetic term: {:?}", inner.as_rule())
            )),
        }
    }

    fn parse_arithmetic_operand(pair: Pair<Rule>) -> Result<ComplexField, AquaParseError> {
        let operand = pair.into_inner().next()
            .ok_or_else(|| AquaParseError::InvalidInput("Empty operand".to_string()))?;

        match operand.as_rule() {
            Rule::value => Ok(ComplexField {
                column_ref: None,
                literal: Some(Self::parse_literal(operand)?),
                aggregate: None,
                nested_expr: None,
            }),
            Rule::qualified_column => {
                let column_ref = Self::parse_qualified_column(operand)?;
                Ok(ComplexField {
                    column_ref: Some(column_ref),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                })
            },
            Rule::identifier => Ok(ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: operand.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
            }),
            Rule::aggregate_expr => {
                let agg_func = Self::parse_aggregate_function(operand)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: Some(agg_func),
                    nested_expr: None,
                })
            },
            _ => Err(AquaParseError::InvalidInput(
                format!("Invalid operand type: {:?}", operand.as_rule())
            )),
        }
    }

    fn parse_literal(pair: Pair<Rule>) -> Result<AquaLiteral, AquaParseError> {
        match pair.as_rule() {
            Rule::value => {
                let inner = pair.into_inner().next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Empty value".to_string()))?;
                
                match inner.as_rule() {
                    Rule::string => {
                        // Remove the single quotes and store the inner content
                        let inner_str = inner.as_str();
                        let clean_str = inner_str[1..inner_str.len()-1].to_string();
                        Ok(AquaLiteral::String(clean_str))
                    },
                    Rule::number => {
                        // Try to parse as integer first, then as float
                        inner.as_str().parse::<i64>()
                            .map(AquaLiteral::Integer)
                            .or_else(|_| inner.as_str().parse::<f64>()
                                .map(AquaLiteral::Float))
                            .map_err(|_| AquaParseError::InvalidInput("Invalid number".to_string()))
                    },
                    Rule::boolean_keyword => {
                        match inner.as_str() {
                            "true" => Ok(AquaLiteral::Boolean(true)),
                            "false" => Ok(AquaLiteral::Boolean(false)),
                            _ => Err(AquaParseError::InvalidInput("Invalid boolean value".to_string()))
                        }
                    },
                    _ => Err(AquaParseError::InvalidInput(format!("Invalid literal type: {:?}", inner.as_rule())))
                }
            },
            _ => Err(AquaParseError::InvalidInput("Expected value".to_string()))
        }
    }

    fn parse_qualified_column(pair: Pair<Rule>) -> Result<ColumnRef, AquaParseError> {
        let mut inner = pair.into_inner();
        let table = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing table in qualified column".to_string()))?
            .as_str()
            .to_string();
        
        let column = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing column in qualified column".to_string()))?
            .as_str()
            .to_string();

        Ok(ColumnRef {
            table: Some(table),
            column,
        })
    }

    fn parse_aggregate_function(pair: Pair<Rule>) -> Result<AggregateFunction, AquaParseError> {
        let mut inner = pair.into_inner();
        
        let func_type = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing aggregate function type".to_string()))?;
        
        let function = match func_type.as_str() {
            "max" => AggregateType::Max,
            "min" => AggregateType::Min,
            "avg" => AggregateType::Avg,
            "sum" => AggregateType::Sum,
            "count" => AggregateType::Count,
            _ => return Err(AquaParseError::InvalidInput(
                format!("Invalid aggregate function: {}", func_type.as_str())
            )),
        };

        let column_ref = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing column in aggregate function".to_string()))?;

        // Handle special case for COUNT(*)
        let column = if column_ref.as_str() == "*" {
            ColumnRef {
                table: None,
                column: "*".to_string(),
            }
        } else {
            match column_ref.as_rule() {
                Rule::qualified_column => Self::parse_qualified_column(column_ref)?,
                Rule::identifier => ColumnRef {
                    table: None,
                    column: column_ref.as_str().to_string(),
                },
                _ => return Err(AquaParseError::InvalidInput(
                    format!("Invalid column reference in aggregate: {:?}", column_ref.as_rule())
                )),
            }
        };

        Ok(AggregateFunction {
            function,
            column,
        })
    }

    fn parse_field_reference(pair: Pair<Rule>) -> Result<ComplexField, AquaParseError> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let col_ref = Self::parse_qualified_column(pair)?;
                Ok(ComplexField {
                    column_ref: Some(col_ref),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                })
            },
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
            },
            _ => Err(AquaParseError::InvalidInput(
                format!("Expected field reference, got {:?}", pair.as_rule())
            )),
        }
    }
}