use super::error::IrParseError;
use super::{ir_ast_structure::*, IrParser};
use super::literal::LiteralParser;
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pair;

pub struct ConditionParser;

impl ConditionParser {
    pub fn parse(pair: Pair<Rule>) -> Result<FilterClause, Box<IrParseError>> {
        let mut inner = pair.into_inner();

        // Skip 'where' keyword if present
        if inner.peek().map_or(false, |p| p.as_str() == "where") {
            inner.next();
        }

        let conditions = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing filter conditions".to_string()))?;

        Self::parse_conditions(conditions)
    }

    pub fn parse_conditions(conditions_pair: Pair<Rule>) -> Result<FilterClause, Box<IrParseError>> {
        let mut pairs = conditions_pair.into_inner().peekable();

        let first = pairs
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Expected condition".to_string()))?;

        let mut left = match first.as_rule() {
            Rule::filter_term => Self::parse_term(first)?,
            Rule::condition => Self::parse_single_condition(first)?,
            _ => {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Unexpected rule: {:?}",
                    first.as_rule()
                ))))
            }
        };

        // Process any binary operations - Now uses && and || instead of AND/OR
        while let Some(op) = pairs.next() {
            let op = match op.as_str() {
                "&&" => BinaryOp::And,
                "||" => BinaryOp::Or,
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Invalid binary operator: {}",
                        op.as_str()
                    ))))
                }
            };

            let right_term = pairs.next().ok_or_else(|| {
                IrParseError::InvalidInput("Expected right term after operator".to_string())
            })?;

            let right = match right_term.as_rule() {
                Rule::filter_term => Self::parse_term(right_term)?,
                Rule::condition => Self::parse_single_condition(right_term)?,
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Unexpected rule: {:?}",
                        right_term.as_rule()
                    ))))
                }
            };

            left = FilterClause::Expression {
                left: Box::new(left),
                binary_op: op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_term(pair: Pair<Rule>) -> Result<FilterClause, Box<IrParseError>> {
        let mut inner = pair.into_inner();

        let first = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty term".to_string()))?;

        match first.as_rule() {
            Rule::left_parenthesis => {
                // After left_parenthesis we expect where_conditions
                let conditions = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Empty parentheses".to_string()))?;
                Self::parse_conditions(conditions)
            }
            Rule::condition => Self::parse_single_condition(first),
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Invalid term: {:?}",
                first.as_rule()
            )))),
        }
    }

    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<FilterClause, Box<IrParseError>> {
        let mut inner = condition_pair.into_inner();

        // Get the first field
        let first = inner.next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing first part of condition".to_string())
        })?;

        match first.as_rule() {
            Rule::arithmetic_expr => {
                // Handle comparison condition
                let operator_pair = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing operator".to_string()))?;
                let right_expr = inner.next().ok_or_else(|| {
                    IrParseError::InvalidInput("Missing right expression".to_string())
                })?;

                let operator = match operator_pair.as_str() {
                    ">" => ComparisonOp::GreaterThan,
                    "<" => ComparisonOp::LessThan,
                    ">=" => ComparisonOp::GreaterThanEquals,
                    "<=" => ComparisonOp::LessThanEquals,
                    "==" => ComparisonOp::Equal, // Changed from = to ==
                    "!=" => ComparisonOp::NotEqual,
                    op => {
                        return Err(Box::new(IrParseError::InvalidInput(format!(
                            "Invalid operator: {}",
                            op
                        ))))
                    }
                };

                Ok(FilterClause::Base(FilterConditionType::Comparison(
                    Condition {
                        left_field: Self::parse_arithmetic_expr(first)?,
                        operator,
                        right_field: Self::parse_arithmetic_expr(right_expr)?,
                    },
                )))
            }
            Rule::qualified_column | Rule::identifier | Rule::number | Rule::subquery => {
                // Check if this is a NULL check
                let operator_pair = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing operator".to_string()))?;

                if operator_pair.as_rule() == Rule::null_op {
                    let operator = match operator_pair.as_str() {
                        "is null" => NullOp::IsNull,
                        "is not null" => NullOp::IsNotNull,
                        _ => {
                            return Err(Box::new(IrParseError::InvalidInput(format!(
                                "Invalid null operator: {}",
                                operator_pair.as_str()
                            ))))
                        }
                    };

                    Ok(FilterClause::Base(FilterConditionType::NullCheck(
                        NullCondition {
                            field: Self::parse_field_reference(first)?,
                            operator,
                        },
                    )))
                } else {
                    Err(Box::new(IrParseError::InvalidInput(
                        "Expected null operator".to_string(),
                    )))
                }
            }
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Unexpected token in condition: {:?}",
                first.as_rule()
            )))),
        }
    }

    fn parse_arithmetic_expr(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        let mut inner = pair.into_inner();
        let first_term = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty arithmetic expression".to_string()))?;

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
                    subquery: None,
                };
            }
        }

        Ok(result)
    }

    fn parse_arithmetic_term(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        let inner = pair
            .clone()
            .into_inner()
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty arithmetic term".to_string()))?;

        match inner.as_rule() {
            Rule::left_parenthesis => {
                // If we find a left parenthesis, we expect: left_parenthesis ~ arithmetic_expr ~ right_parenthesis
                let expr = pair
                    .into_inner()
                    .nth(1) // Get the arithmetic_expr between parentheses
                    .ok_or_else(|| {
                        IrParseError::InvalidInput("Empty parenthesized expression".to_string())
                    })?;
                Self::parse_arithmetic_expr(expr)
            }
            Rule::arithmetic_factor => Self::parse_arithmetic_operand(inner),
            Rule::subquery => {
                let subquery = IrParser::parse_subquery(inner)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: Some(subquery),
                })
            }
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Unexpected token in arithmetic term: {:?}",
                inner.as_rule()
            )))),
        }
    }

    fn parse_arithmetic_operand(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        let operand = pair
            .into_inner()
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty operand".to_string()))?;

        match operand.as_rule() {
            Rule::value => Ok(ComplexField {
                column_ref: None,
                literal: Some(Self::parse_literal(operand)?),
                aggregate: None,
                nested_expr: None,
                subquery: None,
            }),
            Rule::qualified_column => {
                let column_ref = Self::parse_qualified_column(operand)?;
                Ok(ComplexField {
                    column_ref: Some(column_ref),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                })
            }
            Rule::identifier => Ok(ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: operand.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
            }),
            Rule::aggregate_expr => {
                let agg_func = Self::parse_aggregate_function(operand)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: Some(agg_func),
                    nested_expr: None,
                    subquery: None,
                })
            }
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Invalid operand type: {:?}",
                operand.as_rule()
            )))),
        }
    }

    fn parse_literal(pair: Pair<Rule>) -> Result<IrLiteral, Box<IrParseError>> {
        match pair.as_rule() {
            Rule::value => {
                let inner = pair
                    .into_inner()
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Empty value".to_string()))?;

                match inner.as_rule() {
                    Rule::string => {
                        // Remove the single quotes and store the inner content
                        let inner_str = inner.as_str();
                        let clean_str = inner_str[1..inner_str.len() - 1].to_string();
                        Ok(IrLiteral::String(clean_str))
                    }
                    Rule::number => {
                        // Try to parse as integer first, then as float
                        Ok(inner
                            .as_str()
                            .parse::<i64>()
                            .map(IrLiteral::Integer)
                            .or_else(|_| inner.as_str().parse::<f64>().map(IrLiteral::Float))
                            .map_err(|_| IrParseError::InvalidInput("Invalid number".to_string()))?)
                    }
                    Rule::boolean_keyword => match inner.as_str() {
                        "true" => Ok(IrLiteral::Boolean(true)),
                        "false" => Ok(IrLiteral::Boolean(false)),
                        _ => Err(Box::new(IrParseError::InvalidInput(
                            "Invalid boolean value".to_string(),
                        ))),
                    },
                    _ => Err(Box::new(IrParseError::InvalidInput(format!(
                        "Invalid literal type: {:?}",
                        inner.as_rule()
                    )))),
                }
            }
            _ => Err(Box::new(IrParseError::InvalidInput("Expected value".to_string()))),
        }
    }

    fn parse_qualified_column(pair: Pair<Rule>) -> Result<ColumnRef, Box<IrParseError>> {
        let mut inner = pair.into_inner();
        let table = inner
            .next()
            .ok_or_else(|| {
                IrParseError::InvalidInput("Missing table in qualified column".to_string())
            })?
            .as_str()
            .to_string();

        let column = inner
            .next()
            .ok_or_else(|| {
                IrParseError::InvalidInput("Missing column in qualified column".to_string())
            })?
            .as_str()
            .to_string();

        Ok(ColumnRef {
            table: Some(table),
            column,
        })
    }

    fn parse_aggregate_function(pair: Pair<Rule>) -> Result<AggregateFunction, Box<IrParseError>> {
        let mut inner = pair.into_inner();

        let func_type = inner.next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing aggregate function type".to_string())
        })?;

        let function = match func_type.as_str() {
            "max" => AggregateType::Max,
            "min" => AggregateType::Min,
            "avg" => AggregateType::Avg,
            "sum" => AggregateType::Sum,
            "count" => AggregateType::Count,
            _ => {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Invalid aggregate function: {}",
                    func_type.as_str()
                ))))
            }
        };

        let column_ref = inner.next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing column in aggregate function".to_string())
        })?;

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
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Invalid column reference in aggregate: {:?}",
                        column_ref.as_rule()
                    ))))
                }
            }
        };

        Ok(AggregateFunction { function, column })
    }

    fn parse_field_reference(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let col_ref = Self::parse_qualified_column(pair)?;
                Ok(ComplexField {
                    column_ref: Some(col_ref),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                })
            }
            Rule::identifier => Ok(ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
            }),
            Rule::number => {
                let num = LiteralParser::parse(pair.as_str())
                    .map_err(|e| IrParseError::InvalidInput(e.to_string()))?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: Some(num),
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                })
            }
            Rule::subquery => {
                let subquery = IrParser::parse_subquery(pair)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: Some(subquery),
                })
            }
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Expected field reference, got {:?}",
                pair.as_rule()
            )))),
        }
    }
}
