use super::error::IrParseError;
use super::{ir_ast_structure::*, IrParser};
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pair;

pub struct GroupParser;

impl GroupParser {
    pub fn parse(
        pair: Pair<Rule>,
    ) -> Result<(Vec<ColumnRef>, Option<GroupClause>), Box<IrParseError>> {
        let mut inner = pair.into_inner();

        inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing group keyword".to_string()))?;

        // Get the group by list
        let group_list = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing group columns".to_string()))?;

        let mut columns = Vec::new();
        let mut group_condition = None;

        // Process group columns first
        for item in group_list.into_inner() {
            columns.push(Self::parse_column_ref(item)?);
        }

        if columns.is_empty() {
            return Err(Box::new(IrParseError::InvalidInput(
                "Empty group clause".to_string(),
            )));
        }

        // Check for having condition (inside curly braces)
        if let Some(condition) = inner.next() {
            group_condition = Some(Self::parse_group_conditions(condition)?);
        }

        Ok((columns, group_condition))
    }

    //////////////////////////////////////////////////////////////////////////////////

    //function to parse having conditions
    fn parse_group_conditions(pair: Pair<Rule>) -> Result<GroupClause, Box<IrParseError>> {
        // Get the content between curly braces
        let mut inner = pair.into_inner();
        let group_expr = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty group condition".to_string()))?;

        Self::parse_group_expr(group_expr)
    }

    fn parse_group_expr(pair: Pair<Rule>) -> Result<GroupClause, Box<IrParseError>> {
        let mut pairs = pair.into_inner().peekable();

        let first = pairs
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Expected group term".to_string()))?;

        let mut left = Self::parse_group_term(first)?;

        // Process any binary operations
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

            let right = Self::parse_group_term(right_term)?;

            left = GroupClause::Expression {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_group_term(pair: Pair<Rule>) -> Result<GroupClause, Box<IrParseError>> {
        match pair.as_rule() {
            Rule::having_term => {
                let mut inner = pair.into_inner();
                let first = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Empty term".to_string()))?;

                match first.as_rule() {
                    Rule::left_parenthesis => {
                        // Get group_expr between parentheses
                        let expr = inner.next().ok_or_else(|| {
                            IrParseError::InvalidInput("Empty parentheses".to_string())
                        })?;
                        Self::parse_group_expr(expr)
                    }
                    Rule::condition => Self::parse_single_condition(first),
                    _ => Err(Box::new(IrParseError::InvalidInput(format!(
                        "Invalid term type: {:?}",
                        first.as_rule()
                    )))),
                }
            }
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Expected having_term, got {:?}",
                pair.as_rule()
            )))),
        }
    }

    fn parse_single_condition(
        condition_pair: Pair<Rule>,
    ) -> Result<GroupClause, Box<IrParseError>> {
        let mut inner = condition_pair.into_inner();

        // Get the first field
        let first = inner.next().ok_or_else(|| {
            IrParseError::InvalidInput("Missing first part of condition".to_string())
        })?;

        match first.as_rule() {
            Rule::boolean_keyword => {
                let value = match first.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return Err(Box::new(IrParseError::InvalidInput(
                            "Invalid boolean value".to_string(),
                        )))
                    }
                };

                Ok(GroupClause::Base(GroupBaseCondition::Boolean(value)))
            }
            Rule::arithmetic_expr => {
                //two cases: null check or comparison
                let operator_pair = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing operator".to_string()))?;

                if operator_pair.as_rule() == Rule::null_op {
                    //null check case
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

                    Ok(GroupClause::Base(GroupBaseCondition::NullCheck(
                        NullCondition {
                            field: Self::parse_field_reference(first)?,
                            operator,
                        },
                    )))
                } else {
                    //comparison case
                    let right_expr = inner.next().ok_or_else(|| {
                        IrParseError::InvalidInput("Missing right expression".to_string())
                    })?;

                    let operator = match operator_pair.as_str() {
                        ">" => ComparisonOp::GreaterThan,
                        "<" => ComparisonOp::LessThan,
                        ">=" => ComparisonOp::GreaterThanEquals,
                        "<=" => ComparisonOp::LessThanEquals,
                        "==" => ComparisonOp::Equal,
                        "!=" => ComparisonOp::NotEqual,
                        op => {
                            return Err(Box::new(IrParseError::InvalidInput(format!(
                                "Invalid operator: {}",
                                op
                            ))))
                        }
                    };

                    Ok(GroupClause::Base(GroupBaseCondition::Comparison(
                        Condition {
                            left_field: Self::parse_arithmetic_expr(first)?,
                            operator,
                            right_field: Self::parse_arithmetic_expr(right_expr)?,
                        },
                    )))
                }
            }
            Rule::in_expr => {
                let mut inner = first.into_inner();

                // Parse first expression - could be arithmetic_expr or subquery
                let first_expr = inner.next().ok_or_else(|| {
                    IrParseError::InvalidInput("Missing expression in IN condition".to_string())
                })?;

                let field = if first_expr.as_rule() == Rule::subquery {
                    // If it's a subquery, create ComplexField with subquery
                    ComplexField {
                        subquery: Some(IrParser::parse_subquery(first_expr)?),
                        column_ref: None,
                        literal: None,
                        aggregate: None,
                        nested_expr: None,
                        subquery_vec: None,
                    }
                } else if first_expr.as_rule() == Rule::arithmetic_expr {
                    // Parse arithmetic expression
                    Self::parse_arithmetic_expr(first_expr)?
                } else {
                    return Err(Box::new(IrParseError::InvalidInput(
                        "Invalid expression in IN condition".to_string(),
                    )));
                };

                // Check for NOT (it's optional)
                let is_negated = if let Some(token) = inner.next() {
                    token.as_str().to_lowercase() == "not"
                } else {
                    false
                };

                // If we found NOT, we need to skip past it to get to IN
                if is_negated {
                    inner.next(); // Skip the IN keyword
                } else {
                    // The token we got wasn't NOT, it was IN, so we don't need to skip again
                }

                // Parse the subquery
                let subquery = inner.next().ok_or_else(|| {
                    IrParseError::InvalidInput("Missing subquery in IN expression".to_string())
                })?;
                let subquery_plan = IrParser::parse_subquery(subquery)?;

                Ok(GroupClause::Base(GroupBaseCondition::In(
                    InCondition::InSubquery {
                        field,
                        subquery: subquery_plan,
                        negated: is_negated,
                    },
                )))
            }
            Rule::exists_keyword => {
                // Check if this is "not exists" or just "exists"
                let is_negated = first.as_str().to_lowercase().starts_with("not");

                // Get the subquery expression
                let subquery_expr = inner.next().ok_or_else(|| {
                    IrParseError::InvalidInput("Missing subquery in EXISTS clause".to_string())
                })?;

                // Parse the subquery
                let subquery = IrParser::parse_subquery(subquery_expr)?;

                Ok(GroupClause::Base(GroupBaseCondition::Exists(
                    subquery, is_negated,
                )))
            }
            Rule::qualified_column | Rule::identifier | Rule::subquery => {
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

                    Ok(GroupClause::Base(GroupBaseCondition::NullCheck(
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
                "Invalid condition type: {:?}",
                first.as_rule()
            )))),
        }
    }

    fn parse_arithmetic_expr(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        let mut inner_pairs = pair.into_inner();

        // Parse the first operand
        let first_pair = inner_pairs
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty arithmetic expression".to_string()))?;

        // Start with the first operand
        let mut result = match first_pair.as_rule() {
            Rule::arithmetic_par => Self::parse_parenthesized_expr(first_pair)?,
            Rule::arithmetic_factor => Self::parse_arithmetic_factor(first_pair)?,
            _ => {
                return Err(Box::new(IrParseError::InvalidInput(format!(
                    "Expected arithmetic_par or arithmetic_factor, got {:?}",
                    first_pair.as_rule()
                ))))
            }
        };

        // Process any additional operations
        while let (Some(op), Some(next_operand)) = (inner_pairs.next(), inner_pairs.next()) {
            let operator = op.as_str().to_string();

            let next_field = match next_operand.as_rule() {
                Rule::arithmetic_par => Self::parse_parenthesized_expr(next_operand)?,
                Rule::arithmetic_factor => Self::parse_arithmetic_factor(next_operand)?,
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Expected arithmetic_par or arithmetic_factor, got {:?}",
                        next_operand.as_rule()
                    ))))
                }
            };

            // Combine into a nested expression
            result = ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: Some(Box::new((result, operator, next_field, false))),
                subquery: None,
                subquery_vec: None,
            };
        }

        Ok(result)
    }

    // Method to parse parenthesized expressions
    fn parse_parenthesized_expr(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        let mut inner = pair.into_inner();

        // Skip the left parenthesis
        inner.next();

        // Parse the inner expression
        let expr = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty parentheses".to_string()))?;

        // Create a complex field from the inner expression with parentheses flag set to true
        let mut field = Self::parse_arithmetic_expr(expr)?;

        // If the field has a nested expression, mark it as parenthesized
        if let Some(nested) = field.nested_expr {
            let (left, op, right, _) = *nested;
            field.nested_expr = Some(Box::new((left, op, right, true)));
        }

        Ok(field)
    }

    fn parse_arithmetic_factor(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        let inner = pair
            .into_inner()
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty arithmetic factor".to_string()))?;

        match inner.as_rule() {
            Rule::aggregate_expr => {
                let agg_func = Self::parse_aggregate_function(inner)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: Some(agg_func),
                    nested_expr: None,
                    subquery: None,
                    subquery_vec: None,
                })
            }
            Rule::value => Ok(ComplexField {
                column_ref: None,
                literal: Some(Self::parse_literal(inner)?),
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }),
            Rule::qualified_column => {
                let col_ref = Self::parse_column_ref(inner)?;
                Ok(ComplexField {
                    column_ref: Some(col_ref),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                    subquery_vec: None,
                })
            }
            Rule::identifier => Ok(ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: inner.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
                subquery_vec: None,
            }),
            Rule::subquery => {
                let subquery = IrParser::parse_subquery(inner)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: Some(subquery),
                    subquery_vec: None,
                })
            }
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Invalid arithmetic factor: {:?}",
                inner.as_rule()
            )))),
        }
    }
    // Helper methods for parsing basic elements
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, Box<IrParseError>> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let table = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }
            Rule::identifier => Ok(ColumnRef {
                table: None,
                column: pair.as_str().to_string(),
            }),
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Expected column reference, got {:?}",
                pair.as_rule()
            )))),
        }
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

        let column = if column_ref.as_str() == "*" {
            ColumnRef {
                table: None,
                column: "*".to_string(),
            }
        } else {
            match column_ref.as_rule() {
                Rule::qualified_column => Self::parse_column_ref(column_ref)?,
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

    fn parse_literal(pair: Pair<Rule>) -> Result<IrLiteral, Box<IrParseError>> {
        let inner = pair
            .into_inner()
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty value".to_string()))?;

        match inner.as_rule() {
            Rule::string => {
                let inner_str = inner.as_str();
                let clean_str = inner_str[1..inner_str.len() - 1].to_string();
                Ok(IrLiteral::String(clean_str))
            }
            Rule::number => Ok(inner
                .as_str()
                .parse::<i64>()
                .map(IrLiteral::Integer)
                .or_else(|_| inner.as_str().parse::<f64>().map(IrLiteral::Float))
                .map_err(|_| IrParseError::InvalidInput("Invalid number".to_string()))?),
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

    fn parse_field_reference(pair: Pair<Rule>) -> Result<ComplexField, Box<IrParseError>> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let col_ref = Self::parse_column_ref(pair)?;
                Ok(ComplexField {
                    column_ref: Some(col_ref),
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: None,
                    subquery_vec: None,
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
                subquery_vec: None,
            }),
            Rule::subquery => Ok(ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: Some(IrParser::parse_subquery(pair)?),
                subquery_vec: None,
            }),
            Rule::arithmetic_expr => Self::parse_arithmetic_expr(pair),
            _ => Err(Box::new(IrParseError::InvalidInput(format!(
                "Expected field reference, got {:?}",
                pair.as_rule()
            )))),
        }
    }
}
