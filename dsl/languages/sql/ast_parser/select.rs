use super::error::SqlParseError;
use super::literal::LiteralParser;
use super::sql_ast_structure::*;
use super::builder::SqlASTBuilder;
use crate::dsl::languages::sql::ast_parser::Rule;
use pest::iterators::Pair;

pub struct SelectParser;

impl SelectParser {
    pub fn parse(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        // First, handle the column_with_alias rule
        match pair.as_rule() {
            Rule::asterisk => Ok(SelectType::Simple(ColumnRef {
                table: None,
                column: "*".to_string(),
            })),

            Rule::column_with_alias => {
                // Get the inner column_item
                let mut inner = pair.into_inner();
                let column_item = inner.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing column item".to_string())
                })?;

                // Parse the actual column content
                return Self::parse_column_item(column_item);
            }
            _ => {
                return Err(SqlParseError::InvalidInput(format!(
                    "Expected column_with_alias, got {:?}",
                    pair.as_rule()
                )))
            }
        }
    }

    // New function to parse column_item
    fn parse_column_item(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        let mut inner = pair.into_inner();
        let item = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty column item".to_string()))?;

        match item.as_rule() {
            Rule::variable => Ok(SelectType::Simple(ColumnRef {
                table: None,
                column: item.as_str().to_string(),
            })),
            Rule::table_column => Self::parse_column_ref(item).map(SelectType::Simple),
            Rule::aggregate_expr => {
                let agg = Self::parse_aggregate(item)?;
                Ok(SelectType::Aggregate(agg.0, agg.1))
            },
            Rule::select_expr => Self::parse_complex_expression(item),
            Rule::subquery_expr => {
                // New: Handle subquery in SELECT
                let subquery = Self::parse_subquery(item)?;
                Ok(SelectType::Subquery(Box::new(subquery)))
            },
            _ => Err(SqlParseError::InvalidInput(format!(
                "Invalid column item: {:?}",
                item.as_rule()
            ))),
        }
    }

    // New: Method to parse subqueries
    fn parse_subquery(pair: Pair<Rule>) -> Result<SqlAST, SqlParseError> {
        // Extract the query part from the subquery
        let query = pair.into_inner()
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty subquery".to_string()))?;
        
        // Use the builder to parse the query
        SqlASTBuilder::build_ast_from_pairs(query.into_inner())
    }

    //function to parse column references
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, SqlParseError> {
        match pair.as_rule() {
            Rule::asterisk => Ok(ColumnRef {
                table: None,
                column: "*".to_string(),
            }),
            Rule::table_column => {
                let mut inner = pair.into_inner();
                let table = inner
                    .next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner
                    .next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }

            Rule::variable => Ok(ColumnRef {
                table: None,
                column: pair.as_str().to_string(),
            }),

            _ => Err(SqlParseError::InvalidInput(format!(
                "Expected column reference, got {:?}",
                pair.as_rule()
            ))),
        }
    }

    fn parse_aggregate(pair: Pair<Rule>) -> Result<(AggregateFunction, ColumnRef), SqlParseError> {
        let mut agg = pair.into_inner();
        let func = match agg
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate function".to_string()))?
            .as_str()
            .to_uppercase()
            .as_str()
        {
            "MAX" => AggregateFunction::Max,
            "MIN" => AggregateFunction::Min,
            "SUM" => AggregateFunction::Sum,
            "COUNT" => AggregateFunction::Count,
            "AVG" => AggregateFunction::Avg,
            _ => {
                return Err(SqlParseError::InvalidInput(
                    "Unknown aggregate function".to_string(),
                ))
            }
        };

        let var_pair = agg
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate column".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;

        //if aggregation is different than COUNT and column is *, return error
        if func != AggregateFunction::Count && col_ref.column == "*" {
            return Err(SqlParseError::InvalidInput(
                "Invalid aggregation".to_string(),
            ));
        }

        Ok((func, col_ref))
    }

    fn parse_complex_expression(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        let mut pairs = pair.into_inner().peekable();

        // Get first operand
        let first = pairs
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing first operand".to_string()))?;

        let mut left_field = match first.as_rule() {
            Rule::parenthesized_expr => Self::parse_parenthesized_expr(first)?,
            Rule::column_operand => Self::parse_operand(first)?,
            _ => {
                return Err(SqlParseError::InvalidInput(format!(
                    "Invalid first operand: {:?}",
                    first.as_rule()
                )))
            }
        };

        // If no operator, return just the left field
        while let Some(op) = pairs.next() {
            let symbol = op.as_str().to_string();

            let right = pairs
                .next()
                .ok_or_else(|| SqlParseError::InvalidInput("Missing right operand".to_string()))?;

            let right_field = match right.as_rule() {
                Rule::parenthesized_expr => Self::parse_parenthesized_expr(right)?,
                Rule::column_operand => Self::parse_operand(right)?,
                _ => {
                    return Err(SqlParseError::InvalidInput(format!(
                        "Invalid right operand: {:?}",
                        right.as_rule()
                    )))
                }
            };

            // Create new ComplexField with nested expression
            left_field = ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: Some(Box::new((left_field, symbol, right_field))),
                subquery: None,
            };
        }

        Ok(SelectType::ComplexValue(
            left_field,
            String::new(), // Empty string since we handled operators in nested_expr
            ComplexField {
                // Empty right field since we handled everything in left_field
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
            },
        ))
    }

    // New helper function to parse operands
    fn parse_parenthesized_expr(pair: Pair<Rule>) -> Result<ComplexField, SqlParseError> {
        let mut inner = pair.into_inner();

        // Skip left parenthesis
        inner.next();

        // Get the inner expression
        let expr = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty parentheses".to_string()))?;

        match Self::parse_complex_expression(expr)? {
            SelectType::ComplexValue(left, op, right) => {
                if op.is_empty() {
                    Ok(left) // If no operator, just return the left field
                } else {
                    Ok(ComplexField {
                        column_ref: None,
                        literal: None,
                        aggregate: None,
                        nested_expr: Some(Box::new((left, op, right))),
                        subquery: None,
                    })
                }
            }
            _ => Err(SqlParseError::InvalidInput(
                "Invalid parenthesized expression".to_string(),
            )),
        }
    }

    fn parse_operand(pair: Pair<Rule>) -> Result<ComplexField, SqlParseError> {
        let inner = pair
            .into_inner()
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty operand".to_string()))?;

        match inner.as_rule() {
            Rule::number => Ok(ComplexField {
                column_ref: None,
                literal: Some(LiteralParser::parse(inner.as_str())?),
                aggregate: None,
                nested_expr: None,
                subquery: None,
            }),
            Rule::table_column => Ok(ComplexField {
                column_ref: Some(Self::parse_column_ref(inner)?),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
            }),
            Rule::variable => Ok(ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: inner.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
                subquery: None,
            }),
            Rule::aggregate_expr => {
                let (func, col) = Self::parse_aggregate(inner)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: Some((func, col)),
                    nested_expr: None,
                    subquery: None,
                })
            },
            Rule::subquery_expr => {
                // New: Handle subquery in column operand
                let subquery = Self::parse_subquery(inner)?;
                Ok(ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: None,
                    nested_expr: None,
                    subquery: Some(Box::new(subquery)),
                })
            },
            _ => Err(SqlParseError::InvalidInput(format!(
                "Invalid operand: {:?}",
                inner.as_rule()
            ))),
        }
    }
}