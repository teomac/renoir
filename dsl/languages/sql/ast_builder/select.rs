use super::error::SqlParseError;
use super::literal::LiteralParser;
use super::{sql_ast_structure::*, SqlParser};
use crate::dsl::languages::sql::ast_builder::Rule;
use pest::iterators::Pair;

pub struct SelectParser;

impl SelectParser {
    pub(crate) fn parse(pair: Pair<Rule>) -> Result<SelectType, Box<SqlParseError>> {
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
                Self::parse_column_item(column_item)
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected column_with_alias, got {:?}",
                pair.as_rule()
            )))),
        }
    }

    fn parse_column_item(pair: Pair<Rule>) -> Result<SelectType, Box<SqlParseError>> {
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
            }
            Rule::select_expr => Self::parse_complex_expression(item),
            Rule::subquery_expr => {
                // Handle subquery in SELECT
                let subquery = SqlParser::parse_subquery(item)?;

                // Validate that the subquery only returns one column
                if subquery.select.select.len() != 1 {
                    return Err(Box::new(SqlParseError::InvalidInput(
                        "Subquery in SELECT must return exactly one column".to_string(),
                    )));
                }

                Ok(SelectType::Subquery(Box::new(subquery)))
            }
            Rule::string_literal => {
                // remove quotes from string
                let inner_str = item.as_str();
                let clean_str = inner_str[1..inner_str.len() - 1].to_string();
                Ok(SelectType::StringLiteral(clean_str))
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Invalid column item: {:?}",
                item.as_rule()
            )))),
        }
    }

    //function to parse column references
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, Box<SqlParseError>> {
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

            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected column reference, got {:?}",
                pair.as_rule()
            )))),
        }
    }

    fn parse_aggregate(
        pair: Pair<Rule>,
    ) -> Result<(AggregateFunction, ColumnRef), Box<SqlParseError>> {
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
                return Err(Box::new(SqlParseError::InvalidInput(
                    "Unknown aggregate function".to_string(),
                )))
            }
        };

        let var_pair = agg
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate column".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;

        //if aggregation is different than COUNT and column is *, return error
        if func != AggregateFunction::Count && col_ref.column == "*" {
            return Err(Box::new(SqlParseError::InvalidInput(
                "Invalid aggregation".to_string(),
            )));
        }

        Ok((func, col_ref))
    }

    fn parse_complex_expression(pair: Pair<Rule>) -> Result<SelectType, Box<SqlParseError>> {
        match pair.as_rule() {
            Rule::select_expr => {
                let mut pairs = pair.into_inner().peekable();

                // Get first operand
                let first = pairs.next().ok_or_else(|| {
                    SqlParseError::InvalidInput("Missing first operand".to_string())
                })?;

                let mut left_expr = match first.as_rule() {
                    Rule::parenthesized_expr => Self::parse_parenthesized_expr(first)?,
                    Rule::column_operand => Self::parse_operand(first)?,
                    _ => {
                        return Err(Box::new(SqlParseError::InvalidInput(format!(
                            "Invalid first operand: {:?}",
                            first.as_rule()
                        ))))
                    }
                };

                // Process any subsequent operations
                while let Some(op) = pairs.next() {
                    let symbol = op.as_str().to_string();

                    let right = pairs.next().ok_or_else(|| {
                        SqlParseError::InvalidInput("Missing right operand".to_string())
                    })?;

                    let right_expr = match right.as_rule() {
                        Rule::parenthesized_expr => Self::parse_parenthesized_expr(right)?,
                        Rule::column_operand => Self::parse_operand(right)?,
                        _ => {
                            return Err(Box::new(SqlParseError::InvalidInput(format!(
                                "Invalid right operand: {:?}",
                                right.as_rule()
                            ))))
                        }
                    };

                    left_expr = ArithmeticExpr::NestedExpr(
                        Box::new(left_expr),
                        symbol,
                        Box::new(right_expr),
                        false, // Not parenthesized by default
                    );
                }

                Ok(SelectType::ArithmeticExpr(left_expr))
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected select expression, got {:?}",
                pair.as_rule()
            )))),
        }
    }

    fn parse_parenthesized_expr(pair: Pair<Rule>) -> Result<ArithmeticExpr, Box<SqlParseError>> {
        let mut inner = pair.into_inner();

        // Skip left parenthesis
        inner.next();

        // Get the inner expression
        let expr = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty parentheses".to_string()))?;

        match Self::parse_complex_expression(expr)? {
            SelectType::ArithmeticExpr(expr) => {
                // If the expression is already nested, mark it as parenthesized
                match expr {
                    ArithmeticExpr::NestedExpr(left, op, right, _) => {
                        Ok(ArithmeticExpr::NestedExpr(left, op, right, true))
                    }
                    _ => Ok(expr), // Return as is if not nested
                }
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(
                "Invalid parenthesized expression".to_string(),
            ))),
        }
    }

    fn parse_operand(pair: Pair<Rule>) -> Result<ArithmeticExpr, Box<SqlParseError>> {
        let inner = pair
            .into_inner()
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty operand".to_string()))?;

        match inner.as_rule() {
            Rule::number => Ok(ArithmeticExpr::Literal(LiteralParser::parse(
                inner.as_str(),
            )?)),
            Rule::table_column => Ok(ArithmeticExpr::Column(Self::parse_column_ref(inner)?)),
            Rule::variable => Ok(ArithmeticExpr::Column(ColumnRef {
                table: None,
                column: inner.as_str().to_string(),
            })),
            Rule::aggregate_expr => {
                let (func, col) = Self::parse_aggregate(inner)?;
                Ok(ArithmeticExpr::Aggregate(func, col))
            }
            Rule::subquery_expr => {
                let subquery = SqlParser::parse_subquery(inner)?;
                Ok(ArithmeticExpr::Subquery(Box::new(subquery)))
            }
            _ => Err(Box::new(SqlParseError::InvalidInput(format!(
                "Invalid operand: {:?}",
                inner.as_rule()
            )))),
        }
    }
}
