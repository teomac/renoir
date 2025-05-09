use super::error::SqlParseError;
use super::limit::LimitParser;
use super::order::OrderParser;
use super::sql_ast_structure::*;
use super::validate::validate_ast;
use super::{
    from::FromParser, group_by::GroupByParser, select::SelectParser, where_clause::ConditionParser,
};
use crate::dsl::languages::sql::ast_builder::Rule;
use pest::iterators::Pairs;

pub struct SqlASTBuilder;

impl SqlASTBuilder {
    /// Builds an SQL AST from the parsed SQL query pairs.
    pub(crate) fn build_ast_from_pairs(pairs: Pairs<Rule>) -> Result<SqlAST, Box<SqlParseError>> {
        let mut pairs = pairs.clone();
        if let Some(pair) = pairs.next() {
            match pair.as_rule() {
                Rule::query => {
                    let mut inner = pair.clone().into_inner();
                    inner.next(); // Skip SELECT keyword

                    let distinct = inner
                        .next()
                        .is_some_and(|token| token.as_rule() == Rule::distinct_keyword);

                    if !distinct {
                        // If the next token is not DISTINCT, we need to go back to the beginning
                        inner = pair.into_inner();
                        inner.next(); // Skip SELECT keyword
                    }

                    let select_part = inner.next().ok_or_else(|| {
                        SqlParseError::InvalidInput("Missing SELECT clause".to_string())
                    })?;

                    // Parse the select part differently based on if it's asterisk or column_list
                    let select_columns = match select_part.as_rule() {
                        Rule::asterisk => {
                            vec![SelectColumn {
                                selection: SelectParser::parse(select_part)?,
                                alias: None,
                            }]
                        }
                        Rule::column_list => {
                            // Parse each column in the list
                            select_part
                                .into_inner()
                                .map(|col_pair| {
                                    // Get the initial selection from the first item (column_item)
                                    let mut inner_pairs = col_pair.clone().into_inner();
                                    let _column_item = inner_pairs.next().ok_or_else(|| {
                                        SqlParseError::InvalidInput(
                                            "Missing column item".to_string(),
                                        )
                                    })?;
                                    let selection = SelectParser::parse(col_pair.clone())?;

                                    // Check for alias - look for as_keyword followed by variable
                                    let alias =
                                        if inner_pairs.any(|p| p.as_rule() == Rule::as_keyword) {
                                            // If we found AS keyword, take the next token as the alias
                                            col_pair
                                                .into_inner()
                                                .last() // Get the last token which should be the alias variable
                                                .map(|alias_pair| alias_pair.as_str().to_string())
                                        } else {
                                            None
                                        };

                                    Ok(SelectColumn { selection, alias })
                                })
                                .collect::<Result<Vec<_>, Box<SqlParseError>>>()?
                        }
                        _ => {
                            return Err(Box::new(SqlParseError::InvalidInput(
                                "Invalid SELECT clause".to_string(),
                            )))
                        }
                    };

                    let select_clause = SelectClause {
                        distinct,
                        select: select_columns,
                    };

                    let from_part = inner.next().ok_or_else(|| {
                        SqlParseError::InvalidInput("Missing FROM clause".to_string())
                    })?;

                    let mut where_part = None;

                    let mut group_by_part = None;

                    let mut limit_part = None;

                    let mut order_by_part = None;

                    for next_part in inner {
                        match next_part.as_rule() {
                            Rule::where_expr => where_part = Some(next_part),
                            Rule::group_by_expr => group_by_part = Some(next_part),
                            Rule::order_by_expr => order_by_part = Some(next_part),
                            Rule::limit_expr => {
                                limit_part = Some(next_part);
                            }
                            _ => {}
                        }
                    }

                    let ast = SqlAST {
                        select: select_clause,
                        from: FromParser::parse(from_part)?,
                        filter: if let Some(where_expr) = where_part {
                            Some(ConditionParser::parse(where_expr)?)
                        } else {
                            None
                        },
                        group_by: if let Some(group_expr) = group_by_part {
                            Some(GroupByParser::parse(group_expr)?)
                        } else {
                            None
                        },
                        order_by: if let Some(order_expr) = order_by_part {
                            Some(OrderParser::parse(order_expr)?)
                        } else {
                            None
                        },
                        limit: if let Some(limit) = limit_part {
                            Some(LimitParser::parse(limit)?)
                        } else {
                            None
                        },
                    };

                    validate_ast(&ast)?;

                    return Ok(ast);
                }
                _ => {
                    return Err(Box::new(SqlParseError::InvalidInput(
                        "Expected query".to_string(),
                    )))
                }
            }
        }
        Err(Box::new(SqlParseError::InvalidInput(
            "No valid query found".to_string(),
        )))
    }
}
