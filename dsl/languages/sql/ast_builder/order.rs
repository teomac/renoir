use super::error::SqlParseError;
use super::sql_ast_structure::*;
use crate::dsl::languages::sql::ast_builder::Rule;
use pest::iterators::Pair;

pub struct OrderParser;

impl OrderParser {
    pub(crate) fn parse(pair: Pair<Rule>) -> Result<OrderByClause, Box<SqlParseError>> {
        let mut inner = pair.into_inner();

        inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing ORDER BY keyword".to_string()))?;

        // Get the order by list
        let order_list = inner
            .next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing order columns".to_string()))?;

        let mut items = Vec::new();

        // Process each item in the order by list
        for item in order_list.into_inner() {
            match item.as_rule() {
                Rule::order_item => {
                    let mut item_inner = item.into_inner();

                    // First element is always the column reference
                    let column_ref = item_inner.next().ok_or_else(|| {
                        SqlParseError::InvalidInput("Missing column in ORDER BY".to_string())
                    })?;

                    let column = match column_ref.as_rule() {
                        Rule::table_column | Rule::variable => Self::parse_column_ref(column_ref)?,
                        _ => {
                            return Err(Box::new(SqlParseError::InvalidInput(format!(
                                "Expected column reference, got {:?}",
                                column_ref.as_rule()
                            ))))
                        }
                    };

                      // Default values
                    let mut direction = OrderDirection::Asc; // Default to ascending if not specified
                    let mut nulls_first: Option<bool> = None; // Default to None if not specified

                     // Check for optional direction (ASC/DESC) and nulls handling
                    for option in item_inner.by_ref() {
                        match option.as_rule() {
                            Rule::order_direction => {
                                direction = match option.as_str().to_uppercase().as_str() {
                                    "ASC" => OrderDirection::Asc,
                                    "DESC" => OrderDirection::Desc,
                                    _ => {
                                        return Err(Box::new(SqlParseError::InvalidInput(
                                            "Invalid sort direction".to_string(),
                                        )))
                                    }
                                };
                            }
                            Rule::nulls_handling => {
                                nulls_first = match option.as_str().to_uppercase().as_str() {
                                    "NULLS FIRST" => Some(true),
                                    "NULLS LAST" => Some(false),
                                    _ => {
                                        return Err(Box::new(SqlParseError::InvalidInput(
                                            "Invalid nulls handling".to_string(),
                                        )))
                                    }
                                };
                            }
                            _ => {
                                return Err(Box::new(SqlParseError::InvalidInput(format!(
                                    "Expected order direction or nulls handling, got {:?}",
                                    option.as_rule()
                                ))))
                            }
                        }
                    }

                    items.push(OrderByItem {
                        column,
                        direction,
                        nulls_first,
                    });
                }
                _ => {
                    return Err(Box::new(SqlParseError::InvalidInput(format!(
                        "Expected order item, got {:?}",
                        item.as_rule()
                    ))))
                }
            }
        }

        if items.is_empty() {
            return Err(Box::new(SqlParseError::InvalidInput(
                "Empty ORDER BY clause".to_string(),
            )));
        }

        Ok(OrderByClause { items })
    }

    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, Box<SqlParseError>> {
        match pair.as_rule() {
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
}
