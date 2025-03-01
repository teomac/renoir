use super::error::SqlParseError;
use super::sql_ast_structure::*;
use crate::dsl::languages::sql::ast_parser::Rule;
use pest::iterators::Pair;

pub struct OrderParser;

impl OrderParser {
    pub fn parse(pair: Pair<Rule>) -> Result<OrderByClause, SqlParseError> {
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
                            return Err(SqlParseError::InvalidInput(format!(
                                "Expected column reference, got {:?}",
                                column_ref.as_rule()
                            )))
                        }
                    };

                    // Check for optional direction (ASC/DESC)
                    let direction = if let Some(dir) = item_inner.next() {
                        match dir.as_rule() {
                            Rule::order_direction => match dir.as_str().to_uppercase().as_str() {
                                "ASC" => OrderDirection::Asc,
                                "DESC" => OrderDirection::Desc,
                                _ => {
                                    return Err(SqlParseError::InvalidInput(
                                        "Invalid sort direction".to_string(),
                                    ))
                                }
                            },
                            _ => {
                                return Err(SqlParseError::InvalidInput(format!(
                                    "Expected order direction, got {:?}",
                                    dir.as_rule()
                                )))
                            }
                        }
                    } else {
                        OrderDirection::Asc // Default to ascending if not specified
                    };

                    items.push(OrderByItem { column, direction });
                }
                _ => {
                    return Err(SqlParseError::InvalidInput(format!(
                        "Expected order item, got {:?}",
                        item.as_rule()
                    )))
                }
            }
        }

        if items.is_empty() {
            return Err(SqlParseError::InvalidInput(
                "Empty ORDER BY clause".to_string(),
            ));
        }

        Ok(OrderByClause { items })
    }

    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, SqlParseError> {
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
            _ => Err(SqlParseError::InvalidInput(format!(
                "Expected column reference, got {:?}",
                pair.as_rule()
            ))),
        }
    }
}
