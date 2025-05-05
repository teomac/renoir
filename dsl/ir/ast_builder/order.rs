use super::error::IrParseError;
use super::ir_ast_structure::*;
use crate::dsl::ir::ast_builder::Rule;
use pest::iterators::Pair;

pub struct OrderParser;

impl OrderParser {
    pub(crate) fn parse(pair: Pair<Rule>) -> Result<Vec<OrderByItem>, Box<IrParseError>> {
        let mut inner = pair.into_inner();

        inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing order keyword".to_string()))?;

        // Get the order list
        let order_list = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing order columns".to_string()))?;

        let mut items = Vec::new();

        // Process each item in the order list
        for item in order_list.into_inner() {
            match item.as_rule() {
                Rule::order_item => {
                    let mut item_inner = item.into_inner();

                    // First element is the column reference
                    let column_ref = item_inner.next().ok_or_else(|| {
                        IrParseError::InvalidInput("Missing column in order".to_string())
                    })?;

                    let column = match column_ref.as_rule() {
                        Rule::qualified_column => Self::parse_qualified_column(column_ref)?,
                        Rule::identifier => ColumnRef {
                            table: None,
                            column: column_ref.as_str().to_string(),
                        },
                        _ => {
                            return Err(Box::new(IrParseError::InvalidInput(format!(
                                "Expected column reference, got {:?}",
                                column_ref.as_rule()
                            ))))
                        }
                    };

                    // Check for optional direction
                    let direction = if let Some(dir) = item_inner.next() {
                        match dir.as_rule() {
                            Rule::order_direction => match dir.as_str() {
                                "asc" => OrderDirection::Asc,
                                "desc" => OrderDirection::Desc,
                                _ => {
                                    return Err(Box::new(IrParseError::InvalidInput(
                                        "Invalid sort direction".to_string(),
                                    )))
                                }
                            },
                            _ => {
                                return Err(Box::new(IrParseError::InvalidInput(format!(
                                    "Expected order direction, got {:?}",
                                    dir.as_rule()
                                ))))
                            }
                        }
                    } else {
                        OrderDirection::Asc // Default to ascending
                    };

                    items.push(OrderByItem { column, direction });
                }
                _ => {
                    return Err(Box::new(IrParseError::InvalidInput(format!(
                        "Expected order item, got {:?}",
                        item.as_rule()
                    ))))
                }
            }
        }

        if items.is_empty() {
            return Err(Box::new(IrParseError::InvalidInput(
                "Empty order clause".to_string(),
            )));
        }

        Ok(items)
    }

    fn parse_qualified_column(pair: Pair<Rule>) -> Result<ColumnRef, Box<IrParseError>> {
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
}
