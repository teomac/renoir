use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::SqlParseError;
use super::literal::LiteralParser;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct SelectParser;

impl SelectParser {
    pub fn parse(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        
        
        // First, handle the column_with_alias rule
        match pair.as_rule() {
            Rule::asterisk => {
                Ok(SelectType::Simple(ColumnRef {
                    table: None,
                    column: "*".to_string(),
                }))
            },
            
            Rule::column_with_alias => {
                // Get the inner column_item
                let mut inner = pair.into_inner();
                let column_item = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column item".to_string()))?;
                
                // Parse the actual column content
                return Self::parse_column_item(column_item);
            }
            _ => return Err(SqlParseError::InvalidInput(format!("Expected column_with_alias, got {:?}", pair.as_rule()))),
        }
    }

    // New function to parse column_item
    fn parse_column_item(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        let mut inner = pair.into_inner();
        let item = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Empty column item".to_string()))?;

        match item.as_rule() {
            Rule::variable => {
                Ok(SelectType::Simple(ColumnRef {
                    table: None,
                    column: item.as_str().to_string(),
                }))
            },
            Rule::table_column => {
                Self::parse_column_ref(item).map(SelectType::Simple)
            },
            Rule::aggregate_expr => {
                let agg = Self::parse_aggregate(item)?;
                Ok(SelectType::Aggregate(agg.0, agg.1))
            },
            Rule::select_expr => {
                Self::parse_complex_expression(item)
            },
            _ => Err(SqlParseError::InvalidInput(format!("Invalid column item: {:?}", item.as_rule()))),
        }
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
                let table = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| SqlParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }
            
            Rule::variable => {
                Ok(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                })
            }
            
            _ => Err(SqlParseError::InvalidInput(format!("Expected column reference, got {:?}", pair.as_rule()))),
        }
    }

    fn parse_aggregate(pair: Pair<Rule>) -> Result<(AggregateFunction, ColumnRef), SqlParseError> {
        let mut agg = pair.into_inner();
        let func = match agg.next()
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
            _ => return Err(SqlParseError::InvalidInput("Unknown aggregate function".to_string())),
        };
        
        let var_pair = agg.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing aggregate column".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;

        //if aggregation is different than COUNT and column is *, return error
        if func != AggregateFunction::Count && col_ref.column == "*" {
            return Err(SqlParseError::InvalidInput("Invalid aggregation".to_string()));
        }
            
        Ok((func, col_ref))
    }

    fn parse_complex_expression(pair: Pair<Rule>) -> Result<SelectType, SqlParseError> {
        let mut complex = pair.into_inner();
        let var_pair = complex.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing first operand".to_string()))?;

        //parse left side of the expression
        let left = match var_pair.as_rule() {
            Rule::number => ComplexField {
                column_ref: None,
                literal: Some(LiteralParser::parse(var_pair.as_str())?),
                aggregate: None,
            },
            Rule::variable => ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: var_pair.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
            },
            Rule::table_column => ComplexField {
                column_ref: Some(Self::parse_column_ref(var_pair)?),
                literal: None,
                aggregate: None,
            },
            Rule::aggregate_expr => 
            ComplexField {
                column_ref: None,
                literal: None,
                aggregate: Some(Self::parse_aggregate(var_pair)?),
            },
            _ => ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
            },
        };
            
        let op = complex.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator".to_string()))?
            .as_str()
            .to_string();


        //parse right side of the expression
        let var_pair2 = complex.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing second operand".to_string()))?;
        let right = match var_pair2.as_rule() {
            Rule::number => ComplexField {
                column_ref: None,
                literal: Some(LiteralParser::parse(var_pair2.as_str())?),
                aggregate: None,
            },
            Rule::variable => ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: var_pair2.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
            },
            Rule::table_column => ComplexField {
                column_ref: Some(Self::parse_column_ref(var_pair2)?),
                literal: None,
                aggregate: None,
            },
            Rule::aggregate_expr =>
                ComplexField {
                    column_ref: None,
                    literal: None,
                    aggregate: Some(Self::parse_aggregate(var_pair2)?),
                },
            _ => ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
            },
        };

        Ok(SelectType::ComplexValue(left, op, right))
    }
}