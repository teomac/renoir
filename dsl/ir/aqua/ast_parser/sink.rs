use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::AquaParseError;
use super::literal::LiteralParser;
use crate::dsl::ir::aqua::ast_parser::Rule;

pub struct SinkParser;

impl SinkParser {
    pub fn parse(pair: Pair<Rule>) -> Result<Vec<SelectClause>, AquaParseError> {
        let mut inner = pair.into_inner();
        
        // Skip the 'select' keyword if present
        if inner.peek().map_or(false, |p| p.as_str() == "select") {
            inner.next();
        }

        let sink_expr = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing sink expression".to_string()))?;

        match sink_expr.as_rule() {
            Rule::asterisk => {
                Ok(vec![SelectClause::Column(ColumnRef {
                    table: None,
                    column: "*".to_string(),
                }, None)])
            },
            Rule::column_list => {
                sink_expr.into_inner()
                    .map(|column_item| {
                        let mut inner_pairs = column_item.into_inner();
                        
                        // Get the main expression
                        let expr = inner_pairs.next()
                            .ok_or_else(|| AquaParseError::InvalidInput("Missing column expression".to_string()))?;

                        // Look for alias - will be after AS keyword
                        let mut alias = None;
                        while let Some(next) = inner_pairs.next() {
                            match next.as_rule() {
                                Rule::as_keyword => {
                                    if let Some(alias_ident) = inner_pairs.next() {
                                        alias = Some(alias_ident.as_str().to_string());
                                    }
                                },
                                _ => {}
                            }
                        }

                        // Process the main expression based on its type
                        match expr.as_rule() {
                            Rule::identifier | Rule::qualified_column => {
                                Ok(SelectClause::Column(Self::parse_column_ref(expr)?, alias))
                            },
                            Rule::aggregate_expr => {
                                let agg_func = Self::parse_aggregate_function(expr)?;
                                Ok(SelectClause::Aggregate(agg_func, alias))
                            },
                            Rule::complex_op => {
                                let (col_ref, op, lit) = Self::parse_complex_expression(expr)?;
                                Ok(SelectClause::ComplexValue(col_ref, op, lit, alias))
                            },
                            _ => Err(AquaParseError::InvalidInput(
                                format!("Invalid column expression: {:?}", expr.as_rule())
                            )),
                        }
                    })
                    .collect()
            },
            _ => Err(AquaParseError::InvalidInput(
                format!("Invalid sink expression: {:?}", sink_expr.as_rule())
            )),
        }
    }

    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, AquaParseError> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let table = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing stream name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing field name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ColumnRef {
                    table: Some(table),
                    column,
                })
            }
            Rule::identifier | Rule::asterisk => {
                Ok(ColumnRef {
                    table: None,
                    column: pair.as_str().to_string(),
                })
            }
            _ => Err(AquaParseError::InvalidInput(
                format!("Expected field reference, got {:?}", pair.as_rule())
            )),
        }
    }

    // Modified to return AggregateFunction directly instead of SelectClause
    fn parse_aggregate_function(pair: Pair<Rule>) -> Result<AggregateFunction, AquaParseError> {
        let mut agg = pair.into_inner();
        let func = match agg.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing aggregate function".to_string()))?
            .as_str()
            .to_lowercase()
            .as_str() 
        {
            "max" => AggregateType::Max,
            "min" => AggregateType::Min,
            "avg" => AggregateType::Avg,
            "sum" => AggregateType::Sum,
            "count" => AggregateType::Count,
            unknown => return Err(AquaParseError::InvalidInput(
                format!("Unknown aggregate function: {}", unknown)
            )),
        };
        
        let var_pair = agg.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing aggregate field".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;
            
        Ok(AggregateFunction {
            function: func,
            column: col_ref,
        })
    }


     // Modified to return tuple of components instead of SelectClause
    fn parse_complex_expression(pair: Pair<Rule>) -> Result<(ColumnRef, String, AquaLiteral), AquaParseError> {
        let mut complex = pair.into_inner();
        
        let var_pair = complex.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing first operand".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;
            
        let op = complex.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing operator".to_string()))?
            .as_str()
            .to_string();
            
        let val_str = complex.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing second operand".to_string()))?
            .as_str();
            
        let literal = LiteralParser::parse(val_str)?;
        Ok((col_ref, op, literal))
    }
}