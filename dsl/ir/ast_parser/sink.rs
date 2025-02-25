use super::error::IrParseError;
use super::ir_ast_structure::*;
use super::literal::LiteralParser;
use crate::dsl::ir::ast_parser::Rule;
use pest::iterators::Pair;

pub struct SinkParser;

impl SinkParser {
    pub fn parse(pair: Pair<Rule>) -> Result<Vec<SelectClause>, IrParseError> {
        let mut inner = pair.into_inner();

        // Skip the 'select' keyword if present
        if inner.peek().map_or(false, |p| p.as_str() == "select") {
            inner.next();
        }

        let sink_expr = inner
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing sink expression".to_string()))?;

        match sink_expr.as_rule() {
            Rule::asterisk => Ok(vec![SelectClause::Column(
                ColumnRef {
                    table: None,
                    column: "*".to_string(),
                },
                None,
            )]),
            Rule::column_list => {
                sink_expr
                    .into_inner()
                    .map(|column_item| {
                        let mut inner_pairs = column_item.into_inner();

                        // Get the main expression
                        let expr = inner_pairs.next().ok_or_else(|| {
                            IrParseError::InvalidInput("Missing column expression".to_string())
                        })?;

                        // Look for alias - will be after AS keyword
                        let mut alias = None;
                        while let Some(next) = inner_pairs.next() {
                            match next.as_rule() {
                                Rule::as_keyword => {
                                    if let Some(alias_ident) = inner_pairs.next() {
                                        alias = Some(alias_ident.as_str().to_string());
                                    }
                                }
                                _ => {}
                            }
                        }

                        // Process the main expression based on its type
                        match expr.as_rule() {
                            Rule::complex_op => Self::parse_complex_operation(expr, alias),
                            Rule::aggregate_expr => Ok(SelectClause::Aggregate(
                                Self::parse_aggregate_function(expr)?,
                                alias,
                            )),
                            Rule::qualified_column => {
                                Ok(SelectClause::Column(Self::parse_column_ref(expr)?, alias))
                            }
                            Rule::identifier => Ok(SelectClause::Column(
                                ColumnRef {
                                    table: None,
                                    column: expr.as_str().to_string(),
                                },
                                alias,
                            )),
                            _ => Err(IrParseError::InvalidInput(format!(
                                "Invalid column expression: {:?}",
                                expr.as_rule()
                            ))),
                        }
                    })
                    .collect()
            }
            _ => Err(IrParseError::InvalidInput(format!(
                "Invalid sink expression: {:?}",
                sink_expr.as_rule()
            ))),
        }
    }

    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, IrParseError> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let table = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing stream name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner
                    .next()
                    .ok_or_else(|| IrParseError::InvalidInput("Missing field name".to_string()))?
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
            _ => Err(IrParseError::InvalidInput(format!(
                "Expected field reference, got {:?}",
                pair.as_rule()
            ))),
        }
    }

    // Modified to return AggregateFunction directly instead of SelectClause
    fn parse_aggregate_function(pair: Pair<Rule>) -> Result<AggregateFunction, IrParseError> {
        let mut agg = pair.into_inner();

        let func = match agg
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing aggregate function".to_string()))?
            .as_str()
            .to_lowercase()
            .as_str()
        {
            "max" => AggregateType::Max,
            "min" => AggregateType::Min,
            "avg" => AggregateType::Avg,
            "sum" => AggregateType::Sum,
            "count" => AggregateType::Count,
            unknown => {
                return Err(IrParseError::InvalidInput(format!(
                    "Unknown aggregate function: {}",
                    unknown
                )))
            }
        };

        let var_pair = agg
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Missing aggregate field".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;

        Ok(AggregateFunction {
            function: func,
            column: col_ref,
        })
    }

    // Modified to return tuple of components instead of SelectClause
    fn parse_complex_operation(
        pair: Pair<Rule>,
        alias: Option<String>,
    ) -> Result<SelectClause, IrParseError> {
        let mut pairs = pair.into_inner().peekable();
        
        // Parse first operand
        let mut left_field = match pairs.next() {
            Some(first) => match first.as_rule() {
                Rule::parenthesized_expr => Self::parse_parenthesized_expr(first)?,
                Rule::column_operand => Self::parse_operand(first)?,
                _ => return Err(IrParseError::InvalidInput(
                    format!("Invalid first operand: {:?}", first.as_rule())
                )),
            },
            None => return Err(IrParseError::InvalidInput("Missing operand".to_string())),
        };
    
        // Process operators and operands in pairs
        while pairs.peek().is_some() {
            let op = pairs.next()
                .map(|p| p.as_str().to_string())
                .ok_or_else(|| IrParseError::InvalidInput("Expected operator".to_string()))?;
            
            let right_field = match pairs.next() {
                Some(right_pair) => match right_pair.as_rule() {
                    Rule::parenthesized_expr => Self::parse_parenthesized_expr(right_pair)?,
                    Rule::column_operand => Self::parse_operand(right_pair)?,
                    _ => return Err(IrParseError::InvalidInput(
                        format!("Invalid right operand: {:?}", right_pair.as_rule())
                    )),
                },
                None => return Err(IrParseError::InvalidInput("Missing right operand".to_string())),
            };
    
            left_field = ComplexField {
                column_ref: None,
                literal: None,
                aggregate: None,
                nested_expr: Some(Box::new((left_field, op, right_field))),
            };
        }
    
        Ok(SelectClause::ComplexValue(left_field, alias))
    }
    
    fn parse_parenthesized_expr(pair: Pair<Rule>) -> Result<ComplexField, IrParseError> {
        let mut inner = pair.into_inner();
        
        // Skip left parenthesis
        inner.next();
    
        let expr = inner.next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty parentheses".to_string()))?;
    
        match expr.as_rule() {
            Rule::select_expr => {
                let inner_expr = expr.into_inner().next()
                    .ok_or_else(|| IrParseError::InvalidInput("Empty expression".to_string()))?;
    
                match inner_expr.as_rule() {
                    Rule::complex_op => {
                        if let SelectClause::ComplexValue(left_field, _) = 
                            Self::parse_complex_operation(inner_expr, None)? {
                            Ok(left_field)
                        } else {
                            Err(IrParseError::InvalidInput("Invalid complex operation".to_string()))
                        }
                    },
                    _ => Err(IrParseError::InvalidInput("Invalid parenthesized expression".to_string())),
                }
            },
            _ => Err(IrParseError::InvalidInput("Invalid parenthesized expression content".to_string())),
        }
    }

    fn parse_operand(pair: Pair<Rule>) -> Result<ComplexField, IrParseError> {
        let operand = pair
            .into_inner()
            .next()
            .ok_or_else(|| IrParseError::InvalidInput("Empty operand".to_string()))?;

        match operand.as_rule() {
            Rule::number => Ok(ComplexField {
                column_ref: None,
                literal: Some(LiteralParser::parse(operand.as_str())?),
                aggregate: None,
                nested_expr: None,
            }),
            Rule::qualified_column => Ok(ComplexField {
                column_ref: Some(Self::parse_column_ref(operand)?),
                literal: None,
                aggregate: None,
                nested_expr: None,
            }),
            Rule::identifier => Ok(ComplexField {
                column_ref: Some(ColumnRef {
                    table: None,
                    column: operand.as_str().to_string(),
                }),
                literal: None,
                aggregate: None,
                nested_expr: None,
            }),
            Rule::aggregate_expr => Ok(ComplexField {
                column_ref: None,
                literal: None,
                aggregate: Some(Self::parse_aggregate_function(operand)?),
                nested_expr: None,
            }),
            _ => Err(IrParseError::InvalidInput(format!(
                "Invalid operand: {:?}",
                operand.as_rule()
            ))),
        }
    }
}
