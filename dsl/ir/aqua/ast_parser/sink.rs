use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::AquaParseError;
use super::literal::LiteralParser;
use crate::dsl::ir::aqua::ast_parser::Rule;

pub struct SinkParser;

impl SinkParser {
    pub fn parse(pair: Pair<Rule>) -> Result<SelectClause, AquaParseError> {
        let mut inner = pair.into_inner();
        
        // Skip the 'select' keyword if present
        if inner.peek().map_or(false, |p| p.as_str() == "select") {
            inner.next();
        }

        let sink_expr = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing sink expression".to_string()))?;

        let selection = match sink_expr.as_rule() {
            Rule::identifier | Rule::qualified_column => {
                let col_ref = Self::parse_column_ref(sink_expr)?;
                SelectClause::Column(col_ref)
            },
            Rule::aggregate_expr => {
                Self::parse_aggregate(sink_expr)?
            },
            Rule::complex_op => {
                Self::parse_complex_expression(sink_expr)?
            },
            _ => return Err(AquaParseError::InvalidInput(
                format!("Invalid sink expression: {:?}", sink_expr.as_rule())
            )),
        };

        Ok(selection)
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
            Rule::identifier => {
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

    fn parse_aggregate(pair: Pair<Rule>) -> Result<SelectClause, AquaParseError> {
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
            unknown => return Err(AquaParseError::InvalidInput(
                format!("Unknown aggregate function: {}", unknown)
            )),
        };
        
        let var_pair = agg.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing aggregate field".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;
            
        Ok(SelectClause::Aggregate(AggregateFunction {
            function: func,
            column: col_ref,
        }))
    }

    fn parse_complex_expression(pair: Pair<Rule>) -> Result<SelectClause, AquaParseError> {
        let mut complex = pair.into_inner();
        
        // Parse the field reference (left operand)
        let var_pair = complex.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing first operand".to_string()))?;
        let col_ref = Self::parse_column_ref(var_pair)?;
            
        // Parse the operator
        let op = complex.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing operator".to_string()))?
            .as_str()
            .to_string();
            
        // Parse the value (right operand)
        let val_str = complex.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing second operand".to_string()))?
            .as_str();
            
        let literal = LiteralParser::parse(val_str)?;
        Ok(SelectClause::ComplexValue(col_ref, op, literal))
    }
}