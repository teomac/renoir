use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::Rule;

pub struct GroupByParser;

impl GroupByParser {
    pub fn parse(pair: Pair<Rule>) -> Result<GroupByClause, SqlParseError> {
        let mut inner = pair.into_inner();
        
        inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing GROUP BY keyword".to_string()))?;
        
        // Get the group by list
        let group_by_list = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing GROUP BY columns".to_string()))?;
        println!("groupbylist: {:?}", group_by_list);
            
        let mut columns = Vec::new();

        let mut having: Option<HavingClause> = None;
        
        // Process group by items first
        for item in group_by_list.into_inner() {
            columns.push(Self::parse_column_ref(item)?);
        }
        
        // Check for HAVING clause
        while let Some(next_token) = inner.next() {
            match next_token.as_rule() {
                Rule::having_keyword => {
                    if let Some(having_conditions) = inner.next() {
                        having = Some(Self::parse_having_conditions(having_conditions)?);
                    }
                }
                _ => {}
            }
        }
        
        if columns.is_empty() {
            return Err(SqlParseError::InvalidInput("Empty GROUP BY clause".to_string()));
        }
        
        Ok(GroupByClause { 
            columns,
            having,
        })
    }
    
    //function to parse column reference
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, SqlParseError> {
        match pair.as_rule() {
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
            _ => Err(SqlParseError::InvalidInput(
                format!("Expected column reference, got {:?}", pair.as_rule())
            )),
        }
    }

    //////////////////////////////////////////////////////////////////////////////////

    //function to parse having conditions
    fn parse_having_conditions(pair: Pair<Rule>) -> Result<HavingClause, SqlParseError> {
        let mut pairs = pair.into_inner().peekable();
        
        let first_condition = pairs.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing condition".to_string()))?;
        let mut current = HavingClause {
            condition: Self::parse_single_condition(first_condition)?,
            binary_op: None,
            next: None
        };
        
        let mut last = &mut current;
        
        while let Some(op_pair) = pairs.next() {
            if let Some(condition_pair) = pairs.next() {
                let op = match op_pair.as_str().to_uppercase().as_str() {
                    "AND" => BinaryOp::And,
                    "OR" => BinaryOp::Or,
                    _ => return Err(SqlParseError::InvalidInput("Invalid binary operator".to_string())),
                };
                
                last.binary_op = Some(op);
                last.next = Some(Box::new(HavingClause {
                    condition: Self::parse_single_condition(condition_pair)?,
                    binary_op: None,
                    next: None,
                }));
                
                if let Some(ref mut next) = last.next {
                    last = next;
                }
            }
        }
        
        Ok(current)
    }

    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<HavingCondition, SqlParseError> {
        let mut inner = condition_pair.into_inner();

        //parse left field
        let left_field_pair = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing variable in condition".to_string()))?;

        let left_field = Self::parse_having_field(left_field_pair)?;
            
        let operator = match inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing operator in left field".to_string()))?
            .as_str() 
        {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            ">=" => ComparisonOp::GreaterOrEqualThan,
            "<=" => ComparisonOp::LessOrEqualThan,
            "=" => ComparisonOp::Equal,
            "!=" | "<>" => ComparisonOp::NotEqual,
            op => return Err(SqlParseError::InvalidInput(format!("Invalid operator: {}", op))),
        };

        //parse right condition

        let right_field_pair = inner.next()
            .ok_or_else(|| SqlParseError::InvalidInput("Missing value or variable in right field".to_string()))?;

        let right_field = Self::parse_having_field(right_field_pair)?;

        Ok(HavingCondition {
            left_field,
            operator,
            right_field,
        })
    }

    // New helper function to parse column references
    fn parse_having_field(pair: Pair<Rule>) -> Result<HavingField, SqlParseError> {
        match pair.as_rule() {
            Rule::number => {
                //first we try to parse as int
                let value = pair.as_str().parse::<i64>()
                    .map(SqlLiteral::Integer)
                    .unwrap_or_else(|_| {
                        //if it fails, we try to parse as float
                        pair.as_str().parse::<f64>()
                            .map(SqlLiteral::Float)
                            .unwrap_or_else(|_| {
                                //parse as boolean
                                match pair.as_str() {
                                    "true" => SqlLiteral::Boolean(true),
                                    "false" => SqlLiteral::Boolean(false),
                                    _ => {
                                        //if it fails, we return as string
                                        SqlLiteral::String(pair.as_str().to_string())
                                    }
                                }
                            })
                    });
                Ok(HavingField{
                    column: None,
                    value: Some(value),
                    aggregate: None,
                })

            }

            Rule::aggregate_expr => {
                let mut inner = pair.into_inner();
                let aggregate = match inner.next().unwrap().as_str() {
                    "SUM" => AggregateFunction::Sum,
                    "AVG" => AggregateFunction::Avg,
                    "COUNT" => AggregateFunction::Count,
                    "MIN" => AggregateFunction::Min,
                    "MAX" => AggregateFunction::Max,
                    _ => return Err(SqlParseError::InvalidInput("Invalid aggregate function".to_string())),
                };
                let column = Self::parse_column_ref(inner.next().unwrap())?;
                Ok(HavingField{
                    column: None,
                    value: None,
                    aggregate: Some((aggregate, column)),
                })
            }

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
                Ok(HavingField{
                    column: Some(ColumnRef {
                        table: Some(table),
                        column,
                    }),
                    value: None,
                    aggregate: None,
                })
            }
            Rule::variable => {
                Ok(HavingField{
                    column: Some(ColumnRef {
                        table: None,
                        column: pair.as_str().to_string(),
                    }),
                    value: None,
                    aggregate: None,
                })
            }
            _ => Err(SqlParseError::InvalidInput(format!("Expected column reference, got {:?}", pair.as_rule()))),
        }
    }
}