use pest::iterators::Pair;
use super::ast_structure::*;
use super::error::AquaParseError;
use crate::dsl::ir::aqua::ast_parser::Rule;

pub struct GroupParser;

impl GroupParser {
    pub fn parse(pair: Pair<Rule>) -> Result<GroupByClause, AquaParseError> {
        let mut inner = pair.into_inner();

        inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing group keyword".to_string()))?;
        
        // Get the group by list
        let group_list = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing group columns".to_string()))?;
        //println!("grouplist: {:?}", group_list);

        let mut columns = Vec::new();
        let mut group_condition= None;

        //process group columns first
        for item in group_list.into_inner() {
            columns.push(Self::parse_column_ref(item)?);
        }

        if columns.is_empty() {
            return Err(AquaParseError::InvalidInput("Empty group clause".to_string()));
        }

        // Check for condition
        if let Some(condition) = inner.next() {
            group_condition = Some(Self::parse_group_conditions(condition)?);
        }
        
        Ok(GroupByClause { 
            columns,
            group_condition,
        })
    }



    //function to parse column ref
    fn parse_column_ref(pair: Pair<Rule>) -> Result<ColumnRef, AquaParseError> {
        match pair.as_rule() {
            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let table = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing column name".to_string()))?
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
                format!("Expected column reference, got {:?}", pair.as_rule())
            )),
        }
    }


     //////////////////////////////////////////////////////////////////////////////////

    //function to parse having conditions
    fn parse_group_conditions(pair: Pair<Rule>) -> Result<GroupCondition, AquaParseError> {
        let mut pairs = pair.into_inner().peekable();
        
        let first_condition = pairs.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing condition".to_string()))?;
        let mut current = GroupCondition {
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
                    _ => return Err(AquaParseError::InvalidInput("Invalid binary operator".to_string())),
                };
                
                last.binary_op = Some(op);
                last.next = Some(Box::new(GroupCondition{
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

    fn parse_single_condition(condition_pair: Pair<Rule>) -> Result<Condition, AquaParseError> {
        let mut inner = condition_pair.into_inner();

        //parse left field
        let left_field_pair = inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing variable in condition".to_string()))?;

        let left_field = Self::parse_field(left_field_pair)?;
            
        let operator = match inner.next()
            .ok_or_else(|| AquaParseError::InvalidInput("Missing operator in left field".to_string()))?
            .as_str() 
        {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            ">=" => ComparisonOp::GreaterThanEquals,
            "<=" => ComparisonOp::LessThanEquals,
            "=" | "==" => ComparisonOp::Equal,
            "!=" | "<>" => ComparisonOp::NotEqual,
            op => return Err(AquaParseError::InvalidInput(format!("Invalid operator: {}", op))),
        };

        //parse right condition

        let right_field_pair = inner.next()
            .ok_or_else(|| AquaParseError   ::InvalidInput("Missing value or variable in right field".to_string()))?;

        let right_field = Self::parse_field(right_field_pair)?;

        Ok(Condition {
            left_field,
            operator,
            right_field,
        })
    }

     // New helper function to parse column references
     fn parse_field(pair: Pair<Rule>) -> Result<ComplexField, AquaParseError> {
        match pair.as_rule() {
            Rule::value => {
                //first we try to parse as int
                let value = pair.as_str().parse::<i64>()
                    .map(AquaLiteral::Integer)
                    .unwrap_or_else(|_| {
                        //if it fails, we try to parse as float
                        pair.as_str().parse::<f64>()
                            .map(AquaLiteral::Float)
                            .unwrap_or_else(|_| {
                                //parse as boolean
                                match pair.as_str() {
                                    "true" => AquaLiteral::Boolean(true),
                                    "false" => AquaLiteral::Boolean(false),
                                    _ => {
                                        //if it fails, we return as string
                                        AquaLiteral::String(pair.as_str().to_string())
                                    }
                                }
                            })
                    });
                Ok(ComplexField{
                    column: None,
                    literal: Some(value),
                    aggregate: None,
                })

            }

            Rule::aggregate_expr => {
                let mut inner = pair.into_inner();
                let function = match inner.next().unwrap().as_str() {
                    "sum" => AggregateType::Sum,
                    "avg" => AggregateType::Avg,
                    "count" => AggregateType::Count,
                    "min" => AggregateType::Min,
                    "max" => AggregateType::Max,
                    _ => return Err(AquaParseError::InvalidInput("Invalid aggregate function".to_string())),
                };
                let column = Self::parse_column_ref(inner.next().unwrap())?;
                Ok(ComplexField{
                    column: None,
                    literal: None,
                    aggregate: Some(AggregateFunction{
                        function,
                        column
                    }),
                })
            }

            Rule::qualified_column => {
                let mut inner = pair.into_inner();
                let table = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing table name".to_string()))?
                    .as_str()
                    .to_string();
                let column = inner.next()
                    .ok_or_else(|| AquaParseError::InvalidInput("Missing column name".to_string()))?
                    .as_str()
                    .to_string();
                Ok(ComplexField{
                    column: Some(ColumnRef {
                        table: Some(table),
                        column,
                    }),
                    literal: None,
                    aggregate: None,
                })
            }
            Rule::identifier => {
                Ok(ComplexField{
                    column: Some(ColumnRef {
                        table: None,
                        column: pair.as_str().to_string(),
                    }),
                    literal: None,
                    aggregate: None,
                })
            }
            _ => Err(AquaParseError::InvalidInput(format!("Expected column reference, got {:?}", pair.as_rule()))),
        }
    }
}