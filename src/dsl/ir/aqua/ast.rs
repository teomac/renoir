use pest::Parser;
use pest_derive::Parser;
use pest::iterators::Pair;

#[derive(Parser)]
#[grammar = "dsl/ir/aqua/grammar.pest"]
pub struct AquaParser;

// Main AST structure for a query
#[derive(Debug, PartialEq, Clone)]
pub struct AquaAST {
    // Every query starts with a stream
    pub from: FromClause,
    // Selection (required) - either column or aggregation
    pub select: SelectClause,
    // Optional filtering condition
    pub filter: Option<Condition>,
}

// Stream source definition
#[derive(Debug, PartialEq, Clone)]
pub struct FromClause {
    pub stream_name: String,
}

// Select clause can be either a simple column or an aggregation
#[derive(Debug, PartialEq, Clone)]
pub enum SelectClause {
    Column(String),              // Simple column selection
    Aggregate(AggregateFunction), // Aggregation function
    ComplexOp(String, char, String),
    ComplexValue(String, char, f64),
}

// Aggregation function with its column
#[derive(Debug, PartialEq, Clone)]
pub struct AggregateFunction {
    pub function: AggregateType,
    pub column: String,
}

// Available aggregation functions
#[derive(Debug, PartialEq, Clone)]
pub enum AggregateType {
    Max,
    Min,
    Avg,
}

// Represents a filtering condition
#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub variable: String,
    pub operator: ComparisonOp,
    pub value: f64,
}

// Available comparison operators
#[derive(Debug, PartialEq, Clone)]
pub enum ComparisonOp {
    GreaterThan,
    LessThan,
    Equals,
    GreaterThanEquals,
    LessThanEquals,
}

impl AquaAST {
    pub fn parse(input: &str) -> Result<Self, pest::error::Error<Rule>> {
        let pairs = AquaParser::parse(Rule::query, input)?;
        let mut from = None;
        let mut select = None;
        let mut filter = None;

        // Parse the query pairs
        for pair in pairs.into_iter().next().unwrap().into_inner() {
            match pair.as_rule() {
                Rule::from_clause => {
                    from = Some(FromClause::parse(pair)?);
                }
                Rule::select_clause => {
                    select = Some(SelectClause::parse(pair)?);
                }
                Rule::where_clause => {
                    filter = Some(Condition::parse(pair)?);
                }
                Rule::EOI => {}
                _ => {}
            }
        }

        // If no explicit select clause, default to selecting all
        if select.is_none() {
            select = Some(SelectClause::Column("*".to_string()));
        }

        Ok(AquaAST {
            from: from.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing FROM clause".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
            select: select.unwrap_or_else(|| SelectClause::Column("*".to_string())),
            filter,
        })
    }
}

impl FromClause {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut stream_name = None;

        for inner_pair in pair.into_inner() {
            if let Rule::stream_identifier = inner_pair.as_rule() {
                stream_name = Some(inner_pair.as_str().to_string());
            }
        }

        Ok(FromClause {
            stream_name: stream_name.ok_or_else(|| pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Missing stream identifier".to_string(),
                },
                pest::Position::from_start(""),
            ))?,
        })
    }
}

impl SelectClause {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::aggregate_expr => {
                    let mut inner = inner_pair.into_inner();
                    let func = inner.next().unwrap();
                    let column = inner.next().unwrap();
                    
                    return Ok(SelectClause::Aggregate(AggregateFunction {
                        function: AggregateType::from_str(func.as_str())?,
                        column: column.as_str().to_string(),
                    }));
                }
                Rule::identifier => {
                    return Ok(SelectClause::Column(inner_pair.as_str().to_string()));
                }
                Rule::complex_op => {
                    let mut inner = inner_pair.into_inner();
                    let variable = inner.next().unwrap().as_str().to_string();
                    let operator = inner.next().unwrap().as_str().chars().next().unwrap();
                    let var2 = inner.next().unwrap().as_str().to_string();
                    match var2.parse::<f64>() {
                            Ok(var2) => return Ok(SelectClause::ComplexValue(variable, operator, var2)),
                            Err(_) => return Ok(SelectClause::ComplexOp(variable, operator, var2)),
                        }
                    }
                    _ => unreachable!(),
                }
            }

            Err(pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: "Invalid SELECT clause".to_string(),
                },
                pest::Position::from_start(""),
            ))
        }
    }

impl Condition {
    fn parse(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut inner = pair.into_inner();
        let condition = inner.next().unwrap().into_inner();
        
        let mut iter = condition.into_iter();
        let variable = iter.next().unwrap().as_str().to_string();
        let operator = ComparisonOp::from_str(iter.next().unwrap().as_str())?;
        let value = iter.next().unwrap().as_str().parse().unwrap();

        Ok(Condition {
            variable,
            operator,
            value,
        })
    }
}

impl ComparisonOp {
    fn from_str(s: &str) -> Result<Self, pest::error::Error<Rule>> {
        match s {
            ">" => Ok(ComparisonOp::GreaterThan),
            "<" => Ok(ComparisonOp::LessThan),
            "==" => Ok(ComparisonOp::Equals),
            ">=" => Ok(ComparisonOp::GreaterThanEquals),
            "<=" => Ok(ComparisonOp::LessThanEquals),
            _ => Err(pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: format!("Invalid comparison operator: {}", s),
                },
                pest::Position::from_start(""),
            )),
        }
    }
}

impl AggregateType {
    fn from_str(s: &str) -> Result<Self, pest::error::Error<Rule>> {
        match s {
            "max" => Ok(AggregateType::Max),
            "min" => Ok(AggregateType::Min),
            "avg" => Ok(AggregateType::Avg),
            _ => Err(pest::error::Error::new_from_pos(
                pest::error::ErrorVariant::CustomError {
                    message: format!("Invalid aggregate function: {}", s),
                },
                pest::Position::from_start(""),
            )),
        }
    }
}

// Example usage and tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql_style() {
        let ast = AquaAST::parse("from input:Stream select max where value > 10").unwrap();
        assert_eq!(ast.from.stream_name, "input");
        assert!(matches!(ast.select, SelectClause::Aggregate(AggregateFunction {
            function: AggregateType::Max,
            ..
        })));
        assert!(matches!(ast.filter, Some(Condition {
            variable: _,
            operator: ComparisonOp::GreaterThan,
            value: 10.0,
        })));
    }

    #[test]
    fn test_parse_method_style() {
        let ast = AquaAST::parse("input:Stream.filter(value > 10).max(value)").unwrap();
        assert_eq!(ast.from.stream_name, "input");
        assert!(matches!(ast.select, SelectClause::Aggregate(AggregateFunction {
            function: AggregateType::Max,
            ..
        })));
        assert!(matches!(ast.filter, Some(Condition {
            variable: _,
            operator: ComparisonOp::GreaterThan,
            value: 10.0,
        })));
    }
}