use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "operator/sql/grammar.pest"]
pub struct SQLParser;

#[derive(Debug, PartialEq, Clone)]
pub struct Query {
    pub select_clause: SelectClause,
    pub from_clause: FromClause,
    pub where_clause: WhereClause,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectClause {
    pub variable: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FromClause {
    pub table: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Condition {
    pub variable: String,
    pub operator: Operator,
    pub value: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    GreaterThan,
    LessThan,
    Equals,
}

impl Query {
    pub fn parse(input: &str) -> Result<Self, pest::error::Error<Rule>> {
        // Parse the input with the SQLParser
        let pairs = SQLParser::parse(Rule::query, input)?;

        // Iterate over pairs to find the query rule
        for pair in pairs {
            match pair.as_rule() {
                Rule::query => {
                    let mut inner_pairs = pair.into_inner();
                    inner_pairs.next();
                    // Extract components of the query
                    let select_var = inner_pairs.next().unwrap().as_str().to_string();
                    inner_pairs.next(); // Skipping the "FROM" keyword

                    let table = inner_pairs.next().unwrap().as_str().to_string();
                    inner_pairs.next(); // Skipping the "WHERE" keyword

                    let expr = inner_pairs.next().unwrap().into_inner();
                    let condition = Self::parse_condition(expr);

                    // Construct and return the Query struct
                    return Ok(Query {
                        select_clause: SelectClause {
                            variable: select_var,
                        },
                        from_clause: FromClause {
                            table,
                        },
                        where_clause: WhereClause {
                            condition,
                        },
                    });
                }
                _ => unreachable!(),
            }
        }
        unreachable!()
    }

    fn parse_condition(mut expr: pest::iterators::Pairs<Rule>) -> Condition {
        expr = expr.next().unwrap().into_inner();
        println!("expr: {}", expr);

        let variable = expr.next().unwrap().as_str().to_string();
        println!("variable: {}", variable);
        let operator = match expr.next().unwrap().as_str() {
            ">" => Operator::GreaterThan,
            "<" => Operator::LessThan,
            "=" => Operator::Equals,
            _ => unreachable!(),
        };
        let value = expr.next().unwrap().as_str().parse().unwrap();
        println!("value: {}", value);

        Condition {
            variable,
            operator,
            value,
        }
    }
}
