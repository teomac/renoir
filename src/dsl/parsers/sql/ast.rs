use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "dsl/parsers/sql/grammar.pest"]
pub struct SqlParser;

#[derive(Debug, PartialEq, Clone)]
pub struct SqlAST {
    pub select: SelectClause,
    pub from: FromClause,
    pub filter: WhereClause,
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
    pub operator: ComparisonOp,
    pub value: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ComparisonOp {
    GreaterThan,
    LessThan, 
    Equals,
}

impl SqlAST {
    pub fn parse(input: &str) -> Result<Self, pest::error::Error<Rule>> {
        let pairs = SqlParser::parse(Rule::query, input)?;
        
        for pair in pairs {
            if let Rule::query = pair.as_rule() {
                let mut inner = pair.into_inner();
                inner.next(); // Skip SELECT
                
                let select_var = inner.next().unwrap().as_str().to_string();
                inner.next(); // Skip FROM
                
                let table = inner.next().unwrap().as_str().to_string();
                inner.next(); // Skip WHERE
                
                let expr = inner.next().unwrap().into_inner();
                let condition = Self::parse_condition(expr);

                return Ok(SqlAST {
                    select: SelectClause {
                        variable: select_var,
                    },
                    from: FromClause {
                        table,
                    },
                    filter: WhereClause {
                        condition,
                    },
                });
            }
        }
        unreachable!()
    }

    fn parse_condition(mut expr: pest::iterators::Pairs<Rule>) -> Condition {
        let expr = expr.next().unwrap().into_inner();
        
        let mut parts = expr.into_iter();
        let variable = parts.next().unwrap().as_str().to_string();
        
        let operator = match parts.next().unwrap().as_str() {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            "=" => ComparisonOp::Equals,
            _ => unreachable!(),
        };
        
        let value = parts.next().unwrap().as_str().parse().unwrap();

        Condition {
            variable,
            operator,
            value,
        }
    }
}