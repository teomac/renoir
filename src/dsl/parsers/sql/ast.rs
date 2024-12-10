use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "dsl/parsers/sql/grammar.pest"]
pub struct SqlParser;

#[derive(Debug, PartialEq, Clone)]
pub struct SqlAST {
    pub select: SelectClause,
    pub from: FromClause,
    pub filter: Option<WhereClause>, // Made optional
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectClause {
    pub selection: SelectType,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectType {
    Simple(String),
    Aggregate(AggregateFunction, String),
    ComplexValue(String, char, i64),
}


#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunction {
    Max,
}

#[derive(Debug, PartialEq, Clone)] 
pub struct FromClause {
    pub table: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct  WhereClause {
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
                
                let select_part = inner.next().unwrap();
                let selection = match select_part.as_rule() {
                    Rule::variable => SelectType::Simple(select_part.as_str().to_string()),
                    Rule::aggregate_expr => {
                        let mut agg = select_part.into_inner();
                        let func = match agg.next().unwrap().as_str().to_uppercase().as_str() {
                            "MAX" => AggregateFunction::Max,
                            _ => unreachable!(),
                        };
                        let var = agg.next().unwrap().as_str().to_string();
                        SelectType::Aggregate(func, var)
                    },
                    Rule::select_expr => {
                        let mut complex = select_part.into_inner();
                        let var1 = complex.next().unwrap().as_str().to_string();
                        let op = complex.next().unwrap().as_str().chars().next().unwrap();
                        let val = complex.next().unwrap().as_str().parse().unwrap();
                        SelectType::ComplexValue(var1, op, val)
                        
                    },
                    _ => unreachable!(),
                };
                
                inner.next(); // Skip FROM
                let table = inner.next().unwrap().as_str().to_string();
                
                // Handle optional where expression
                let filter = match inner.next() {
                    Some(where_pair) if where_pair.as_rule() == Rule::where_expr => {
                        let expr = where_pair.into_inner().nth(1).unwrap().into_inner();
                        Some(WhereClause {
                            condition: Self::parse_condition(expr)
                        })
                    },
                    _ => None
                };

                return Ok(SqlAST {
                    select: SelectClause { selection },
                    from: FromClause { table },
                    filter,
                });
            }
        }
        unreachable!()
    }

    fn parse_condition(mut expr: pest::iterators::Pairs<Rule>) -> Condition {
        let variable = expr.next().unwrap().as_str().to_string();
        let operator = match expr.next().unwrap().as_str() {
            ">" => ComparisonOp::GreaterThan,
            "<" => ComparisonOp::LessThan,
            "=" => ComparisonOp::Equals,
            _ => unreachable!(),
        };
        let value = expr.next().unwrap().as_str().parse().unwrap();

        Condition {
            variable,
            operator,
            value,
        }
    }

    pub fn to_aqua_string(&self) -> String {
        let mut parts = Vec::new();

        // FROM clause
        parts.push(format!("from input:Stream"));

        // WHERE clause (only if present)
        if let Some(where_clause) = &self.filter {
            let condition = &where_clause.condition;
            parts.push(format!("where {} {} {}",
                condition.variable,
                SqlAST::convert_operator(&condition.operator),
                condition.value
            ));
        }

        // SELECT clause
        match &self.select.selection {
            SelectType::Simple(column) => {
                parts.push(format!("select {}", column));
            },
            SelectType::Aggregate(func, column) => {
                let agg = match func {
                    AggregateFunction::Max => "max",
                    // Add other aggregates as needed
                };
                parts.push(format!("select {}({})", agg, column));
            }
            SelectType::ComplexValue(var1, op, val) => {
                parts.push(format!("select {} {} {}", var1, op, val));
            }
        }

        // Join all parts with newlines
        parts.join("\n")
    }

    pub fn convert_operator(op: &ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::GreaterThan => ">",
        ComparisonOp::LessThan => "<",
        ComparisonOp::Equals => "==",
    }
}}