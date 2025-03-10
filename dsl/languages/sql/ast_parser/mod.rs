pub mod builder;
pub mod error;
pub mod from;
pub mod group_by;
pub mod limit;
pub mod literal;
pub mod order;
pub mod select;
pub mod sql_ast_structure;
pub mod validate;
pub mod where_clause;
use pest::iterators::Pair;
pub use sql_ast_structure::{
    AggregateFunction, ComparisonOp as SqlOperator, FromClause, SelectColumn, SelectType, SqlAST,
    WhereClause, WhereCondition,
};

use crate::dsl::languages::sql::ast_parser::builder::SqlASTBuilder;
use crate::dsl::languages::sql::ast_parser::error::SqlParseError;
use pest::Parser;
use pest_derive::Parser;

//test
#[derive(Parser)]
#[grammar = "dsl/languages/sql/sql_grammar.pest"]

pub struct SqlParser;

impl SqlParser {
    pub fn parse_query(input: &str) -> Result<SqlAST, SqlParseError> {
        let pairs = Self::parse(Rule::query, input).map_err(|e| SqlParseError::PestError(e))?;

        //println!("Pairs: {:?}", pairs);

        SqlASTBuilder::build_ast_from_pairs(pairs)
    }

    pub fn parse_subquery(pair: Pair<Rule>) -> Result<SqlAST, SqlParseError> {
        if pair.as_rule() != Rule::subquery_expr {
            return Err(SqlParseError::InvalidInput(format!(
                "Expected subquery expression, got {:?}",
                pair.as_rule()
            )));
        }
        
        println!("Parsing subquery: {:?}", pair);
        
        // For subqueries, we need to create a new parser instance
        // We'll extract the SQL text from the subquery and parse it directly
        let subquery_text = pair.as_str();
        println!("Subquery text: {}", subquery_text);
        
        // Remove the outer parentheses
        let inner_sql = if subquery_text.starts_with("(") && subquery_text.ends_with(")") {
            &subquery_text[1..subquery_text.len()-1]
        } else {
            subquery_text
        };
        println!("Inner SQL: {}", inner_sql);
        
        // Parse the inner SQL directly using the main parser
        use crate::dsl::languages::sql::ast_parser::SqlParser;
        SqlParser::parse_query(inner_sql)
    }
}
