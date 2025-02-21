pub mod builder;
pub mod error;
pub mod where_clause;
pub mod select;
pub mod from;
pub mod literal;
pub mod group_by;
pub mod sql_ast_structure;
pub mod order;
pub mod limit;
pub use sql_ast_structure::{
    SqlAST, 
    WhereCondition, 
    FromClause, 
    SelectClause, 
    WhereClause, 
    ComparisonOp as SqlOperator,
    SelectType,
    AggregateFunction
};

use pest::Parser;
use pest_derive::Parser;
use crate::dsl::languages::sql::ast_parser::error::SqlParseError;
use crate::dsl::languages::sql::ast_parser::builder::SqlASTBuilder;

//test
#[derive(Parser)]
#[grammar = "dsl/languages/sql/sql_grammar.pest"]


pub struct SqlParser;

impl SqlParser {
    pub fn parse_query(input: &str) -> Result<SqlAST, SqlParseError> {
        let pairs = Self::parse(Rule::query, input)
            .map_err(|e| SqlParseError::PestError(e))?;

        //println!("Pairs: {:?}", pairs);
        
        SqlASTBuilder::build_ast_from_pairs(pairs)
    }
}