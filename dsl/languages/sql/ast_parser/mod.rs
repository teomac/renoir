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
}
