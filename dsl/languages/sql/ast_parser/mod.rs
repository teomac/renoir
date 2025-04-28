pub(crate) mod builder;
pub(crate) mod error;
pub(crate) mod from;
pub(crate) mod group_by;
pub(crate) mod limit;
pub(crate) mod literal;
pub(crate) mod order;
pub(crate) mod select;
pub(crate) mod sql_ast_structure;
pub(crate) mod validate;
pub(crate) mod where_clause;
use pest::iterators::Pair;
pub use sql_ast_structure::SqlAST;

use crate::dsl::languages::sql::ast_parser::builder::SqlASTBuilder;
use crate::dsl::languages::sql::ast_parser::error::SqlParseError;
use pest::Parser;
use pest_derive::Parser;

//test
#[derive(Parser)]
#[grammar = "dsl/languages/sql/sql_grammar.pest"]
pub struct SqlParser;

impl SqlParser {
    pub(crate) fn parse_query(input: &str) -> Result<SqlAST, Box<SqlParseError>> {
        let pairs =
            Self::parse(Rule::query, input).map_err(|e| Box::new(SqlParseError::from(e)))?;

        SqlASTBuilder::build_ast_from_pairs(pairs)
    }

    pub(crate) fn parse_subquery(pair: Pair<Rule>) -> Result<SqlAST, Box<SqlParseError>> {
        if pair.as_rule() != Rule::subquery_expr {
            return Err(Box::new(SqlParseError::InvalidInput(format!(
                "Expected subquery expression, got {:?}",
                pair.as_rule()
            ))));
        }

        // For subqueries, we need to create a new parser instance
        // We'll extract the SQL text from the subquery and parse it directly
        let subquery_text = pair.as_str();

        // Remove the outer parentheses
        let inner_sql = if subquery_text.starts_with("(") && subquery_text.ends_with(")") {
            &subquery_text[1..subquery_text.len() - 1]
        } else {
            subquery_text
        };

        // Parse the inner SQL directly using the main parser
        SqlParser::parse_query(inner_sql)
    }
}
