use crate::dsl::languages::sql::ast_parser::Rule;
use pest::error::Error as PestError;
use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug)]
pub enum SqlParseError {
    PestError(Box<PestError<Rule>>),
    InvalidInput(String),
    InvalidType(String),
    // Add other error types as needed
}

impl Display for SqlParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SqlParseError::PestError(e) => write!(f, "Parse error: {}", e),
            SqlParseError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            SqlParseError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
        }
    }
}

impl Error for SqlParseError {}

impl From<PestError<Rule>> for SqlParseError {
    fn from(error: PestError<Rule>) -> Self {
        SqlParseError::PestError(Box::new(error))
    }
}
