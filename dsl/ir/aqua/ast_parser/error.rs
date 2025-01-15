use std::error::Error;
use std::fmt::{self, Display};
use pest::error::Error as PestError;
use crate::dsl::ir::aqua::ast_parser::Rule;

#[derive(Debug)]
pub enum AquaParseError {
    PestError(PestError<Rule>),
    InvalidInput(String),
    InvalidType(String),
}

impl Display for AquaParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AquaParseError::PestError(e) => write!(f, "Parse error: {}", e),
            AquaParseError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            AquaParseError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
        }
    }
}

impl Error for AquaParseError {}

impl From<PestError<Rule>> for AquaParseError {
    fn from(error: PestError<Rule>) -> Self {
        AquaParseError::PestError(error)
    }
}