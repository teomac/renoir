use std::error::Error;
use std::fmt::{self, Display};
use pest::error::Error as PestError;
use crate::dsl::ir::ast_parser::Rule;

#[derive(Debug)]
pub enum IrParseError {
    PestError(PestError<Rule>),
    InvalidInput(String),
    InvalidType(String),
}

impl Display for IrParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IrParseError::PestError(e) => write!(f, "Parse error: {}", e),
            IrParseError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            IrParseError::InvalidType(msg) => write!(f, "Invalid type: {}", msg),
        }
    }
}

impl Error for IrParseError {}

impl From<PestError<Rule>> for IrParseError {
    fn from(error: PestError<Rule>) -> Self {
        IrParseError::PestError(error)
    }
}