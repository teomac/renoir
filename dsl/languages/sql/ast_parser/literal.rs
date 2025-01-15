use super::ast_structure::SqlLiteral;
use super::error::SqlParseError;

pub struct LiteralParser;

impl LiteralParser {
    pub fn parse(val: &str) -> Result<SqlLiteral, SqlParseError> {
        if let Ok(float_val) = val.parse::<f64>() {
            Ok(SqlLiteral::Float(float_val))
        } else if let Ok(int_val) = val.parse::<i64>() {
            Ok(SqlLiteral::Integer(int_val))
        } else if let Ok(bool_val) = val.parse::<bool>() {
            Ok(SqlLiteral::Boolean(bool_val))
        } else {
            Ok(SqlLiteral::String(val.to_string()))
        }
    }
}