use super::ast_structure::AquaLiteral;
use super::error::AquaParseError;

pub struct LiteralParser;

impl LiteralParser {
    pub fn parse(val: &str) -> Result<AquaLiteral, AquaParseError> {
        // Try parsing as boolean first
        if val == "true" || val == "false" {
            return Ok(AquaLiteral::Boolean(val == "true"));
        }
        // Try parsing as float since integers can be parsed as floats
        if let Ok(float_val) = val.parse::<f64>() {
            // Check if it's actually an integer
            if float_val.fract() == 0.0 {
                if let Ok(int_val) = val.parse::<i64>() {
                    return Ok(AquaLiteral::Integer(int_val));
                }
            }
            Ok(AquaLiteral::Float(float_val))
        } else if let Ok(bool_val) = val.parse::<bool>() {
            Ok(AquaLiteral::Boolean(bool_val))
        } else {
            // Handle string literals - strip quotes if present
            let cleaned_val = val.trim_matches('"').trim_matches('\'').to_string();
            Ok(AquaLiteral::String(cleaned_val))
        }
    }

    pub fn parse_column_ref(column_ref: &str) -> Result<AquaLiteral, AquaParseError> {
        let parts: Vec<&str> = column_ref.split('.').collect();
        match parts.len() {
            1 => Ok(AquaLiteral::ColumnRef(super::ast_structure::ColumnRef {
                table: None,
                column: parts[0].to_string(),
            })),
            2 => Ok(AquaLiteral::ColumnRef(super::ast_structure::ColumnRef {
                table: Some(parts[0].to_string()),
                column: parts[1].to_string(),
            })),
            _ => Err(AquaParseError::InvalidInput(format!(
                "Invalid column reference format: {}",
                column_ref
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_integer() {
        assert!(matches!(
            LiteralParser::parse("42"),
            Ok(AquaLiteral::Integer(42))
        ));
    }

    #[test]
    fn test_parse_float() {
        assert!(matches!(
            LiteralParser::parse("42.5"),
            Ok(AquaLiteral::Float(42.5))
        ));
    }

    #[test]
    fn test_parse_boolean() {
        assert!(matches!(
            LiteralParser::parse("true"),
            Ok(AquaLiteral::Boolean(true))
        ));
        assert!(matches!(
            LiteralParser::parse("false"),
            Ok(AquaLiteral::Boolean(false))
        ));
    }

    #[test]
    fn test_parse_string() {
        assert!(matches!(
            LiteralParser::parse("\"hello\""),
            Ok(AquaLiteral::String(s)) if s == "hello"
        ));
    }

    #[test]
    fn test_parse_column_ref() {
        assert!(matches!(
            LiteralParser::parse_column_ref("table.column"),
            Ok(AquaLiteral::ColumnRef(ref cr)) if cr.table == Some("table".to_string()) && cr.column == "column"
        ));
    }
}