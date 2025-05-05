use super::error::IrParseError;
use super::ir_ast_structure::IrLiteral;

pub struct LiteralParser;

impl LiteralParser {
    pub(crate) fn parse(val: &str) -> Result<IrLiteral, Box<IrParseError>> {
        // Try parsing as boolean first
        if val == "true" || val == "false" {
            return Ok(IrLiteral::Boolean(val == "true"));
        }
        // Try parsing as float since integers can be parsed as floats
        if let Ok(float_val) = val.parse::<f64>() {
            // Check if it's actually an integer
            if float_val.fract() == 0.0 {
                if let Ok(int_val) = val.parse::<i64>() {
                    return Ok(IrLiteral::Integer(int_val));
                }
            }
            Ok(IrLiteral::Float(float_val))
        } else if let Ok(bool_val) = val.parse::<bool>() {
            Ok(IrLiteral::Boolean(bool_val))
        } else {
            // Handle string literals - strip quotes if present
            let cleaned_val = val.trim_matches('"').trim_matches('\'').to_string();
            Ok(IrLiteral::String(cleaned_val))
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
            Ok(IrLiteral::Integer(42))
        ));
    }

    #[test]
    fn test_parse_float() {
        assert!(matches!(
            LiteralParser::parse("42.5"),
            Ok(IrLiteral::Float(42.5))
        ));
    }

    #[test]
    fn test_parse_boolean() {
        assert!(matches!(
            LiteralParser::parse("true"),
            Ok(IrLiteral::Boolean(true))
        ));
        assert!(matches!(
            LiteralParser::parse("false"),
            Ok(IrLiteral::Boolean(false))
        ));
    }

    #[test]
    fn test_parse_string() {
        assert!(matches!(
            LiteralParser::parse("\"hello\""),
            Ok(IrLiteral::String(s)) if s == "hello"
        ));
    }
}
