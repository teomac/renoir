use csv::Reader;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::path::Path;

#[derive(Debug)]
pub struct ParseTypeError {
    message: String,
}

impl fmt::Display for ParseTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ParseTypeError {}

/// Parses a comma-separated string of types and returns a vector of strings representing the types.
pub(crate) fn parse_type_string(input: &str) -> Result<Vec<String>, ParseTypeError> {
    // Check for empty input
    if input.trim().is_empty() {
        return Err(ParseTypeError {
            message: "Empty input string".to_string(),
        });
    }

    // Split the string by commas and process each type
    let types: Result<Vec<String>, ParseTypeError> = input
        .split(',')
        .map(|t| {
            let trimmed = t.trim();
            if trimmed.is_empty() {
                return Err(ParseTypeError {
                    message: "Empty type in comma-separated list".to_string(),
                });
            }

            let base_type = match trimmed.to_lowercase().as_str() {
                "int" | "integer" | "i64" | "i32" => "i64",
                "str" | "string" | "String" => "String",
                "float" | "f64" | "f32" => "f64",
                "bool" | "boolean" => "bool",
                invalid_type => {
                    return Err(ParseTypeError {
                        message: format!("Invalid type: {}", invalid_type),
                    })
                }
            };

            // Create the indexed type string
            Ok(base_type.to_string())
        })
        .collect();

    types
}

/// Reads a CSV file and returns a vector of column names.
pub(crate) fn get_csv_columns<P: AsRef<Path>>(path: P) -> Vec<String> {
    // Open the CSV file
    let file = File::open(path).expect("Unable to open file");
    let mut reader = Reader::from_reader(file);

    // Get the headers
    let headers = reader.headers().expect("Unable to read headers");

    // Convert headers into owned String values
    let columns: Vec<String> = headers.iter().map(|header| header.to_string()).collect();

    columns
}
