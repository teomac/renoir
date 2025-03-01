use csv::Reader;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::Write;
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

pub fn parse_type_string(input: &str) -> Result<Vec<String>, ParseTypeError> {
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
            Ok(format!("{}", base_type))
        })
        .collect();

    types
}

pub fn get_csv_columns<P: AsRef<Path>>(path: P) -> Vec<String> {
    // Open the CSV file
    let file = File::open(path).expect("Unable to open file");
    let mut reader = Reader::from_reader(file);

    // Get the headers
    let headers = reader.headers().expect("Unable to read headers");

    // Convert headers into owned String values
    let columns: Vec<String> = headers.iter().map(|header| header.to_string()).collect();

    columns
}

pub fn combine_arrays(keys: &Vec<String>, values: &Vec<String>) -> HashMap<String, String> {
    let mut map = HashMap::new();

    for (key, value) in keys.iter().zip(values.iter()) {
        map.insert(key.clone(), value.clone());
    }

    map
}

fn parse_field(field: &str) -> (&str, &str) {
    let numeric_start = field
        .chars()
        .position(|c| c.is_numeric())
        .unwrap_or(field.len());

    let base_type = &field[..numeric_start];
    let number = &field[numeric_start..];

    (base_type, number)
}

pub fn create_struct(fields: &Vec<(String, String)>, index: String) -> String {
    //insert the index into the struct name
    let mut output = format!(
        "#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n\
        struct StructVar{} {{\n",
        index
    );

    for (field_name, rust_type) in fields {
        writeln!(&mut output, "    {}: Option<{}>,", field_name, rust_type).unwrap();
    }

    output.push_str("}\n");
    output
}

pub fn generate_field_list(fields: &Vec<String>) -> Vec<(String, String)> {
    //insert the index into the struct name

    let mut field_list = Vec::new();

    for field_desc in fields {
        let (base_type, number) = parse_field(field_desc);
        let field_name = format!("{}{}", base_type, number);
        let rust_type = match base_type {
            "int" | "integer" | "i64" | "i32" => "i64",
            "float" | "f64" | "f32" => "f64",
            "bool" | "boolean" => "bool",
            "str" | "string" | "String" => "String",
            _ => panic!("Unsupported type: {}", base_type),
        };

        field_list.push((field_name.clone(), rust_type.to_string()));
    }

    field_list
}
