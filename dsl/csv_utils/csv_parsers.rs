use std::error::Error;
use std::fmt;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use csv::Reader;
use std::fmt::Write;

use crate::dsl::struct_object::object::QueryObject;

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

    let mut type_counters: HashMap<String, i32> = HashMap::new();

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
                "int" | "integer" | "i64" | "i32" => "int",
                "str" | "string" | "String" => "string",
                "float" | "f64" | "f32" => "float",
                "bool" | "boolean" => "bool",
                invalid_type => return Err(ParseTypeError {
                    message: format!("Invalid type: {}", invalid_type),
                }),
            };

            // Increment counter for this type
            let counter = type_counters.entry(base_type.to_string()).or_insert(0);
            *counter += 1;

            // Create the indexed type string
            Ok(format!("{}{}", base_type, counter))
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
    let columns: Vec<String> = headers
        .iter()
        .map(|header| header.to_string())
        .collect();
    
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

pub fn create_struct(fields: &Vec<String>, index: String) -> String {
    //insert the index into the struct name
    let mut output = format!(
        "#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n\
        struct StructVar{} {{\n",
        index
    );

    let mut field_list = Vec::new();

    for field_desc in fields {
        let (base_type, number) = parse_field(field_desc);
        let field_name = format!("{}{}", base_type, number);
        let rust_type = match base_type {
            "int" => "i64",
            "float" => "f64",
            "bool" => "bool",
            "string" => "String",
            _ => panic!("Unsupported type: {}", base_type),
        };

        field_list.push((field_name.clone(), rust_type.to_string()));

        
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
            "int" => "i64",
            "float" => "f64",
            "bool" => "bool",
            "string" => "String",
            _ => panic!("Unsupported type: {}", base_type),
        };

        field_list.push((field_name.clone(), rust_type.to_string()));
    }

    field_list
}