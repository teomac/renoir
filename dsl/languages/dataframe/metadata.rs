use indexmap::IndexMap;
use serde_json::Value;
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Failed to parse JSON: {0}")]
    JsonParsing(String),

    #[error("Invalid metadata structure")]
    InvalidStructure,

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Empty metadata")]
    EmptyMetadata,

    #[error("No tables provided")]
    NoTables,

    #[error("Insufficient CSV paths: {0} tables but only {1} CSV paths")]
    InsufficientCsvPaths(usize, usize),
}

/// Converts a list of metadata JSON strings and CSV paths into an IndexMap of table names to (CSV path, type definitions)
///
/// # Arguments
/// * `metadata_list` - List of JSON strings containing table metadata
/// * `csv_paths` - List of CSV paths to be assigned in order to each table
///
/// # Returns
/// * `IndexMap<String, (String, String)>` - Map of table name to (CSV path, type definitions)
pub fn extract_metadata(
    metadata_list: Vec<String>,
    csv_paths: Vec<String>,
) -> io::Result<IndexMap<String, (String, IndexMap<String, String>)>> {
    if metadata_list.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            MetadataError::EmptyMetadata,
        ));
    }

    let mut input_tables: IndexMap<String, (String, IndexMap<String, String>)> = IndexMap::new();
    let mut csv_path_index = 0;

    for metadata_json in metadata_list {
        // Parse the metadata JSON
        let metadata: Value = serde_json::from_str(&metadata_json).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                MetadataError::JsonParsing(e.to_string()),
            )
        })?;

        // Process each table in the metadata
        if let Some(tables) = metadata.as_object() {
            if tables.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    MetadataError::NoTables,
                ));
            }

            for (table_name, table_info) in tables {
                // Check if we have enough CSV paths
                if csv_path_index >= csv_paths.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        MetadataError::InsufficientCsvPaths(tables.len(), csv_paths.len()),
                    ));
                }

                // Get the next CSV path in order
                let csv_path = &csv_paths[csv_path_index];
                csv_path_index += 1;

                // Extract column information for type definitions
                let columns = match table_info.get("columns") {
                    Some(cols) => cols.as_array().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            MetadataError::MissingField("columns".to_string()),
                        )
                    })?,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            MetadataError::MissingField("columns".to_string()),
                        ))
                    }
                };

                // Generate type definition string: "col1:type1,col2:type2,..."
                let mut type_defs = IndexMap::new();
                for column in columns.iter() {
                    let name = column.get("name").and_then(|n| n.as_str()).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            MetadataError::MissingField("column name".to_string()),
                        )
                    })?;

                    let spark_type =
                        column.get("type").and_then(|t| t.as_str()).ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                MetadataError::MissingField("column type".to_string()),
                            )
                        })?;

                    // Convert Spark types to Renoir types
                    let renoir_type = match spark_type {
                        "LongType()" => "i64",
                        "IntegerType()" => "i64",
                        "DoubleType()" => "f64",
                        "FloatType()" => "f64",
                        "StringType()" => "String",
                        "BooleanType()" => "bool",
                        "DateType()" => "String", // Using String for dates as a simplification
                        "TimestampType()" => "String", // Using String for timestamps as a simplification
                        _ => "String",                 // Default to String for unknown types
                    };

                    type_defs.insert(name.to_string(), renoir_type.to_string());
                }

                // Add the table to our input_tables map
                input_tables.insert(table_name.clone(), (csv_path.clone(), type_defs));
            }
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                MetadataError::InvalidStructure,
            ));
        }
    }

    if input_tables.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            MetadataError::NoTables,
        ));
    }

    Ok(input_tables)
}