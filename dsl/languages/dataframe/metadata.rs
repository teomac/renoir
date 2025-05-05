use std::{collections::HashMap, io};
use indexmap::IndexMap;
use serde_json::Value;
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
    pub fn extract_metadata(metadata_list: Vec<String>, csv_paths: Vec<String>) -> io::Result<IndexMap<String, (String, String)>> {
        if metadata_list.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                MetadataError::EmptyMetadata,
            ));
        }
        
        let mut input_tables: IndexMap<String, (String, String)> = IndexMap::new();
        let mut csv_path_index = 0;
        
        for metadata_json in metadata_list {
            // Parse the metadata JSON
            let metadata: Value = serde_json::from_str(&metadata_json)
                .map_err(|e| io::Error::new(
                    io::ErrorKind::InvalidData,
                    MetadataError::JsonParsing(e.to_string()),
                ))?;
            
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
                        Some(cols) => cols.as_array().ok_or_else(|| io::Error::new(
                            io::ErrorKind::InvalidData,
                            MetadataError::MissingField("columns".to_string()),
                        ))?,
                        None => return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            MetadataError::MissingField("columns".to_string()),
                        )),
                    };
                    
                    // Generate type definition string: "col1:type1,col2:type2,..."
                    let mut type_defs = String::new();
                    for (i, column) in columns.iter().enumerate() {
                        let name = column.get("name").and_then(|n| n.as_str()).ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                MetadataError::MissingField("column name".to_string()),
                            )
                        })?;
                        
                        let spark_type = column.get("type").and_then(|t| t.as_str()).ok_or_else(|| {
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
                            _ => "String", // Default to String for unknown types
                        };
                        
                        if i > 0 {
                            type_defs.push(',');
                        }
                        type_defs.push_str(&format!("{}:{}", name, renoir_type));
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


/// Extract expression IDs and their corresponding table information from the Catalyst plan
/// 
/// # Arguments
/// * `catalyst_plan` - The Catalyst logical plan as a JSON Value array
/// * `input_tables` - IndexMap containing table information (table_name -> (csv_path, type_defs))
/// 
/// # Returns
/// * HashMap<String, String> mapping expression IDs to table names
pub fn extract_expr_ids(catalyst_plan: &[Value], input_tables: &IndexMap<String, (String, String)>) -> HashMap<String, String> {
    let mut expr_to_table: HashMap<String, String> = HashMap::new();
    
    // Process LogicalRDD nodes to extract expression IDs
    for node in catalyst_plan.iter() {
        if let Some(class) = node["class"].as_str() {
            if class.ends_with("LogicalRDD") {
                // Extract column information from this RDD
                let mut rdd_columns = Vec::new();
                if let Some(output) = node["output"].as_array() {
                    for column_list in output.iter() {
                        if let Some(columns) = column_list.as_array() {
                            for column in columns.iter() {
                                if let Some(name) = column.get("name").and_then(|n| n.as_str()) {
                                    rdd_columns.push(name.to_string());
                                }
                            }
                        }
                    }
                }
                
                // Match this RDD with a table in input_tables based on column names
                let table_name = match_table_by_columns(&rdd_columns, input_tables);
                
                // Now map all the expression IDs to this table
                if let Some(output) = node["output"].as_array() {
                    for column_list in output.iter() {
                        if let Some(columns) = column_list.as_array() {
                            for column in columns.iter() {
                                if let Some(expr_id_obj) = column.get("exprId") {
                                    // Extract expression ID
                                    if let (Some(id), Some(jvm_id)) = (
                                        expr_id_obj.get("id").and_then(|id| id.as_u64()),
                                        expr_id_obj.get("jvmId").and_then(|j| j.as_str())
                                    ) {
                                        let expr_id_str = format!("{}_{}", id, jvm_id);
                                        
                                        // Map expression ID to table
                                        expr_to_table.insert(expr_id_str, table_name.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Now handle aliases and projections to propagate table assignments
    process_alias_propagation(catalyst_plan, &mut expr_to_table);
    
    expr_to_table
}


/// Process the plan to propagate table assignments through aliases
/// 
/// # Arguments
/// * `catalyst_plan` - The Catalyst logical plan as a JSON Value array
/// * `expr_to_table` - Mutable reference to expression ID -> table mapping
fn process_alias_propagation(catalyst_plan: &[Value], expr_to_table: &mut HashMap<String, String>) {
    // First, identify all alias relationships (new_expr_id -> source_expr_id)
    let mut alias_relationships: HashMap<String, String> = HashMap::new();
    
    for node in catalyst_plan {
        if let Some(class) = node["class"].as_str() {
            if class.ends_with("Project") {
                if let Some(proj_list) = node["projectList"].as_array() {
                    for proj_array in proj_list {
                        if let Some(projections) = proj_array.as_array() {
                            // Handle Alias expressions
                            if projections.len() >= 2 {
                                let first = &projections[0];
                                if let Some(first_class) = first["class"].as_str() {
                                    if first_class.ends_with("Alias") {
                                        // Get alias expression ID
                                        if let (Some(expr_id_obj), Some(child_idx)) = (
                                            first.get("exprId"),
                                            first.get("child").and_then(|c| c.as_u64())
                                        ) {
                                            if let (Some(id), Some(jvm_id)) = (
                                                expr_id_obj.get("id").and_then(|id| id.as_u64()),
                                                expr_id_obj.get("jvmId").and_then(|j| j.as_str())
                                            ) {
                                                let alias_id = format!("{}_{}", id, jvm_id);
                                                
                                                // Find the source column
                                                let source_idx = (child_idx + 1) as usize;
                                                if projections.len() > source_idx {
                                                    let source = &projections[source_idx];
                                                    if let Some(source_expr_id) = source.get("exprId") {
                                                        if let (Some(src_id), Some(src_jvm_id)) = (
                                                            source_expr_id.get("id").and_then(|id| id.as_u64()),
                                                            source_expr_id.get("jvmId").and_then(|j| j.as_str())
                                                        ) {
                                                            let source_id = format!("{}_{}", src_id, src_jvm_id);
                                                            alias_relationships.insert(alias_id, source_id);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Now propagate table assignments through aliases
    // We may need multiple passes to handle chains of aliases
    let mut changes_made = true;
    let mut iterations = 0;
    let max_iterations = 10; // Prevent infinite loops
    
    while changes_made && iterations < max_iterations {
        changes_made = false;
        iterations += 1;
        
        // Clone to avoid borrowing issues
        let relationships = alias_relationships.clone();
        
        for (alias_id, source_id) in relationships {
            // If the source has a table but the alias doesn't, propagate
            if let Some(table) = expr_to_table.get(&source_id) {
                if !expr_to_table.contains_key(&alias_id) {
                    expr_to_table.insert(alias_id, table.clone());
                    changes_made = true;
                }
            }
        }
    }
}

/// Match an RDD to a table from input_tables based on column names
/// 
/// # Arguments
/// * `rdd_columns` - List of column names from the RDD
/// * `input_tables` - IndexMap containing table information
/// 
/// # Returns
/// * String with the matched table name, or a fallback name if no match
fn match_table_by_columns(rdd_columns: &[String], input_tables: &IndexMap<String, (String, String)>) -> String {
    for (table_name, (_, type_defs)) in input_tables {
        // Extract column names from type_defs
        let table_columns: Vec<String> = type_defs
            .split(',')
            .filter_map(|def| {
                def.split(':').next().map(|col| col.trim().to_string())
            })
            .collect();
        
        // Check if this table matches the RDD columns
        let mut match_score = 0;
        for rdd_col in rdd_columns {
            if table_columns.contains(rdd_col) {
                match_score += 1;
            }
        }
        
        // If we found a good match (at least 75% of columns match)
        if match_score >= (rdd_columns.len() * 3 / 4) {
            return table_name.clone();
        }
    }
    
    // If no good match found, return a generic name
    "unknown_table".to_string()
}