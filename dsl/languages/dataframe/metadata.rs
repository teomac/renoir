use indexmap::IndexMap;
use serde_json::Value;
use std::{collections::HashMap, io};
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

/// Extract expression IDs and their corresponding column and source information from the Catalyst plan
///
/// # Arguments
/// * `catalyst_plan` - The Catalyst logical plan as a JSON Value array
/// * `input_tables` - IndexMap containing table information (table_name -> (csv_path, type_defs))
///
/// # Returns
/// * IndexMap<usize, (String, String)> mapping expression IDs to (column_name, source_name)
pub fn extract_expr_ids(
    catalyst_plan: &[Value],
    input_tables: &IndexMap<String, (String, IndexMap<String, String>)>,
) -> IndexMap<usize, (String, String)> {
    let mut expr_to_column_source: IndexMap<usize, (String, String)> = IndexMap::new();

    // First pass: find all qualifiers in the entire plan recursively
    find_columns_from_qualifiers_recursive(catalyst_plan, &mut expr_to_column_source);

    // Second pass: apply some heuristics for expressions without qualifiers
    apply_column_heuristics(catalyst_plan, &mut expr_to_column_source, input_tables);

    // Sort keys for initial table matching
    expr_to_column_source.sort_unstable_keys();

    //Third pass: process LogicalRelation nodes to match them to input tables
    let _ = add_initial_table_mappings(&mut expr_to_column_source, input_tables);

    // Process alias propagation to propagate column assignments through aliases
    //process_alias_propagation(catalyst_plan, &mut expr_to_column_source);

    println!(
        "Expression to column/source mapping: {:?}",
        expr_to_column_source
    );
    expr_to_column_source
}

/// Recursively process the entire Catalyst plan to find qualifier information
fn find_columns_from_qualifiers_recursive(
    plan: &[Value],
    expr_to_column_source: &mut IndexMap<usize, (String, String)>,
) {
    for node in plan {
        // Process output columns at current level
        process_node_qualifiers(node, expr_to_column_source);

        // Handle various node types that might contain nested plans
        if let Some(obj) = node.as_object() {
            // Process direct child
            if let Some(child_idx) = obj.get("child").and_then(|c| c.as_u64()) {
                let child_idx = child_idx as usize + 1; // +1 because Catalyst indices are relative
                if plan.len() > child_idx {
                    find_columns_from_qualifiers_recursive(
                        &plan[child_idx..child_idx + 1],
                        expr_to_column_source,
                    );
                }
            }

            // Process left and right children for joins
            if let Some(left_idx) = obj.get("left").and_then(|l| l.as_u64()) {
                let left_idx = left_idx as usize + 1;
                if plan.len() > left_idx {
                    find_columns_from_qualifiers_recursive(
                        &plan[left_idx..left_idx + 1],
                        expr_to_column_source,
                    );
                }
            }

            if let Some(right_idx) = obj.get("right").and_then(|r| r.as_u64()) {
                let right_idx = right_idx as usize + 1;
                if plan.len() > right_idx {
                    find_columns_from_qualifiers_recursive(
                        &plan[right_idx..right_idx + 1],
                        expr_to_column_source,
                    );
                }
            }

            // Recursively process nested subquery plans
            if let Some(subplan) = obj.get("plan").and_then(|p| p.as_array()) {
                find_columns_from_qualifiers_recursive(subplan, expr_to_column_source);
            }

            // Process conditions which may contain subqueries
            if let Some(condition) = obj.get("condition").and_then(|c| c.as_array()) {
                // Process each element in the condition
                for cond in condition {
                    //process_node_qualifiers(cond, expr_to_column_source);

                    // If this condition contains a subquery, process it
                    if let Some(plan) = cond.get("plan").and_then(|p| p.as_array()) {
                        find_columns_from_qualifiers_recursive(plan, expr_to_column_source);
                    }
                }
            }

            // Process projection lists which may contain subqueries
            if let Some(project_list) = obj.get("projectList").and_then(|p| p.as_array()) {
                for proj_array in project_list {
                    if let Some(projections) = proj_array.as_array() {
                        for proj in projections {
                            //process_node_qualifiers(proj, expr_to_column_source);

                            // Handle subqueries in projections
                            if let Some(plan) = proj.get("plan").and_then(|p| p.as_array()) {
                                find_columns_from_qualifiers_recursive(plan, expr_to_column_source);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Process qualifier information in a node and extract column and source mappings
fn process_node_qualifiers(
    node: &Value,
    expr_to_column_source: &mut IndexMap<usize, (String, String)>,
) {
    // Extract qualifier, column name, and exprId if present
    if let Some(expr_id_obj) = node.get("exprId") {
        if let Some(id) = expr_id_obj.get("id").and_then(|id| id.as_u64()) {
            let expr_id = id as usize;

            // Get column name
            let column_name = node
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("unknown_column")
                .to_string();

            // Get source name from qualifier or default
            let source_name = if let Some(qualifier) = node.get("qualifier") {
                extract_table_from_qualifier(qualifier)
                    .unwrap_or_else(|| "unknown_source".to_string())
            } else {
                "unknown_source".to_string()
            };

            expr_to_column_source.insert(expr_id, (column_name, source_name));
        }
    }
}

/// Extract table name from qualifier value (handles both string and array formats)
fn extract_table_from_qualifier(qualifier: &Value) -> Option<String> {
    if qualifier.is_string() {
        if let Some(qual_str) = qualifier.as_str() {
            // Format: "[table_name]"
            if qual_str.starts_with('[') && qual_str.ends_with(']') {
                Some(qual_str[1..qual_str.len() - 1].to_string())
            } else {
                Some(qual_str.to_string())
            }
        } else {
            None
        }
    } else if let Some(qual_array) = qualifier.as_array() {
        // Handle array format if present
        if !qual_array.is_empty() {
            qual_array
                .first()
                .and_then(|q| q.as_str())
                .map(|s| s.to_string())
        } else {
            None
        }
    } else {
        None
    }
}

/// Apply heuristics for expressions without qualifier information
fn apply_column_heuristics(
    plan: &[Value],
    expr_to_column_source: &mut IndexMap<usize, (String, String)>,
    input_tables: &IndexMap<String, (String, IndexMap<String, String>)>,
) {
    // For LogicalRelation nodes without qualifiers, use table position
    let mut rdd_index = 0;

    for node in plan {
        if let Some(class) = node.get("class").and_then(|c| c.as_str()) {
            if class.ends_with("LogicalRDD") || class.ends_with("LogicalRelation") {
                // Find expression IDs that don't have mappings yet
                let table_name = match_relation_to_table(node, input_tables, rdd_index);
                rdd_index += 1;

                if let Some(output) = node.get("output").and_then(|o| o.as_array()) {
                    for column_list in output {
                        if let Some(columns) = column_list.as_array() {
                            for column in columns {
                                if let Some(expr_id_obj) = column.get("exprId") {
                                    if let Some(id) =
                                        expr_id_obj.get("id").and_then(|id| id.as_u64())
                                    {
                                        let expr_id = id as usize;

                                        // Get column name
                                        let column_name = column
                                            .get("name")
                                            .and_then(|n| n.as_str())
                                            .unwrap_or("unknown_column")
                                            .to_string();

                                        // Only add if not already mapped
                                        expr_to_column_source
                                            .entry(expr_id)
                                            .or_insert((column_name, table_name.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Recursively process subquery plans
            if let Some(obj) = node.as_object() {
                if let Some(subplan) = obj.get("plan").and_then(|p| p.as_array()) {
                    apply_column_heuristics(subplan, expr_to_column_source, input_tables);
                }
            }
        }
    }
}

/// Match a LogicalRelation node to a table name based on position or context
fn match_relation_to_table(
    node: &Value,
    input_tables: &IndexMap<String, (String, IndexMap<String, String>)>,
    rdd_index: usize,
) -> String {
    // First, check if there are any existing qualifiers we can use
    let mut table_from_qualifier = None;
    if let Some(output) = node.get("output").and_then(|o| o.as_array()) {
        for column_list in output {
            if let Some(columns) = column_list.as_array() {
                for column in columns {
                    if let Some(qualifier) = column.get("qualifier") {
                        if let Some(table) = extract_table_from_qualifier(qualifier) {
                            table_from_qualifier = Some(table);
                            break;
                        }
                    }
                }
                if table_from_qualifier.is_some() {
                    break;
                }
            }
        }
    }

    if let Some(table) = table_from_qualifier {
        return table;
    }

    // If no qualifier found, fall back to position-based assignment
    if input_tables.len() > 1 && rdd_index < input_tables.len() {
        // Get the table name at this index
        let table_name = input_tables.keys().nth(rdd_index).unwrap();
        return table_name.clone();
    }

    // Last resort: use "unknown_table" as fallback
    "unknown_table".to_string()
}

/// Process the plan to propagate column assignments through aliases
///
/// # Arguments
/// * `catalyst_plan` - The Catalyst logical plan as a JSON Value array
/// * `expr_to_column_source` - Mutable reference to expression ID -> (column_name, source_name) mapping
fn process_alias_propagation(
    catalyst_plan: &[Value],
    expr_to_column_source: &mut IndexMap<usize, (String, String)>,
) {
    // First, identify all alias relationships (new_expr_id -> source_expr_id)
    let mut alias_relationships: HashMap<usize, usize> = HashMap::new();

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
                                            first.get("child").and_then(|c| c.as_u64()),
                                        ) {
                                            if let Some(id) =
                                                expr_id_obj.get("id").and_then(|id| id.as_u64())
                                            {
                                                let alias_id = id as usize;

                                                // Find the source column
                                                let source_idx = (child_idx + 1) as usize;
                                                if projections.len() > source_idx {
                                                    let source = &projections[source_idx];
                                                    if let Some(source_expr_id) =
                                                        source.get("exprId")
                                                    {
                                                        if let Some(src_id) = source_expr_id
                                                            .get("id")
                                                            .and_then(|id| id.as_u64())
                                                        {
                                                            let source_id = src_id as usize;
                                                            alias_relationships
                                                                .insert(alias_id, source_id);
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

    // Now propagate column assignments through aliases
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
            // If the source has a column/source but the alias doesn't, propagate
            if let Some((column_name, source_name)) = expr_to_column_source.get(&source_id).cloned()
            {
                if let indexmap::map::Entry::Vacant(e) = expr_to_column_source.entry(alias_id) {
                    e.insert((column_name.clone(), source_name.clone()));
                    changes_made = true;
                }
            }
        }
    }
}

/// Add initial table mappings based on LogicalRelation nodes
fn add_initial_table_mappings(
    expr_to_column_source: &mut IndexMap<usize, (String, String)>,
    input_tables: &IndexMap<String, (String, IndexMap<String, String>)>,
) -> io::Result<()> {
    //Iterate through the input tables and match them to LogicalRelation nodes

    let mut offset: usize = *expr_to_column_source.first().unwrap().0;

    for table_name in input_tables.keys() {
        let mut processed_items: usize = 0;
        let mut latest_expr_id: usize = 0;
        let col_numbers = input_tables
            .get(table_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Table not found"))?
            .1
            .len();

        //now add the table name to the first n expression IDs in the expr_to_column_source map
        for i in 0..col_numbers {
            let expr_id = offset + i;
            if let indexmap::map::Entry::Vacant(e) = expr_to_column_source.entry(expr_id) {
                e.insert((format!("{}_col{}", table_name, i), table_name.to_string()));
            } else {
                let current_value = expr_to_column_source.get(&expr_id).unwrap();
                expr_to_column_source
                    .insert(expr_id, (current_value.0.clone(), table_name.to_string()));
            }
            processed_items += 1;
            latest_expr_id = expr_id;
        }

        if processed_items >= expr_to_column_source.len() {
            break;
        } else {
            // the offset is equal to the next expression ID after the last processed item. We need to retrieve it from the map
            if let Some((next_expr_id, _)) = expr_to_column_source
                .iter()
                .find(|(k, _)| *k > &latest_expr_id)
            {
                offset = *next_expr_id;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Failed to find next expression ID after processing input tables",
                ));
            }
        }
    }

    Ok(())
}
