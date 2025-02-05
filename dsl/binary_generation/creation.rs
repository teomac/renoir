use std::fs;
use std::io;
use std::path::PathBuf;

use crate::dsl::struct_object::object::QueryObject;

pub struct RustProject {
    pub project_path: PathBuf,
}

impl RustProject {
    pub fn create_empty_project() -> io::Result<RustProject> {
        // Get path to renoir and convert to string with forward slashes
        let renoir_path = std::env::current_dir()?
            .parent()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "Parent directory not found")
            })?
            .join("renoir")
            .to_string_lossy()
            .replace('\\', "/");

        // Create project directory in current directory
        let project_path = std::env::current_dir()?.join("query_project");

        if !project_path.exists() {
            fs::create_dir_all(&project_path)?;

            // Create Cargo.toml
            let cargo_toml = format!(
                r#"[package]
                name = "query_binary"
                version = "0.1.0"
                edition = "2021"
                
                [dependencies]
                renoir = {{ path = "{}" }}
                serde_json = "1.0.133"
                serde = "1.0.217"
                "#,
                renoir_path
            );

            fs::write(project_path.join("Cargo.toml"), cargo_toml)?;

            // Create src directory
            fs::create_dir_all(project_path.join("src"))?;

            // Create an empty main.rs file
            let empty_main = r#"
                fn main() {
                    // Empty main function
                }
            "#;

            fs::write(project_path.join("src").join("main.rs"), empty_main)?;
        }

        Ok(RustProject { project_path })
    }

    pub fn update_main_rs(&self, main_content: &str) -> io::Result<()> {
        fs::write(self.project_path.join("src").join("main.rs"), main_content)
    }
}

fn generate_all_columns_output_struct(query_object: &QueryObject) -> String {
    let mut result = String::new();

    // If it's a simple query without join
    if !query_object.has_join {
        let table_name = query_object.table_names_list.first().unwrap();
        if let Some(struct_map) = query_object.table_to_struct.get(table_name) {
            for (field_name, field_type) in struct_map {
                result.push_str(&format!("    {}: Option<{}>,\n", field_name, field_type));
            }
        }
    } else {
        // For queries with joins
        for table_name in &query_object.table_names_list {
            let suffix = query_object
                .get_alias(table_name)
                .unwrap_or(table_name)
                .to_string();

            if let Some(struct_map) = query_object.table_to_struct.get(table_name) {
                for (field_name, field_type) in struct_map {
                    let field_name_with_suffix = format!("{}_{}", field_name, suffix);
                    result.push_str(&format!(
                        "    {}: Option<{}>,\n",
                        field_name_with_suffix, field_type
                    ));
                }
            }
        }
    }

    result
}

pub fn create_template(query_object: &QueryObject) -> String {
    let table_names = query_object.table_names_list.clone();
    let struct_names = query_object
        .table_to_struct_name
        .values()
        .cloned()
        .collect::<Vec<String>>();

    // Generate struct definitions for input and output tables
    let struct_definitions =
        generate_struct_declarations(&table_names, &struct_names, &query_object);

    let mut stream_declarations = Vec::new();

    // case 1: no join inside the query
    if !query_object.has_join {
        let table_name = table_names.first().unwrap();
        let stream = format!(
            r#"let stream0 = ctx.stream_csv::<{}>("{}"){}.write_csv(move |_| r"{}.csv".clone().into(), true);"#,
            query_object.get_struct_name(table_name).unwrap(),
            query_object.get_csv(table_name).unwrap(),
            query_object.renoir_string,
            query_object.output_path
        );
        stream_declarations.push(stream);
    }
    // case 2: join inside the query
    else {
        // println!("{:?}", table_names);
        for (i, table_name) in table_names.iter().enumerate() {
            if i == 0 {
                let stream = format!(
                    r#"let stream{} = ctx.stream_csv::<{}>("{}"){}.write_csv(move |_| r"{}.csv".clone().into(), true);"#,
                    i,
                    query_object.get_struct_name(table_name).unwrap(),
                    query_object.get_csv(table_name).unwrap(),
                    query_object.renoir_string,
                    query_object.output_path
                );
                stream_declarations.push(stream);
            } else {
                let stream = format!(
                    r#"let stream{} = ctx.stream_csv::<{}>("{}");"#,
                    i,
                    query_object.get_struct_name(table_name).unwrap(),
                    query_object.get_csv(table_name).unwrap()
                );
                stream_declarations.push(stream);
            }
        }
        stream_declarations.reverse();
    }

    // Join all stream declarations with newlines
    let streams = stream_declarations.join("\n");

    // Generate output handling for all streams

    // Create the main.rs content
    format!(
        r#"use renoir::prelude::*;
        use serde::{{Deserialize, Serialize}};
        use serde_json;
        use std::fs;

        {}

        fn main() {{
            let ctx = StreamContext::new_local();

            {}
            
            ctx.execute_blocking();
        }}"#,
        struct_definitions, streams,
    )
}

pub fn generate_struct_declarations(
    table_names: &Vec<String>,
    struct_names: &Vec<String>,
    query_object: &QueryObject,
) -> String {
    //Part1: generate struct definitions for input tables

    // Use iterators to zip through table_names, struct_names, and field_lists to maintain order
    let mut result: String = table_names
        .iter()
        .enumerate()
        .map(|(i, _table_name)| {
            // Generate struct definition
            let mut struct_def = String::new();
            struct_def.push_str(
                "#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n",
            );
            struct_def.push_str(&format!("struct {} {{\n", struct_names[i]));

            // Generate field definitions directly from table to struct mapping

            let fields_str: String = query_object
                .table_to_struct
                .get(_table_name)
                .unwrap()
                .iter()
                .map(|(field_name, field_type)| {
                    format!("    {}: Option<{}>,\n", field_name, field_type)
                })
                .collect();

            struct_def.push_str(&fields_str);
            struct_def.push_str("}\n\n");

            struct_def
        })
        .collect();

    //Part2: generate struct definitions for output tables
    result.push_str(
        "#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n",
    );
    result.push_str("struct OutputStruct {\n");

    // Check if we have SELECT *
    let has_select_star = query_object
        .result_column_to_input
        .iter()
        .any(|(col, (_, input_col, _))| col == "*" && input_col == "*");

    // Add fields from result_column_to_input (if there is a join, add the suffix)
    if has_select_star {
        result.push_str(&generate_all_columns_output_struct(query_object));
    } else {
        // Add fields from result_column_to_input (if there is a join, add the suffix)
        for (result_col, (result_type, _, table_name)) in &query_object.result_column_to_input {
            let field_name = if query_object.has_join {
                let suffix = query_object
                    .get_alias(&table_name)
                    .unwrap_or(&table_name)
                    .to_string();
                format!("{}_{}", result_col, suffix)
            } else {
                result_col.to_string()
            };

            result.push_str(&format!("    {}: Option<{}>,\n", field_name, result_type));
        }
    }

    result.push_str("}\n");

    result
}
