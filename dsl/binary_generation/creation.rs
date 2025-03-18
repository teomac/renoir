use std::fmt::Write;
use std::fs;
use std::io;
use std::path::PathBuf;

use crate::dsl::struct_object::object::QueryObject;

pub struct RustProject {
    pub project_path: PathBuf,
}

impl RustProject {
    pub fn create_empty_project(path: &String) -> io::Result<RustProject> {
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
        let project_path = PathBuf::from(path);

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
                csv = "1.2.2"
                ordered-float = {{version = "5.0.0", features = ["serde"]}}
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

pub fn create_template(query_object: &QueryObject, is_subquery: bool) -> String {
    let all_streams = &query_object.streams;
    //streams are returned in reverse order

    let mut table_names = Vec::new();
    for (_, stream) in all_streams.iter() {
        table_names.push(stream.source_table.clone());
    }
    table_names.reverse();

    // Generate struct definitions for input and output tables
    let struct_definitions = generate_struct_declarations(&table_names, query_object);

    let mut stream_declarations: Vec<String> = Vec::new();

    let all_stream_names = all_streams.keys().cloned().collect::<Vec<String>>();

    for (i, stream_name) in all_stream_names.iter().enumerate() {
        let mut stream;
        if i == all_stream_names.len() - 1 {
            let stream_object = all_streams.get(stream_name).unwrap();
            let stream_op_chain = stream_object.op_chain.concat();

            stream = format!(
                r#"let {} = {}{};
                 "#,
                stream_name,
                stream_op_chain,
                if !is_subquery {
                    format!(
                        r#".write_csv(move |_| r"{}.csv".into(), true)"#,
                        query_object.output_path
                    )
                } else {
                    ".collect_vec()".to_string()
                }
            );

            stream.push_str("ctx.execute_blocking();");

            //insert order by string
            stream.push_str(&query_object.order_by_string);

            //insert limit string
            stream.push_str(&query_object.limit_string);

            //insert distinct string
            stream.push_str(&query_object.distinct_string);
        } else {
            let stream_object = all_streams.get(stream_name).unwrap();
            let stream_op_chain = stream_object.op_chain.concat();
            stream = format!(
                r#"let {} = {};
             "#,
                stream_name, stream_op_chain
            );
        }

        stream_declarations.push(stream);
    }

    // Join all stream declarations with newlines
    let streams = stream_declarations.join("\n");

    // Create the main.rs content
    format!(
        r#"use renoir::prelude::*;
        use serde::{{Deserialize, Serialize}};
        use serde_json;
        use std::fs;
        use csv;
        use ordered_float::OrderedFloat;

        {}

        fn main() {{
            let ctx = StreamContext::new_local();

            {}
            {}
            
        }}"#,
        struct_definitions,
        streams,
        if is_subquery {
            format!(
                r#"let result = stream0.get();
            if result.is_some() && result.as_ref().unwrap().first().is_some() && result.as_ref().unwrap().first().as_ref().unwrap().{}.is_some() {{
                println!("{{:?}}", result.clone().unwrap().first().unwrap().{}.clone().unwrap());
            }} else {{
                println!("{{}}", "".to_string());
            }}"#,
                query_object.result_column_types.first().unwrap().0,
                query_object.result_column_types.first().unwrap().0
            )
        } else {
            "".to_string()
        }
    )
}

pub fn generate_struct_declarations(table_names: &[String], query_object: &QueryObject) -> String {
    //Part1: generate struct definitions for input tables

    let struct_names = query_object.get_all_structs();

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

            let fields_str = query_object
                .tables_info
                .get(_table_name)
                .unwrap()
                .iter()
                .fold(String::new(), |mut output, (field_name, field_type)| {
                    let _ = writeln!(output, "{}: Option<{}>,\n", field_name, field_type);
                    output
                });

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

    // Add fields from result_column_types
    for (field_name, field_type) in query_object.result_column_types.clone() {
        result.push_str(&format!("    {}: Option<{}>,\n", field_name, field_type));
    }

    result.push_str("}\n");

    result
}
