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
                indexmap = "2.6.0"
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
    let mut all_streams = query_object.streams.clone();
    //streams are returned in reverse order
    all_streams.reverse();

    // Generate struct definitions for input and output tables
    let struct_definitions = generate_struct_declarations(query_object);

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
        r#"
        #![allow(non_camel_case_types)]
        #![allow(unused_variables)]
        use renoir::{{config::ConfigBuilder, prelude::*}};
        use serde::{{Deserialize, Serialize}};
        use serde_json;
        use std::fs;
        use csv;
        use ordered_float::OrderedFloat;

        {}

        fn main() {{
            let config = ConfigBuilder::new_local(1).unwrap();

            let ctx = StreamContext::new(config.clone());

            {}
            {}
            
        }}"#,
        struct_definitions,
        streams,
        if is_subquery {
            format!(
                r#"let result = {}.get();
            if let Some(values) = result {{
        let values: Vec<_> = values
            .iter()
            .filter_map(|record| record.{}.clone())
            .collect();
        
        if !values.is_empty() {{
            println!("{{:?}}", values);
            }} else {{
            println!("");
            }}
            }} else {{
        println!("");
            }}"#,
                query_object.streams.first().unwrap().0,
                query_object.result_column_types.first().unwrap().0,
            )
        } else {
            "".to_string()
        }
    )
}

pub fn generate_struct_declarations(query_object: &QueryObject) -> String {
    //Part1: generate struct definitions for input tables

    // Use iterators to zip through table_names, struct_names, and field_lists to maintain order
    let structs = query_object.structs.clone();

    //iterate and print all structs
    let mut result: String = structs
        .iter()
        .map(|(struct_name, fields)| {
            // Generate struct definition
            let mut struct_def = String::new();
            struct_def.push_str(
                "#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n",
            );
            struct_def.push_str(&format!("struct {} {{\n", struct_name));

            // Generate field definitions directly from table to struct mapping
            let fields_str =
                fields
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

    let all_streams = query_object.streams.clone();

    for (_stream_name, stream) in all_streams.iter() {
        if stream.final_struct.is_empty() {
            continue;
        } else {
            result.push_str(
                "#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n",
            );
            result.push_str(&format!("struct {} {{\n", stream.final_struct_name.last().unwrap()));

            // Add fields from stream
            for (field_name, field_type) in stream.final_struct.clone() {
                result.push_str(&format!("    {}: Option<{}>,\n", field_name, field_type));
            }

            result.push_str("}\n");
        }
    }

    result
}
