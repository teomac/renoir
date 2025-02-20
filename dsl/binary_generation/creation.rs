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
                csv = "1.2.2"
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

    let csv_path = query_object.output_path.replace("\\", "/");

    // Generate limit/offset handling code if needed
    let limit_offset_code = if let Some(limit_clause) = &query_object.ir_ast.as_ref().unwrap().limit {
        let start_index = limit_clause.offset.unwrap_or(0);
        format!(
            r#"
            // Process limit and offset after CSV is written
            let mut rdr = csv::Reader::from_path(format!("{}.csv")).unwrap();
            let mut wtr = csv::Writer::from_path(format!("{}_final.csv")).unwrap();
            
            // Copy the header
            let headers = rdr.headers().unwrap().clone();
            wtr.write_record(&headers).unwrap();

            // Process records with limit and offset
            for (i, result) in rdr.records().enumerate() {{
                if i >= {} && i < {} {{
                    if let Ok(record) = result {{
                        wtr.write_record(&record).unwrap();
                    }}
                }}
                if i >= {} {{
                    break;
                }}
            }}
            wtr.flush().unwrap();
            drop(wtr);
            drop(rdr);

            "#,
            csv_path,
            csv_path,
            start_index,
            start_index + limit_clause.limit,
            start_index + limit_clause.limit,

        )
    } else {
        String::new()
    };

    // case 1: no join inside the query
    if !query_object.has_join {
        let table_name = table_names.first().unwrap();
        let stream = format!(
            r#"let stream0 = ctx.stream_csv::<{}>("{}"){}.write_csv(move |_| r"{}.csv".into(), true);
            ctx.execute_blocking();
            {}"#,
            query_object.get_struct_name(table_name).unwrap(),
            query_object.get_csv(table_name).unwrap(),
            query_object.renoir_string,
            query_object.output_path,
            limit_offset_code
        );
        stream_declarations.push(stream);
    }
    // case 2: join inside the query
    else {
        for (i, table_name) in table_names.iter().enumerate() {
            if i == 0 {
                let stream = format!(
                    r#"let stream{} = ctx.stream_csv::<{}>("{}"){}.write_csv(move |_| r"{}.csv".into(), true);
                    ctx.execute_blocking();
                    {}"#,
                    i,
                    query_object.get_struct_name(table_name).unwrap(),
                    query_object.get_csv(table_name).unwrap(),
                    query_object.renoir_string,
                    query_object.output_path,
                    limit_offset_code
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

    // Create the main.rs content
    format!(
        r#"use renoir::prelude::*;
        use serde::{{Deserialize, Serialize}};
        use serde_json;
        use std::fs;
        use csv;

        {}

        fn main() {{
            let ctx = StreamContext::new_local();

            {}
            
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

    // Add fields from result_column_types
    for (field_name, field_type) in &query_object.result_column_types {
        result.push_str(&format!("    {}: Option<{}>,\n", field_name, field_type));
    }

    result.push_str("}\n");

    result
}