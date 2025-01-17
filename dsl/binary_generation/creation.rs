use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;

use crate::dsl::struct_object::object::QueryObject;
use crate::stream;


pub struct RustProject {
    pub project_path: PathBuf,
}

impl RustProject {
    pub fn create_empty_project() -> io::Result<RustProject> {
        // Get path to renoir and convert to string with forward slashes
        let renoir_path = std::env::current_dir()?
            .parent()
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Parent directory not found"
            ))?
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
        fs::write(
            self.project_path.join("src").join("main.rs"),
            main_content
        )
    }
}

pub fn create_template(query_object: &QueryObject) -> String {

    let paths = query_object.table_to_csv.values();
    let table_names = query_object.get_all_table_names();
    let struct_names = query_object.get_all_structs().join("\n");

    let struct_definitions = generate_struct_declarations(&query_object.table_to_struct, &query_object.table_to_struct_name);


    let mut stream_declarations = Vec::new();

    // case 1: no join inside the query
    if !query_object.has_join {
        let table_name = table_names.first().unwrap();
        let stream = format!(
            r#"let stream0 = ctx.stream_csv::<{}>("{}"){}.collect_vec();"#,
            query_object.get_struct_name(table_name).unwrap(), query_object.get_csv(table_name).unwrap(), query_object.renoir_string
        );
        stream_declarations.push(stream);
    }

    // case 2: join inside the query
    else {
        for (i, table_name) in table_names.iter().enumerate() {
            if i == 0 {
                let stream = format!(
                    r#"let stream{} = ctx.stream_csv::<{}>("{}"){}.collect_vec();"#,
                    i, query_object.get_struct_name(table_name).unwrap(), query_object.get_csv(table_name).unwrap(), query_object.renoir_string
                );
                stream_declarations.push(stream);
            }
            else {
                let stream = format!(
                    r#"let stream{} = ctx.stream_csv::<{}>("{}");"#,
                    i, query_object.get_struct_name(table_name).unwrap(), query_object.get_csv(table_name).unwrap()
                );
                stream_declarations.push(stream);
            }
        }
        stream_declarations.reverse();
    }

    // Join all stream declarations with newlines
    let streams = stream_declarations.join("\n");

     // Generate output handling for all streams
     let mut output_handling = Vec::new();
         let output = format!(
             r#"if let Some(output0) = stream0.get() {{
                 println!("Stream output: {{:?}}", output0);
                 fs::write(
                     "output0.json",
                     serde_json::to_string(&output0).unwrap()
                 ).expect("Failed to write output0 to file");
             }}"#,
         );
         output_handling.push(output);
     
     // Join all output handling with newlines
     let outputs = output_handling.join("\n            ");
    
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

            {}
        }}"#,
        struct_definitions,
        streams,
        outputs
    )

}

pub fn generate_struct_declarations(
    table_to_struct: &HashMap<String, HashMap<String, String>>,
    table_to_struct_name: &HashMap<String, String>
) -> String {
    let mut result = String::new();

    for (table, fields) in table_to_struct {
        if let Some(struct_name) = table_to_struct_name.get(table) {
            let field_names: Vec<_> = fields.keys().collect();
            let field_types: Vec<_> = fields.values().collect();

            result.push_str(&format!("#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n"));
            result.push_str(&format!("struct {} {{\n", struct_name));

            for (name, type_str) in field_names.iter().zip(field_types.iter()) {
                let rust_type = if type_str.contains("int") {
                    "i64"
                } else if type_str.contains("float") {
                    "f64"
                } else if type_str.contains("bool") {
                    "bool"
                } else {
                    "String"
                };
                result.push_str(&format!("    {}: Option<{}>,\n", name, rust_type));
            }

            result.push_str("}\n\n");
        }
    }

    result
}