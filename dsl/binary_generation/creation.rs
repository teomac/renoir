use std::fs;
use std::io;
use std::path::PathBuf;


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

pub fn create_template(operator_chain: &Vec<String>, csv_path: &Vec<String>, struct_string: &Vec<String>) -> String {

    let mut paths = Vec::<String>::new();

    for path in csv_path  {
        let absolute_path = std::env::current_dir()
            .unwrap()
            .join(path)
            .to_string_lossy()
            .replace('\\', "/");

        paths.push(absolute_path);
    }

    // Join all struct definitions with newlines
    let struct_definitions = struct_string.join("\n");

      // Generate stream declarations
    let mut stream_declarations = Vec::new();
    for (i, (path, ops)) in paths.iter().zip(operator_chain.iter()).enumerate() {
        let stream = format!(
            r#"let stream{} = ctx.stream_csv::<StructVar{}>("{}"){}.collect_vec();"#,
            i, i, path, ops
        );
        stream_declarations.push(stream);
    }
    
    // Join all stream declarations with newlines
    let streams = stream_declarations.join("\n            ");

     // Generate output handling for all streams
     let mut output_handling = Vec::new();
     for i in 0..paths.len() {
         let output = format!(
             r#"if let Some(output{}) = stream{}.get() {{
                 println!("Stream {} output: {{:?}}", output{});
                 fs::write(
                     "output{}.json",
                     serde_json::to_string(&output{}).unwrap()
                 ).expect("Failed to write output{} to file");
             }}"#,
             i, i, i, i, i, i, i
         );
         output_handling.push(output);
     }
     
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