use csv_parsers::*;

use crate::dsl::parsers::csv_utils::*;
use std::fs;
use std::process::Command;
use crate::dsl::ir::aqua::*;
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

pub fn create_template(operator_chain: &str, csv_path: &str, struct_string: &str) -> String {

    let absolute_path = std::env::current_dir()
            .unwrap()
            .join(csv_path)
            .to_string_lossy()
            .replace('\\', "/");

    // Create the main.rs content
    let main_rs = format!(
        r#"use renoir::prelude::*;
        use serde::{{Deserialize, Serialize}};
        use serde_json;
        use std::fs;

        {}

        fn main() {{
            let ctx = StreamContext::new_local();

            let output = ctx.stream_csv::<Struct_var_0>("{}"){}.collect_vec();
            
            ctx.execute_blocking();

            if let Some(output) = output.get() {{
                // Print values directly to stdout
                println!("{{:?}}", output);
                
                // Save to output.json file
                fs::write(
                    "output.json",
                    serde_json::to_string(&output).unwrap()
                ).expect("Failed to write output to file");
            }}
        }}"#,
        struct_string,
        absolute_path,
        operator_chain
    );

    main_rs

}

pub fn binary_execution(output_path: &str, rust_project: RustProject) -> io::Result<Vec<f64>> {
    // Ensure output directory exists
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        fs::create_dir_all(parent)?;
    }

    // Build the binary using cargo in debug mode
    let status = Command::new("cargo")
        .args(&["build"])
        .current_dir(&rust_project.project_path)
        .status()?;

    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Failed to compile the binary"
        ));
    }

    let binary_name = if cfg!(windows) {
        "query_binary.exe"
    } else {
        "query_binary"
    };

    // Execute the binary with the provided input range
    let output = Command::new(
        rust_project.project_path.join("target/debug").join(binary_name),
    )
        //.current_dir(std::env::current_dir()?)
        .output()?;

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Binary execution failed: {}", error)
        ));
    }

    // Parse the JSON output into Vec<f64>
    let output_str = String::from_utf8(output.stdout)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    let result: Vec<f64> = serde_json::from_str(&output_str)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(result)
} 

pub fn query_csv(query_str: &str, output_path: &str, csv_path: &str, user_defined_types: &str) -> io::Result<Vec<f64>>
{
    // step 1: if not existing, create a Rust project
    let rust_project = RustProject::create_empty_project()?;

    // step 2: open csv input, read column names and data types
    let user_defined_types = parse_type_string(user_defined_types)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    let columns = get_csv_columns(csv_path);
    let hash_map = combine_arrays(&columns, &user_defined_types);

    // step 3: create the struct
    let struct_string = create_struct(&user_defined_types);

    // step 4: parse the query
    let query = query_to_string_aqua(query_str, hash_map);

    // step 5: generate main.rs and update it in the Rust project
    let main = create_template(&query, csv_path, &struct_string);
    rust_project.update_main_rs(&main)?;

    // step 6: compile the binary
    let result = binary_execution(output_path, rust_project);

    result

}