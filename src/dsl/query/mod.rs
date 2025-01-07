use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;
use std::fs;
use std::process::Command;
use crate::dsl::ir::aqua::*;
use std::io;
use std::path::PathBuf;

pub struct RustProject {
    pub project_path: PathBuf,
}

//use super::ir::{Expression, IrOperator, Literal, Operation};

pub trait QueryExt<Op: Operator>
where
    Op::Out: Clone + 'static + Into<f64> + TryFrom<f64>,
{
    fn generate_json_file<F>(self, output_path: &str,  execute_fn: F) -> io::Result<()> 
    where
        F: FnOnce();
    fn query_full<F>(self, query_str: &str, output_path: &str, execute_fn: F) -> std::io::Result<Vec<f64>>
    where F: FnOnce();
}

impl<Op> QueryExt<Op> for Stream<Op> 
where   
    Op: Operator + 'static,
    Op::Out: ExchangeData + PartialOrd + Into<f64> + Clone +  'static + TryFrom<f64>,
{
    fn query_full<F>(self, query_str: &str, output_path: &str, execute_fn: F) -> io::Result<Vec<f64>>
    where
        F: FnOnce()
    {
        // step 1: if not existing, create a Rust project
        let rust_project = RustProject::create_empty_project()?;

        // step 2: parse the query
        let query = query_to_string_aqua(query_str);

        // step 3: generate the JSON file containing the stream data
        let _ = self.generate_json_file(output_path, execute_fn);

        // step 4: generate main.rs and update it in the Rust project
        let main = create_template(&query);
        rust_project.update_main_rs(&main)?;

        // step 5: compile the binary
        let result = binary_execution(output_path, rust_project);

        result

    }

    fn generate_json_file<F>(self, output_path: &str,  execute_fn: F) -> io::Result<()> 
    where
        F: FnOnce()
    {
        let stream = self.collect_vec();

        // This calls ctx.execute_blocking(), passed as input
        execute_fn();

        // Convert the stream data to Vec<f64> directly from self
        let stream_data: Vec<f64> = stream
            .get()
            .unwrap_or_default()
            .into_iter()
            .map(|x| x.into())
            .collect(); 
    
        // Save stream data to JSON file
        let stream_json_path = std::path::Path::new(output_path)
            .parent()
            .ok_or_else(|| io::Error::new(
                io::ErrorKind::NotFound,
                "Parent directory not found"
            ))?
            .join("stream_data.json");

        fs::write(
            &stream_json_path,
            serde_json::to_string(&stream_data)?
        )?;

        Ok(())
    }

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

pub fn create_template(operator_chain: &str) -> String {
    // Create the main.rs content
    let main_rs = format!(
        r#"use renoir::{{dsl::query::QueryExt, prelude::*}};
        use serde_json;
        use std::fs;

        fn main() {{
            // Read the original stream data
            let stream_data: Vec<f64> = serde_json::from_str(
                &fs::read_to_string("stream_data.json").expect("Failed to read stream data")
            ).expect("Failed to parse stream data");

            let ctx = StreamContext::new_local();

            let output = ctx.stream_iter(stream_data.into_iter()){}.collect_vec();
            
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
        .output()?;

    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Binary execution failed"
        ));
    }

    // Parse the JSON output into Vec<f64>
    let output_str = String::from_utf8(output.stdout)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    
    let result: Vec<f64> = serde_json::from_str(&output_str)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(result)
} 

