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

            let output = ctx.stream_csv::<StructVar0>("{}"){}.collect_vec();
            
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