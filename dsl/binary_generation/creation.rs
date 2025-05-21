use std::fs;
use std::io;
use std::path::PathBuf;

pub struct RustProject {
    pub project_path: PathBuf,
}

impl RustProject {
    /// Creates a new Rust project with the specified path.
    /// If the project already exists, it does nothing.
    pub(crate) fn create_empty_project(path: &String, renoir_path: &Option<String>) -> io::Result<RustProject> {

        // Check if the provided renoir_path is valid. If not, use the default path from the parent directory.
        let renoir = if let Some(ref renoir_path_str) = renoir_path {
            renoir_path_str.clone()
        } else {
            std::env::current_dir()?
                .parent()
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, "Parent directory not found")
                })?
                .join("renoir")
                .to_string_lossy()
                .replace('\\', "/")
        };

        // Create project directory in current directory
        let project_path = PathBuf::from(path);

        if !project_path.exists() {
            fs::create_dir_all(&project_path)?;

            // Create Cargo.toml
            let cargo_toml = format!(
                r#"[package]
                name = "renoir_binary"
                version = "0.1.0"
                edition = "2024"
                
                [dependencies]
                renoir = {{ path = "{}" }}
                serde_json = "1.0.133"
                serde = "1.0.217"
                csv = "1.2.2"
                indexmap = "2.6.0"
                ordered-float = {{version = "5.0.0", features = ["serde"]}}
                "#,
                renoir
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

    /// Updates the main.rs file with the provided content.
    pub(crate) fn update_main_rs(&self, main_content: &str) -> io::Result<()> {
        fs::write(self.project_path.join("src").join("main.rs"), main_content)
    }
}
