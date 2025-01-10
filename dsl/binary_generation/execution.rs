
use std::fs;
use std::process::Command;
use std::io;
use super::creation;




pub fn binary_execution(output_path: &str, rust_project: creation::RustProject) -> io::Result<String> {
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

    Ok(output_str)
} 