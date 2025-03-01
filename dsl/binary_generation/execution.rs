use super::creation;
use std::fs;
use std::io;
use std::process::Command;

pub fn binary_execution(
    output_path: &str,
    rust_project: creation::RustProject,
) -> io::Result<String> {
    // Ensure output directory exists
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        fs::create_dir_all(parent)?;
    }

    // Added: Format the code using cargo fmt
    let fmt_status = Command::new("cargo")
        .args(&["fmt"])
        .current_dir(&rust_project.project_path)
        .status()?;

    if !fmt_status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Failed to format the code",
        ));
    }

    // Added: Fix the code using cargo fix
    let fix_status = Command::new("cargo")
        .args(&["fix", "--bin", "query_binary", "--allow-dirty"])
        .current_dir(&rust_project.project_path)
        .status()?;

    if !fix_status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Failed to fix the code",
        ));
    }

    // Build the binary using cargo in debug mode
    let status = Command::new("cargo")
        .args(&["build"])
        .current_dir(&rust_project.project_path)
        .status()?;

    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Failed to compile the binary",
        ));
    }

    let binary_name = if cfg!(windows) {
        "query_binary.exe"
    } else {
        "query_binary"
    };

    // Execute the binary with the provided input range
    let output = Command::new(
        rust_project
            .project_path
            .join("target/debug")
            .join(binary_name),
    )
    //.current_dir(std::env::current_dir()?)
    .output()?;

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Binary execution failed: {}", error),
        ));
    }

    // Parse the JSON output into Vec<f64>
    let output_str = String::from_utf8(output.stdout)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(output_str)
}
