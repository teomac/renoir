use super::cleaning::preprocess_rust_code;
use super::creation;
use std::fs;
use std::io;
use std::process::Command;

/// Executes the generated Rust binary and returns the output as a string.
pub(crate) fn binary_execution(
    output_path: &str,
    rust_project: creation::RustProject,
) -> io::Result<String> {
    // Ensure output directory exists
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        fs::create_dir_all(parent)?;
    }

    preprocess_rust_code(&rust_project.project_path)?;

    // Added: Format the code using cargo fmt
    let fmt_status = Command::new("cargo")
        .args(["fmt"])
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
        .args(["fix", "--bin", "renoir_binary", "--allow-dirty"])
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
        .args(["build", "--release"])
        .current_dir(&rust_project.project_path)
        .status()?;

    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Failed to compile the binary",
        ));
    }

    let binary_name = if cfg!(windows) {
        "renoir_binary.exe"
    } else {
        "renoir_binary"
    };

    // Execute the binary with the provided input range
    let output = Command::new(
        rust_project
            .project_path
            .join("target/release")
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

    let output_str = String::from_utf8(output.stdout)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(output_str)
}
