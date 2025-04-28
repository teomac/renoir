use regex::Regex;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::Path;

/// Preprocesses the Rust code to remove unused struct definitions
pub(crate) fn preprocess_rust_code(project_path: &Path) -> io::Result<()> {
    let main_rs_path = project_path.join("src").join("main.rs");
    let content = fs::read_to_string(&main_rs_path)?;

    // First pass: identify which structs are actually used in the main function
    let used_structs = identify_used_structs(&content);

    // Second pass: filter out unused struct definitions
    let filtered_content = remove_unused_struct_definitions(&content, &used_structs);

    // Write the cleaned content back
    fs::write(main_rs_path, filtered_content)?;

    Ok(())
}

/// Identifies which struct types are used in the code
fn identify_used_structs(content: &str) -> HashSet<String> {
    let mut used_structs = HashSet::new();

    // First, find the main function content
    if let Some(main_content) = content.split("fn main()").nth(1) {
        // Find generic type parameters, e.g., stream_csv::<Struct_table1>
        let generic_re = Regex::new(r"<([A-Za-z0-9_]+)>").unwrap();
        for cap in generic_re.captures_iter(main_content) {
            if let Some(m) = cap.get(1) {
                used_structs.insert(m.as_str().to_string());
            }
        }

        // Find variable type annotations, e.g., let x: Struct_table1
        let type_annotation_re = Regex::new(r": ([A-Za-z0-9_]+)").unwrap();
        for cap in type_annotation_re.captures_iter(main_content) {
            if let Some(m) = cap.get(1) {
                used_structs.insert(m.as_str().to_string());
            }
        }

        // Find struct instantiations, e.g., Struct_stream0 { ... }
        let struct_instantiation_re = Regex::new(r"([A-Za-z0-9_]+) \{").unwrap();
        for cap in struct_instantiation_re.captures_iter(main_content) {
            if let Some(m) = cap.get(1) {
                used_structs.insert(m.as_str().to_string());
            }
        }
    }

    // Also check for any structs used in type definitions of used structs
    // This is a recursive relationship, so we'll iterate until no new structs are found
    let mut prev_size = 0;
    let type_re = Regex::new(r": Option<([A-Za-z0-9_]+)>").unwrap();
    while prev_size != used_structs.len() {
        prev_size = used_structs.len();

        // Find struct definitions
        for struct_name in used_structs.clone() {
            // Find the struct definition
            let struct_def_pattern = format!("struct {} {{", struct_name);
            if let Some(struct_def_pos) = content.find(&struct_def_pattern) {
                // Find the end of the struct definition
                if let Some(struct_def_end) = content[struct_def_pos..].find("\n}") {
                    let struct_def =
                        &content[struct_def_pos..(struct_def_pos + struct_def_end + 2)];
                    // Look for other struct types in this definition
                    for cap in type_re.captures_iter(struct_def) {
                        if let Some(m) = cap.get(1) {
                            let type_name = m.as_str();
                            // Only add if it looks like a struct name (starts with uppercase)
                            if !type_name.is_empty()
                                && type_name.chars().next().unwrap().is_uppercase()
                            {
                                used_structs.insert(type_name.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    used_structs
}

/// Removes unused struct definitions from the content
fn remove_unused_struct_definitions(content: &str, used_structs: &HashSet<String>) -> String {
    // Find all struct definitions
    let struct_def_regex =
        Regex::new(r"(?ms)^#\[derive.*?\nstruct ([A-Za-z0-9_]+).*?\n\}").unwrap();

    let result = struct_def_regex.replace_all(content, |caps: &regex::Captures| {
        let struct_name = &caps[1];
        if used_structs.contains(struct_name) {
            // Keep the struct definition
            caps[0].to_string()
        } else {
            // Replace with empty string to remove it
            String::new()
        }
    });

    // Clean up any double newlines created by removal
    let newline_cleanup = Regex::new(r"\n{3,}").unwrap();
    newline_cleanup.replace_all(&result, "\n\n").to_string()
}
