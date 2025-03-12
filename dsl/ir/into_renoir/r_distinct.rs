use crate::dsl::ir::QueryObject;

pub fn process_distinct(query_object: &mut QueryObject) {
    let csv_path = query_object.output_path.replace("\\", "/");

    // Generate code to remove duplicates from the CSV
    let distinct_code = format!(
        r#"
        // Process DISTINCT - remove duplicate rows from CSV
        let mut rdr = csv::Reader::from_path(format!("{}.csv")).unwrap();
        let mut wtr = csv::Writer::from_path(format!("{}_distinct.csv")).unwrap();
        
        // Copy the header
        let headers = rdr.headers().unwrap().clone();
        wtr.write_record(&headers).unwrap();
        
        // Use a HashSet to track unique rows
        let mut seen_rows = std::collections::HashSet::new();
        
        // Process records and keep only unique ones
        for result in rdr.records() {{
            if let Ok(record) = result {{
                // Convert record to a string that can be hashed
                let record_str = record.iter().collect::<Vec<_>>().join("\t");
                
                // Only write this record if we haven't seen it before
                if seen_rows.insert(record_str) {{
                    wtr.write_record(&record).unwrap();
                }}
            }}
        }}
        
        wtr.flush().unwrap();
        drop(wtr);
        drop(rdr);
        
        // Replace original file with distinct version
        std::fs::rename(format!("{}_distinct.csv"), format!("{}.csv")).unwrap();
        "#,
        csv_path, csv_path, csv_path, csv_path
    );

    query_object.distinct_string = distinct_code;
}
