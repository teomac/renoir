use crate::dsl::ir::QueryObject;

pub fn process_limit(offset: Option<i64>, limit: i64, query_object: &mut QueryObject) -> String {
    let csv_path = query_object.output_path.replace("\\", "/");

    // Generate limit/offset handling code if needed
    let start_index = offset.unwrap_or(0);
    let final_string = format!(
        r#"
            // Process limit and offset after CSV is written
            let mut rdr = csv::Reader::from_path(format!("{}.csv")).unwrap();
            let mut wtr = csv::Writer::from_path(format!("{}_final.csv")).unwrap();
            
            // Copy the header
            let headers = rdr.headers().unwrap().clone();
            wtr.write_record(&headers).unwrap();

            // Process records with limit and offset
            for (i, result) in rdr.records().enumerate() {{
                if i >= {} && i < {} {{
                    if let Ok(record) = result {{
                        wtr.write_record(&record).unwrap();
                    }}
                }}
                if i >= {} {{
                    break;
                }}
            }}
            wtr.flush().unwrap();
            drop(wtr);
            drop(rdr);

            "#,
        csv_path,
        csv_path,
        start_index,
        start_index + limit,
        start_index + limit,
    );

    query_object.limit_string = final_string.clone();
    final_string
}
