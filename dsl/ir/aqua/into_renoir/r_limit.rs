use crate::dsl::ir::aqua::QueryObject;

pub fn process_limit(query_object: &QueryObject) -> String {
    let csv_path = query_object.output_path.replace("\\", "/");

    // Generate limit/offset handling code if needed
    if let Some(limit_clause) = &query_object.ir_ast.as_ref().unwrap().limit {
        let start_index = limit_clause.offset.unwrap_or(0);
        return format!(
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
            start_index + limit_clause.limit,
            start_index + limit_clause.limit,

        )
    } else {
        return String::new()
    };
}