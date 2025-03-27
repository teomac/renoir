use indexmap::IndexMap;
use crate::dsl::struct_object::support_structs::StreamInfo;
use std::{collections::HashSet, fmt::Write};


#[derive(Debug, Clone)]
pub struct Fields {

    pub imports: String,
    pub structs: IndexMap<String, IndexMap<String, String>>, //struct name, struct
    pub streams: IndexMap<String, StreamInfo>, //stream name, stream
    pub output_path: String,

    pub main: String, //final main string
}

impl Default for Fields {
     fn default() -> Self {
        Self::new()
   }
    }

impl Fields {
    pub fn new() -> Self {
        Fields {
            imports: {r#"#![allow(non_camel_case_types)]
        #![allow(unused_variables)]
        use renoir::{{config::ConfigBuilder, prelude::*}};
        use serde::{{Deserialize, Serialize}};
        use serde_json;
        use std::fs;
        use csv;
        use ordered_float::OrderedFloat;"#.to_string()},
            structs: IndexMap::new(),
            streams: IndexMap::new(),
            main: String::new(),
            output_path: String::new(),
        }
    }

    pub fn fill_main(&mut self) {
        self.main.push_str(&self.imports);
        self.main.push_str("\n\n");
        self.main.push_str(&Self::generate_struct_declarations(self.structs.clone()));
        self.main.push_str("\n\n");

        self.main.push_str(r#" fn main() {{
            let config = ConfigBuilder::new_local(1).unwrap();

            let ctx = StreamContext::new(config.clone());
            "#);

        let mut streams = self.streams.clone();
        streams.sort_unstable_keys();
        streams.reverse();

        for (i, (stream_name, stream)) in streams.iter().enumerate() {
            self.main.push_str(&format!(
                r#"let {} = {};
             "#,
                stream_name, 
                if i == self.streams.len() - 1 {
                    format!(r#"{} .write_csv(move |_| r"{}.csv".into(), true)"#, stream.op_chain.concat(),
                        self.output_path)
                }
                else {
                stream.op_chain.concat()}
            ));

            //if it is the last stream push .write_csv

            //if subquery push execute blocking
            //if subquery push code to extract result

            self.main.push_str("\n\n");
        }

        self.main.push_str("ctx.execute_blocking(); }}");
    }

    pub fn fill(&mut self, structs: IndexMap<String, IndexMap<String, String>>, streams: IndexMap<String, StreamInfo>) {
        //push every struct from query_object
        for (struct_name, struct_str) in structs.iter() {
            self.structs.insert(struct_name.clone(), struct_str.clone());
        }

        //push every final_struct from every stream
        //push every stream from query_object
        for (name, stream) in streams.iter() {
            self.streams.insert(name.clone(), stream.clone());
            self.structs.insert(stream.final_struct_name.last().unwrap().clone(), stream.final_struct.clone());
        }
    }

    pub fn generate_struct_declarations(structs: IndexMap<String, IndexMap<String,String>>) -> String {
        //Part1: generate struct definitions for input tables
    
        // Use iterators to zip through table_names, struct_names, and field_lists to maintain order
    
        //iterate and print all structs
        let result: String = structs
            .iter()
            .map(|(struct_name, fields)| {
                // Generate struct definition
                let mut struct_def = String::new();
                struct_def.push_str(
                    "#[derive(Clone, Debug, Serialize, Deserialize, PartialOrd, PartialEq, Default)]\n",
                );
                struct_def.push_str(&format!("struct {} {{\n", struct_name));
    
                // Generate field definitions directly from table to struct mapping
                let fields_str =
                    fields
                        .iter()
                        .fold(String::new(), |mut output, (field_name, field_type)| {
                            let _ = writeln!(output, "{}: Option<{}>,\n", field_name, field_type);
                            output
                        });
    
                struct_def.push_str(&fields_str);
                struct_def.push_str("}\n\n");
    
                struct_def
            })
            .collect();

        result
    }

    pub fn fill_subquery_main(&mut self, result_column_types: IndexMap<String, String>) {
        self.main.push_str(&self.imports);
        self.main.push_str("\n\n");
        self.main.push_str(&Self::generate_struct_declarations(self.structs.clone()));
        self.main.push_str("\n\n");

        self.main.push_str(r#" fn main() {
            let config = ConfigBuilder::new_local(1).unwrap();

            let ctx = StreamContext::new(config.clone());
            "#);

        let mut streams = self.streams.clone();
        streams.reverse();

        for (i, (stream_name, stream)) in streams.iter().enumerate() {
            self.main.push_str(&format!(
                r#"let {} = {}{};
             "#,
                stream_name, 
                stream.op_chain.concat(),
                if i == self.streams.len() - 1 {
                    ".collect_vec()"
                }
                else {
                    ""}
            ));
            self.main.push_str("\n\n");

            if i == streams.len() - 1 {
                self.main.push_str("ctx.execute_blocking();\n");

                self.main.push_str(
                    &format!(
                        r#"let result = {}.get();
                    if let Some(values) = result {{
                let values: Vec<_> = values
                    .iter()
                    .filter_map(|record| record{}.{}.clone())
                    .collect();
                
                if !values.is_empty() {{
                    println!("{{:?}}", values);
                    }} else {{
                    println!("");
                    }}
                    }} else {{
                println!("");
                    }}"#,
                        stream_name,
                        stream.access.base_path,
                        result_column_types.first().unwrap().0,
                    )
                    
                );
            }


        }

        self.main.push_str("}");
    }

    pub fn collect_subquery_result(
        &mut self,
        is_single_result: bool,
    ) -> (String, String) {
        let stream_name = self.streams.last().unwrap().0.clone();
        let new_result = format!("{}_result", stream_name);
        let stream = self.streams.get_mut(&stream_name).unwrap();

        let len_check = format!(r#"if {}.len() != 1 {{
            panic!("Subquery did not return a single value");
        }}"#, new_result);

        stream.op_chain.push(format!(
            r#"
                .collect_vec();
        ctx.execute_blocking();
        let result = {}.get();
        let mut {} = vec![];
        if let Some(values) = result {{
            let values: Vec<_> = values
                .iter()
                .filter_map(|record| record{}.{}.clone())
                .collect();

            {} = values;
        }}

        {}

        let ctx = StreamContext::new(config.clone());  
            "#,
            stream_name,
            new_result,
            stream.access.base_path,
            stream.final_struct.first().unwrap().0,
            new_result,
            if is_single_result {len_check} else {String::new()}
        ));
        
        (new_result, stream.final_struct.first().unwrap().1.clone())
    }

}