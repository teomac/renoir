use crate::dsl::ir::IrAST;
use crate::dsl::parsers::sql::SqlAST;
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;
use std::fs;
use std::process::Command;

use super::ir::{Expression, IrOperator, Literal, Operation};

pub trait QueryExt<Op: Operator> {
    fn query(self, query: &str) -> Stream<impl Operator<Out = Op::Out>>;
    fn query_to_binary<F>(self, query_str: &str, output_path: &str, execute_fn: F) -> std::io::Result<Vec<i32>>
    where F: FnOnce();
}

impl<Op> QueryExt<Op> for Stream<Op> 
where   
    Op: Operator + 'static,
    Op::Out: ExchangeData + PartialOrd + Into<i64> + Ord + 'static,
{
    fn query(self, query_str: &str) -> Stream<impl Operator<Out = Op::Out>> {
        let sql_ast = SqlAST::parse(query_str).expect("Failed to parse query");
        let ir = IrAST::parse(&sql_ast);

        match ir.operation {
            Operation::Select(select) => {
                let filtered = match select.filter {
                    Some(Expression::BinaryOp(op)) => {
                        let value = op.right.as_integer();
                        let filter = move |x: &Op::Out| match op.operator {
                            IrOperator::GreaterThan => x.clone().into() > value,
                            IrOperator::LessThan => x.clone().into() < value,
                            IrOperator::Equals => x.clone().into() == value,
                        };
                        self .filter(filter)
                    },
                    _ => unreachable!()
                };

                let is_aggregate = match &select.projections[0].expression {
                    Expression::AggregateOp(_) => true,
                    Expression::Column(_) => false,
                    _ => unreachable!()
                };

                filtered
                    .fold(
                        (Vec::new(), is_aggregate),
                        |acc: &mut (Vec<Op::Out>, bool), x| {
                            if acc.1 {
                                if acc.0.is_empty() || &x > acc.0.last().unwrap() {
                                    acc.0.clear();
                                    acc.0.push(x);
                                }
                            } else {
                                acc.0.push(x);
                            }
                        }
                    )
                    .flat_map(|(vec, _)| vec.into_iter())
            }
        }
    }


    

    fn query_to_binary<F>(self, query_str: &str, output_path: &str, execute_fn: F) -> std::io::Result<Vec<i32>>
    where
        F: FnOnce()
    {

        let stream = self.collect_vec();

        // This calls ctx.execute_blocking(), passed as input
        execute_fn();

        // Convert the stream data to Vec<i64> directly from self
        let stream_data: Vec<i64> = stream
        .get()
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.into())
        .collect(); 
    
        // Save stream data to JSON file
        let stream_json_path = std::path::Path::new(output_path)
        .parent()
        .ok_or_else(|| std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Parent directory not found"
        ))?
        .join("stream_data.json");

        fs::write(
            &stream_json_path,
            serde_json::to_string(&stream_data)?
        )?;

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
    
        // Create temporary directory for our project
        let tmp_dir = tempfile::tempdir()?;
        let project_path = tmp_dir.path();
    
        // Create Cargo.toml
        let cargo_toml = format!(
            r#"[package]
            name = "query_binary"
            version = "0.1.0"
            edition = "2021"
            
            [dependencies]
            renoir = {{ path = "{}" }}
            serde_json = "1.0.133"
            "#,
            renoir_path
        );
    
        fs::write(project_path.join("Cargo.toml"), cargo_toml)?;
    
        // Create src directory
        fs::create_dir(project_path.join("src"))?;
    
        // Generate the operator chain using the existing logic
        let operator_chain = query_to_string(query_str);
    
        // Create the main.rs file with this template
        let main_rs = format!(
            r#"use renoir::{{dsl::query::QueryExt, prelude::*}};
            use serde_json;
            use std::fs;

            fn main() {{

                // Read the original stream data
                let stream_data: Vec<i64> = serde_json::from_str(
                    &fs::read_to_string("stream_data.json").expect("Failed to read stream data")
                ).expect("Failed to parse stream data");

                let ctx = StreamContext::new_local();

                let output = ctx.stream_iter(stream_data.into_iter()){}.collect_vec();
                
                ctx.execute_blocking();

                if let Some(output) = output.get() {{
                    // Serialize to JSON and print to stdout
                    println!("{{}}", serde_json::to_string(&output).unwrap());
                }}
            }}"#,
            operator_chain
        );
    
        fs::write(project_path.join("src").join("main.rs"), main_rs)?;
    
        // Ensure output directory exists
        if let Some(parent) = std::path::Path::new(output_path).parent() {
            fs::create_dir_all(parent)?;
        }
    
        // Build the binary using cargo in debug mode
        let status = Command::new("cargo")
            .args(&["build"])
            .current_dir(&project_path)
            .status()?;
    
        if !status.success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to compile the binary"
            ));
        }

        let binary_name = if cfg!(windows) {
            "query_binary.exe"
        } else {
            "query_binary"
        };
    
        // Copy the binary from the correct debug directory
        fs::copy(
            project_path.join("target/debug").join(binary_name), // Add .exe for Windows
            format!("{}.exe", output_path)
        )?;

        // Execute the binary with the provided input range
        let output = Command::new(output_path)
            .output()?;

        if !output.status.success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Binary execution failed"
            ));
        }

        // Parse the JSON output into Vec<i32>
        let output_str = String::from_utf8(output.stdout)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        let result: Vec<i32> = serde_json::from_str(&output_str)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(result)
    }
}

fn query_to_string(query_str: &str) -> String {
    let sql_ast = SqlAST::parse(query_str).expect("Failed to parse query");
    let ir = IrAST::parse(&sql_ast);

    match ir.operation {
        Operation::Select(select) => {
            let mut final_string = String::new();
            
            // Handle filter
            if let Some(Expression::BinaryOp(op)) = select.filter {
                let value = op.right.as_integer();
                let operator = match op.operator {
                    IrOperator::GreaterThan => ">",
                    IrOperator::LessThan => "<",
                    IrOperator::Equals => "==",
                };
                final_string.push_str(&format!(".filter(|x| x {} &{})", operator, value));
            }

            let is_aggregate = match &select.projections[0].expression {
                Expression::AggregateOp(_) => true,
                Expression::Column(_) => false,
                _ => unreachable!()
            };

            if is_aggregate {
                final_string.push_str(".fold(
                    Vec::new(),
                    |acc: &mut Vec<i32>, x| {
                        if acc.is_empty() || &x > acc.last().unwrap() {
                            acc.clear();
                            acc.push(x);
                        }
                        
                    }
                )");
                final_string.push_str(".flat_map(|vec| vec.into_iter())");
            }
            
            final_string
        }
    }
}

trait AsInteger {
    fn as_integer(&self) -> i64;
}

impl AsInteger for Expression {
    fn as_integer(&self) -> i64 {
        match self {
            Expression::Literal(Literal::Integer(n)) => *n,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamContext;

    #[test]
    fn test_query_filter() {
        let ctx = StreamContext::new_local();
        let input = 0..10;
        let result = ctx
            .stream_iter(input)
            .query("SELECT a FROM input WHERE a > 5")
            .collect_vec();
            
        ctx.execute_blocking();

        let result = result.get().unwrap();
        assert_eq!(result, vec![6, 7, 8, 9]);
    }

    #[test]
    fn test_query_max() {
        let ctx = StreamContext::new_local();
        let input = 0..10;
        let result = ctx
            .stream_iter(input)
            .query("SELECT MAX(a) FROM input WHERE a > 5")
            .collect_vec();
            
        ctx.execute_blocking();

        let result = result.get().unwrap();
        assert_eq!(result, vec![9]);
    }
}