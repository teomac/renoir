use crate::dsl::ir::ir_ast_structure::AggregateType;
use crate::dsl::ir::r_group::r_group_keys::{GroupAccumulatorInfo, GroupAccumulatorValue};
use crate::dsl::ir::{AggregateFunction, QueryObject};
use indexmap::IndexMap;

// Function to create fold operation if needed
pub fn create_fold_operation(
    acc_info: &GroupAccumulatorInfo,
    stream_name: &String,
    keys: &String,
    query_object: &mut QueryObject,
) -> String {
    let mut tuple_types = Vec::new();
    let mut tuple_inits = Vec::new();
    let mut update_code = String::new();
    let mut global_update_code = String::new();

    let single_agg = acc_info.agg_positions.len() == 1;

    let mut agg_map: IndexMap<AggregateFunction, String> = IndexMap::new();

    // First add types and initializers for aggregates
    for (value, (pos, val_type)) in &acc_info.agg_positions {
        match value {
            GroupAccumulatorValue::Aggregate(agg_type, _) => {
                match agg_type {
                    AggregateType::Max | AggregateType::Min | AggregateType::Sum => {
                        // These will be Option types
                        let actual_type = match (agg_type, val_type.as_str()) {
                            (AggregateType::Max | AggregateType::Min, "i64") => {
                                "Option<f64>".to_string()
                            }
                            _ => format!("Option<{}>", val_type),
                        };
                        tuple_types.push(actual_type);
                        tuple_inits.push("None".to_string());
                    }
                    AggregateType::Count => {
                        // Count stays as is
                        tuple_types.push(val_type.clone());
                        match val_type.as_str() {
                            "f64" => tuple_inits.push("0.0".to_string()),
                            "i64" | "usize" => tuple_inits.push("0".to_string()),
                            _ => panic!("Unsupported type for Count: {}", val_type),
                        }
                    }
                    AggregateType::Avg => {
                        // Avg is handled through Sum and Count
                        tuple_types.push(val_type.clone());
                        tuple_inits.push("0.0".to_string());
                    }
                }

                // Generate update code
                match value {
                    GroupAccumulatorValue::Aggregate(agg_type, col) => {
                        let col_type = query_object.get_type(col);
                        let col_access = {
                            let stream_name = if col.table.is_some() {
                                query_object
                                    .get_stream_from_alias(col.table.as_ref().unwrap())
                                    .unwrap()
                            } else if query_object.streams.len() == 1 {
                                query_object.streams.first().unwrap().0
                            } else {
                                panic!("Missing stream reference: {}", col.column);
                            };

                            let stream = query_object.get_stream(stream_name);
                            format!("x{}.{}", stream.get_access().get_base_path(), col.column)
                        };

                        let acc_access = if single_agg {
                            "acc".to_string()
                        } else {
                            format!("acc.{}", pos)
                        };

                        match agg_type {
                            AggregateType::Count => {
                                if col.column == "*" {
                                    update_code.push_str(&format!(
                                        "{}acc{} += 1;\n",
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                    global_update_code.push_str(&format!(
                                        "    {}{} += local_acc{};\n",
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        acc_access,
                                        if single_agg {
                                            "".to_string()
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                    agg_map.insert(
                                        AggregateFunction {
                                            column: col.clone(),
                                            function: AggregateType::Count,
                                        },
                                        format!(
                                            "acc{}",
                                            if single_agg {
                                                String::from("")
                                            } else {
                                                format!(".{}", pos)
                                            }
                                        ),
                                    );
                                } else {
                                    update_code.push_str(&format!(
                                        "if {}.is_some() {{ {}acc{} += 1; }}\n",
                                        col_access,
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                    global_update_code.push_str(&format!(
                                        "   {}{} += local_acc{};\n",
                                        if !single_agg {
                                            String::from("")
                                        } else {
                                            String::from("*")
                                        },
                                        acc_access,
                                        if single_agg {
                                            "".to_string()
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ));
                                    agg_map.insert(
                                        AggregateFunction {
                                            column: col.clone(),
                                            function: AggregateType::Count,
                                        },
                                        format!(
                                            "x.1{}",
                                            if single_agg {
                                                String::from("")
                                            } else {
                                                format!(".{}", pos)
                                            }
                                        ),
                                    );
                                }
                            }
                            AggregateType::Sum => {
                                update_code.push_str(&format!(
                                    "if let Some(val) = {} {{ {}acc{} = Some(acc{}.unwrap_or(0{}) + val); }}\n",
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if col_type == "f64" {
                                        ".0"
                                    } else {
                                        ""
                                    }
                                ));
                                global_update_code.push_str(&format!(
                                    "    {}{} = Some({}.unwrap_or(0{}) + local_acc{}.unwrap_or(0{}));\n",
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    acc_access,
                                    acc_access,
                                    if col_type == "f64" { ".0" } else { "" },
                                    if single_agg { "".to_string() } else { format!(".{}", pos) },
                                    if col_type == "f64" { ".0" } else { "" }
                                ));

                                agg_map.insert(
                                    AggregateFunction {
                                        column: col.clone(),
                                        function: AggregateType::Sum,
                                    },
                                    format!(
                                        "x.1{}",
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ),
                                );
                            }
                            AggregateType::Max => {
                                update_code.push_str(&format!(
                                    "if let Some({}val) = {} {{ {}acc{} = Some(match {}acc{} {{
                                            Some(current_max) => current_max.max({}val as f64),
                                            None => val as f64
                                        }});
                                    }}\n",
                                    if !single_agg || col_type != "i64" {
                                        String::from("")
                                    } else {
                                        String::from("mut ")
                                    },
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg || col_type != "i64" {
                                        String::from("")
                                    } else {
                                        String::from("&mut ")
                                    }
                                ));
                                global_update_code.push_str(&format!(
                                    "    if let Some(val) = local_acc{} {{ {}{} = Some({}.unwrap_or(val).max(val)); }}\n",
                                    if single_agg { "".to_string() } else { format!(".{}", pos) },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    acc_access,
                                    acc_access
                                ));

                                agg_map.insert(
                                    AggregateFunction {
                                        column: col.clone(),
                                        function: AggregateType::Max,
                                    },
                                    format!(
                                        "x.1{}",
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ),
                                );
                            }
                            AggregateType::Min => {
                                update_code.push_str(&format!(
                                    "if let Some({}val) = {} {{{}acc{} = Some(match {}acc{} {{
                                            Some(current_min) => current_min.min({}val as f64),
                                            None => val as f64
                                        }});
                                    }}\n",
                                    if !single_agg || col_type != "i64" {
                                        String::from("")
                                    } else {
                                        String::from("mut ")
                                    },
                                    col_access,
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    if single_agg {
                                        String::from("")
                                    } else {
                                        format!(".{}", pos)
                                    },
                                    if !single_agg || col_type != "i64" {
                                        String::from("")
                                    } else {
                                        String::from("&mut ")
                                    }
                                ));

                                global_update_code.push_str(&format!(
                                    "    if let Some(val) = local_acc{} {{ {}{} = Some({}.unwrap_or(val).min(val)); }}\n",
                                    if single_agg { "".to_string() } else { format!(".{}", pos) },
                                    if !single_agg {
                                        String::from("")
                                    } else {
                                        String::from("*")
                                    },
                                    acc_access,
                                    acc_access
                                ));

                                agg_map.insert(
                                    AggregateFunction {
                                        column: col.clone(),
                                        function: AggregateType::Min,
                                    },
                                    format!(
                                        "x.1{}",
                                        if single_agg {
                                            String::from("")
                                        } else {
                                            format!(".{}", pos)
                                        }
                                    ),
                                );
                            }
                            AggregateType::Avg => {} // Handled through Sum and Count
                        }
                    }
                }
            }
        }
    }

    // Generate the fold string
    let tuple_type = format!("({})", tuple_types.join(", "));
    let tuple_init = format!("({})", tuple_inits.join(", "));

    let fold_str = format!(
        ".group_by_fold(|x| ({}), {}, |acc: &mut {}, x| {{ \n{}}}, |acc: &mut {}, local_acc| {{\n{}}})\n",
        keys,
        tuple_init,
        tuple_type,
        update_code,
        tuple_type,
        global_update_code
    );

    let stream = query_object.get_mut_stream(stream_name);
    stream.update_agg_position(agg_map);

    fold_str
}
