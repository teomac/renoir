// In dsl/ir/ast_builder/validation.rs

use crate::dsl::ir::{ColumnRef, IrPlan, ProjectionColumn};
use std::sync::Arc;

/// Validates that the IR AST has a Project node at the top (possibly under Limit/OrderBy nodes).
/// If not, wraps the AST with a SELECT * projection.
pub fn validate_ir_ast(ast: Arc<IrPlan>) -> Arc<IrPlan> {
    match &*ast {
        // If it's already a Project node, we're good
        IrPlan::Project { .. } => ast,
        
        // If it's a Limit node, check its input recursively
        IrPlan::Limit { input, limit, offset } => {
            let validated_input = validate_ir_ast(input.clone());
            Arc::new(IrPlan::Limit {
                input: validated_input,
                limit: *limit,
                offset: *offset,
            })
        }
        
        // If it's an OrderBy node, check its input recursively
        IrPlan::OrderBy { input, items } => {
            let validated_input = validate_ir_ast(input.clone());
            Arc::new(IrPlan::OrderBy {
                input: validated_input,
                items: items.clone(),
            })
        }
        
        // For any other node type, wrap with a SELECT * projection
        _ => {
            Arc::new(IrPlan::Project {
                input: ast,
                columns: vec![ProjectionColumn::Column(
                    ColumnRef {
                        table: None,
                        column: "*".to_string(),
                    },
                    None,
                )],
                distinct: false,
            })
        }
    }
}