//! Polars Tools - Helper library for working with Polars DataFrames
//!
//! This library provides schema validation and column helper utilities for Polars DataFrames.

pub use polars::prelude::*;
pub use polars_tools_derive::*;

// For internal tests to work with absolute paths
#[doc(hidden)]
pub extern crate self as polars_tools;

/// Validation error types that can occur during schema validation
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Missing required column: {column_name}")]
    MissingColumn { column_name: String },

    #[error("Column '{column_name}' has type {actual_type:?}, expected {expected_type:?}")]
    TypeMismatch {
        column_name: String,
        actual_type: String,
        expected_type: String,
    },

    #[error("Column count mismatch. Expected: {expected:?}, Found: {actual:?}")]
    ColumnCountMismatch {
        expected: std::collections::HashSet<String>,
        actual: std::collections::HashSet<String>,
    },

    #[error("Unexpected column: {column_name}")]
    UnexpectedColumn { column_name: String },

    #[error("Invalid enum value '{value}' for field '{field}'. Valid values are: {valid_values:?}")]
    InvalidEnumValue {
        field: String,
        value: String,
        valid_values: Vec<String>,
    },
}

pub type Result<T> = std::result::Result<T, ValidationError>;

/// Trait for structs that can provide column names for Polars DataFrames
pub trait PolarsColumns {
    /// Get all column names as a vector
    fn column_names() -> Vec<&'static str>;

    /// Get column name at specific index
    fn column_name_at(index: usize) -> Option<&'static str>;

    /// Get column expression for a field name
    fn col_expr(field_name: &str) -> Option<Expr>;
}

/// Extension trait for additional column utilities
pub trait PolarsColumnsExt {
    /// Get all column names (alias for column_names)
    fn columns() -> Vec<&'static str>;
}

/// Trait for enums that can be validated in Polars DataFrames
pub trait ValidatableEnum {
    /// Get all valid string representations of this enum
    fn valid_values() -> Vec<&'static str>;
    
    /// Check if a string value is valid for this enum
    fn is_valid(value: &str) -> bool {
        Self::valid_values().contains(&value)
    }
    
    /// Convert string to enum if valid, otherwise return error
    fn from_str(value: &str) -> Result<Self>
    where 
        Self: Sized;
    
    /// Convert enum to string representation
    fn to_str(&self) -> &'static str;
}
