//! Polars Tools - Helper library for working with Polars DataFrames
//!
//! This library provides schema validation and column helper utilities for Polars DataFrames.

pub use polars::prelude::*;
pub use polars_tools_derive::*;

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

#[cfg(test)]
#[allow(non_upper_case_globals)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_derive_macros() {
        #[derive(Debug, Serialize, Deserialize, PolarsSchema)]
        #[allow(dead_code, non_upper_case_globals)]
        struct TestStruct {
            name: String,
            age: i32,
            active: bool,
        }

        // Test column constants
        assert_eq!(TestStruct::name, "name");
        assert_eq!(TestStruct::age, "age");
        assert_eq!(TestStruct::active, "active");

        // Test column names function
        let cols = TestStruct::column_names();
        assert_eq!(cols, vec!["name", "age", "active"]);

        // Test schema validation
        let df = df![
            "name" => ["Alice", "Bob"],
            "age" => [25, 30],
            "active" => [true, false],
        ]
        .unwrap();

        assert!(TestStruct::validate(&df).is_ok());
        assert!(TestStruct::validate_strict(&df).is_ok());
    }

    #[test]
    fn test_supported_types() {
        #[derive(PolarsSchema)]
        #[allow(dead_code, non_upper_case_globals)]
        struct SupportedTypes {
            int32_col: i32,
            int64_col: i64,
            float32_col: f32,
            float64_col: f64,
            bool_col: bool,
            string_col: String,
        }

        let df = df![
            "int32_col" => [1i32, 2i32],
            "int64_col" => [1i64, 2i64],
            "float32_col" => [1.0f32, 2.0f32],
            "float64_col" => [1.0f64, 2.0f64],
            "bool_col" => [true, false],
            "string_col" => ["test1", "test2"],
        ]
        .unwrap();

        assert!(SupportedTypes::validate(&df).is_ok());
        assert!(SupportedTypes::validate_strict(&df).is_ok());

        // Test column constants work
        assert_eq!(SupportedTypes::int32_col, "int32_col");
        assert_eq!(SupportedTypes::string_col, "string_col");
    }

    #[test]
    fn test_validation_failures() {
        #[derive(PolarsSchema)]
        #[allow(dead_code, non_upper_case_globals)]
        struct TestSchema {
            name: String,
            age: i32,
        }

        // Missing column
        let df_missing = df![
            "name" => ["Alice", "Bob"],
        ]
        .unwrap();
        assert!(TestSchema::validate(&df_missing).is_err());

        // Wrong type
        let df_wrong_type = df![
            "name" => ["Alice", "Bob"],
            "age" => ["25", "30"], // String instead of i32
        ]
        .unwrap();
        assert!(TestSchema::validate(&df_wrong_type).is_err());

        // Extra column (should pass standard validation but fail strict)
        let df_extra = df![
            "name" => ["Alice", "Bob"],
            "age" => [25, 30],
            "extra" => [1, 2],
        ]
        .unwrap();
        assert!(TestSchema::validate(&df_extra).is_ok());
        assert!(TestSchema::validate_strict(&df_extra).is_err());
    }
}
