#![allow(non_upper_case_globals)]
use polars_tools::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct BasicSchema {
    id: i32,
    name: String,
    active: bool,
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct CompleteSchema {
    // Signed integers
    int8_field: i8,
    int16_field: i16,
    int32_field: i32,
    int64_field: i64,
    // Unsigned integers
    uint8_field: u8,
    uint16_field: u16,
    uint32_field: u32,
    uint64_field: u64,
    // Floats
    float32_field: f32,
    float64_field: f64,
    // Other types
    bool_field: bool,
    string_field: String,
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct OptionalSchema {
    required_field: String,
    optional_int: Option<i32>,
    optional_string: Option<String>,
}

#[test]
fn test_basic_validation_success() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "active" => [true, false, true],
    ]
    .unwrap();

    assert!(BasicSchema::validate(&df).is_ok());
}

#[test]
fn test_basic_validation_missing_column() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        // missing "active" column
    ]
    .unwrap();

    let result = BasicSchema::validate(&df);
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Missing required column: active"));
}

#[test]
fn test_basic_validation_wrong_type() {
    let df = df![
        "id" => ["1", "2", "3"], // Should be i32, not String
        "name" => ["Alice", "Bob", "Charlie"],
        "active" => [true, false, true],
    ]
    .unwrap();

    let result = BasicSchema::validate(&df);
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Column 'id' has type"));
}

#[test]
fn test_strict_validation_success() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "active" => [true, false, true],
    ]
    .unwrap();

    assert!(BasicSchema::validate_strict(&df).is_ok());
}

#[test]
fn test_strict_validation_extra_column() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "active" => [true, false, true],
        "extra" => [10, 20, 30], // Extra column should fail strict validation
    ]
    .unwrap();

    // Standard validation should pass
    assert!(BasicSchema::validate(&df).is_ok());

    // Strict validation should fail
    let result = BasicSchema::validate_strict(&df);
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Column count mismatch"));
}

#[test]
fn test_supported_numeric_types() {
    // Test with types that work well with polars df! macro
    let df = df![
        "int32_field" => [1i32, 2i32],
        "int64_field" => [1i64, 2i64],
        "float32_field" => [1.0f32, 2.0f32],
        "float64_field" => [1.0f64, 2.0f64],
        "bool_field" => [true, false],
        "string_field" => ["test1", "test2"],
    ]
    .unwrap();

    #[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct SupportedNumericSchema {
        int32_field: i32,
        int64_field: i64,
        float32_field: f32,
        float64_field: f64,
        bool_field: bool,
        string_field: String,
    }

    assert!(SupportedNumericSchema::validate(&df).is_ok());
    assert!(SupportedNumericSchema::validate_strict(&df).is_ok());
}

#[test]
fn test_empty_dataframe() {
    let df = df![
        "id" => Vec::<i32>::new(),
        "name" => Vec::<String>::new(),
        "active" => Vec::<bool>::new(),
    ]
    .unwrap();

    assert!(BasicSchema::validate(&df).is_ok());
    assert!(BasicSchema::validate_strict(&df).is_ok());
}

#[test]
fn test_single_row_dataframe() {
    let df = df![
        "id" => [42],
        "name" => ["Single"],
        "active" => [true],
    ]
    .unwrap();

    assert!(BasicSchema::validate(&df).is_ok());
    assert!(BasicSchema::validate_strict(&df).is_ok());
}

#[test]
fn test_large_dataframe() {
    let size = 10000;
    let ids: Vec<i32> = (0..size).collect();
    let names: Vec<String> = (0..size).map(|i| format!("name_{}", i)).collect();
    let actives: Vec<bool> = (0..size).map(|i| i % 2 == 0).collect();

    let df = df![
        "id" => ids,
        "name" => names,
        "active" => actives,
    ]
    .unwrap();

    assert!(BasicSchema::validate(&df).is_ok());
    assert!(BasicSchema::validate_strict(&df).is_ok());
}

#[test]
fn test_case_sensitive_columns() {
    let df = df![
        "ID" => [1, 2, 3], // Wrong case
        "name" => ["Alice", "Bob", "Charlie"],
        "active" => [true, false, true],
    ]
    .unwrap();

    let result = BasicSchema::validate(&df);
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Missing required column: id"));
}

#[test]
fn test_column_order_independence() {
    let df = df![
        "active" => [true, false, true], // Different order
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
    ]
    .unwrap();

    assert!(BasicSchema::validate(&df).is_ok());
    assert!(BasicSchema::validate_strict(&df).is_ok());
}
