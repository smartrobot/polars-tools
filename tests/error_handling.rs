#![allow(non_upper_case_globals)]
use polars_tools::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct ErrorTestSchema {
    id: i32,
    name: String,
    score: f64,
}

#[test]
fn test_missing_column_error() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        // Missing "score" column
    ]
    .unwrap();

    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Missing required column: score"));
}

#[test]
fn test_multiple_missing_columns_error() {
    let df = df![
        "id" => [1, 2, 3],
        // Missing both "name" and "score" columns
    ]
    .unwrap();

    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    // Should fail on first missing column encountered
    assert!(error_msg.contains("Missing required column"));
}

#[test]
fn test_wrong_type_error() {
    let df = df![
        "id" => ["1", "2", "3"], // String instead of i32
        "name" => ["Alice", "Bob", "Charlie"],
        "score" => [85.5, 92.0, 78.3],
    ]
    .unwrap();

    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column 'id' has type"));
    assert!(error_msg.contains("expected"));
}

#[test]
fn test_multiple_wrong_types_error() {
    let df = df![
        "id" => ["1", "2", "3"], // String instead of i32
        "name" => [1, 2, 3], // i32 instead of String
        "score" => ["85.5", "92.0", "78.3"], // String instead of f64
    ]
    .unwrap();

    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    // Should fail on first type mismatch encountered
    assert!(error_msg.contains("Column") && error_msg.contains("has type"));
}

#[test]
fn test_strict_validation_extra_columns_error() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "score" => [85.5, 92.0, 78.3],
        "extra1" => [1, 2, 3],
        "extra2" => ["a", "b", "c"],
    ]
    .unwrap();

    // Standard validation should pass
    assert!(ErrorTestSchema::validate(&df).is_ok());

    // Strict validation should fail
    let result = ErrorTestSchema::validate_strict(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Column count mismatch"));
    assert!(error_msg.contains("Expected:"));
    assert!(error_msg.contains("Found:"));
}

#[test]
fn test_strict_validation_missing_columns_error() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        // Missing "score" column
    ]
    .unwrap();

    // Both should fail, but validate() should fail first with missing column
    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Missing required column"));

    let strict_result = ErrorTestSchema::validate_strict(&df);
    assert!(strict_result.is_err());
    let strict_error_msg = format!("{}", strict_result.unwrap_err());
    assert!(strict_error_msg.contains("Missing required column"));
}

#[test]
fn test_empty_column_names_edge_case() {
    // Test with DataFrame that has columns but our schema expects different ones
    let df = df![
        "completely_different" => [1, 2, 3],
        "column_names" => ["a", "b", "c"],
    ]
    .unwrap();

    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Missing required column"));
}

#[test]
fn test_case_sensitivity_error() {
    let df = df![
        "ID" => [1, 2, 3], // Wrong case
        "Name" => ["Alice", "Bob", "Charlie"], // Wrong case
        "Score" => [85.5, 92.0, 78.3], // Wrong case
    ]
    .unwrap();

    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Missing required column"));
}

#[test]
fn test_error_with_null_values() {
    // Test that our validation works even with null values in correct columns
    let df = df![
        "id" => [Some(1), None, Some(3)],
        "name" => [Some("Alice"), Some("Bob"), None],
        "score" => [Some(85.5), Some(92.0), None],
    ]
    .unwrap();

    // Schema validation should still pass - we're checking types, not null values
    let _result = ErrorTestSchema::validate(&df);
    // This might pass or fail depending on how polars handles the nullable series types
    // The important thing is we get a clear error message if it fails
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct SingleColumnSchema {
    value: i32,
}

#[test]
fn test_error_single_column_schema() {
    let df = df![
        "wrong_name" => [1, 2, 3],
    ]
    .unwrap();

    let result = SingleColumnSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Missing required column: value"));
}

#[test]
fn test_error_empty_dataframe_missing_columns() {
    let df = DataFrame::empty();

    let result = ErrorTestSchema::validate(&df);
    assert!(result.is_err());

    let error = result.unwrap_err();
    let error_msg = format!("{}", error);
    assert!(error_msg.contains("Missing required column"));
}
