#![allow(non_upper_case_globals)]
use polars_tools::*;
use serde::{Deserialize, Serialize};

// Test all supported integer types
#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct IntegerTypes {
    int8_col: i8,
    int16_col: i16,
    int32_col: i32,
    int64_col: i64,
    uint8_col: u8,
    uint16_col: u16,
    uint32_col: u32,
    uint64_col: u64,
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct FloatTypes {
    float32_col: f32,
    float64_col: f64,
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct BasicTypes {
    bool_col: bool,
    string_col: String,
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct OptionalTypes {
    required_field: String,
    optional_int: Option<i32>,
    optional_string: Option<String>,
    optional_bool: Option<bool>,
}

#[test]
fn test_integer_type_constants() {
    assert_eq!(IntegerTypes::int8_col, "int8_col");
    assert_eq!(IntegerTypes::int16_col, "int16_col");
    assert_eq!(IntegerTypes::int32_col, "int32_col");
    assert_eq!(IntegerTypes::int64_col, "int64_col");
    assert_eq!(IntegerTypes::uint8_col, "uint8_col");
    assert_eq!(IntegerTypes::uint16_col, "uint16_col");
    assert_eq!(IntegerTypes::uint32_col, "uint32_col");
    assert_eq!(IntegerTypes::uint64_col, "uint64_col");
}

#[test]
fn test_float_type_constants() {
    assert_eq!(FloatTypes::float32_col, "float32_col");
    assert_eq!(FloatTypes::float64_col, "float64_col");
}

#[test]
fn test_basic_type_constants() {
    assert_eq!(BasicTypes::bool_col, "bool_col");
    assert_eq!(BasicTypes::string_col, "string_col");
}

#[test]
fn test_optional_type_constants() {
    assert_eq!(OptionalTypes::required_field, "required_field");
    assert_eq!(OptionalTypes::optional_int, "optional_int");
    assert_eq!(OptionalTypes::optional_string, "optional_string");
    assert_eq!(OptionalTypes::optional_bool, "optional_bool");
}

#[test]
fn test_column_names_all_integer_types() {
    let expected = vec![
        "int8_col",
        "int16_col",
        "int32_col",
        "int64_col",
        "uint8_col",
        "uint16_col",
        "uint32_col",
        "uint64_col",
    ];
    assert_eq!(IntegerTypes::column_names(), expected);
}

#[test]
fn test_column_names_float_types() {
    let expected = vec!["float32_col", "float64_col"];
    assert_eq!(FloatTypes::column_names(), expected);
}

#[test]
fn test_column_names_basic_types() {
    let expected = vec!["bool_col", "string_col"];
    assert_eq!(BasicTypes::column_names(), expected);
}

#[test]
fn test_column_names_optional_types() {
    let expected = vec![
        "required_field",
        "optional_int",
        "optional_string",
        "optional_bool",
    ];
    assert_eq!(OptionalTypes::column_names(), expected);
}

#[test]
fn test_basic_types_validation() {
    let df = df![
        "bool_col" => [true, false, true],
        "string_col" => ["hello", "world", "test"],
    ]
    .unwrap();

    assert!(BasicTypes::validate(&df).is_ok());
    assert!(BasicTypes::validate_strict(&df).is_ok());
}

#[test]
fn test_float_types_validation() {
    let df = df![
        "float32_col" => [1.0f32, 2.5f32, 3.7f32],
        "float64_col" => [1.0f64, 2.5f64, 3.7f64],
    ]
    .unwrap();

    assert!(FloatTypes::validate(&df).is_ok());
    assert!(FloatTypes::validate_strict(&df).is_ok());
}

#[test]
fn test_standard_integer_types_validation() {
    // Test with commonly supported integer types
    let df = df![
        "int32_col" => [1i32, 2i32, 3i32],
        "int64_col" => [1i64, 2i64, 3i64],
    ]
    .unwrap();

    #[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    struct StandardInts {
        int32_col: i32,
        int64_col: i64,
    }

    assert!(StandardInts::validate(&df).is_ok());
    assert!(StandardInts::validate_strict(&df).is_ok());
}

#[test]
fn test_wrong_integer_type_error() {
    let df = df![
        "int32_col" => [1i64, 2i64, 3i64], // i64 instead of i32
        "int64_col" => [1i64, 2i64, 3i64],
    ]
    .unwrap();

    #[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    struct StrictInts {
        int32_col: i32,
        int64_col: i64,
    }

    let result = StrictInts::validate(&df);
    // This should fail because the types don't match exactly
    assert!(result.is_err());
}

#[test]
fn test_wrong_float_type_error() {
    let df = df![
        "float32_col" => [1.0f64, 2.5f64, 3.7f64], // f64 instead of f32
        "float64_col" => [1.0f64, 2.5f64, 3.7f64],
    ]
    .unwrap();

    #[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    struct StrictFloats {
        float32_col: f32,
        float64_col: f64,
    }

    let result = StrictFloats::validate(&df);
    // This should fail because the types don't match exactly
    assert!(result.is_err());
}

#[test]
fn test_wrong_basic_type_error() {
    let df = df![
        "bool_col" => [1, 0, 1], // i32 instead of bool
        "string_col" => ["hello", "world", "test"],
    ]
    .unwrap();

    let result = BasicTypes::validate(&df);
    assert!(result.is_err());
    let error_msg = format!("{}", result.unwrap_err());
    assert!(error_msg.contains("Column 'bool_col' has type"));
}

#[test]
fn test_mixed_types_schema() {
    #[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    struct MixedSchema {
        id: i64,
        score: f64,
        name: String,
        active: bool,
    }

    let df = df![
        "id" => [1i64, 2i64, 3i64],
        "score" => [85.5f64, 92.0f64, 78.3f64],
        "name" => ["Alice", "Bob", "Charlie"],
        "active" => [true, false, true],
    ]
    .unwrap();

    assert!(MixedSchema::validate(&df).is_ok());
    assert!(MixedSchema::validate_strict(&df).is_ok());

    // Test column helpers work with mixed types
    let selected = df
        .clone()
        .lazy()
        .select([
            MixedSchema::expr.id(),
            MixedSchema::expr.name(),
            MixedSchema::expr.score().gt(lit(80.0)).alias("high_score"),
        ])
        .collect()
        .unwrap();

    assert_eq!(selected.width(), 3);
    assert_eq!(selected.height(), 3);
}

// Test a schema with all different basic types
#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct AllBasicTypesSchema {
    tiny_int: i32, // We'll use i32 as polars df! macro supports it well
    big_int: i64,
    small_float: f32,
    big_float: f64,
    flag: bool,
    text: String,
}

#[test]
fn test_all_basic_types_together() {
    let df = df![
        "tiny_int" => [1i32, 2i32, 3i32],
        "big_int" => [100i64, 200i64, 300i64],
        "small_float" => [1.1f32, 2.2f32, 3.3f32],
        "big_float" => [10.1f64, 20.2f64, 30.3f64],
        "flag" => [true, false, true],
        "text" => ["first", "second", "third"],
    ]
    .unwrap();

    assert!(AllBasicTypesSchema::validate(&df).is_ok());
    assert!(AllBasicTypesSchema::validate_strict(&df).is_ok());

    // Test that column helpers work
    assert_eq!(AllBasicTypesSchema::tiny_int, "tiny_int");
    assert_eq!(AllBasicTypesSchema::big_int, "big_int");
    assert_eq!(AllBasicTypesSchema::small_float, "small_float");
    assert_eq!(AllBasicTypesSchema::big_float, "big_float");
    assert_eq!(AllBasicTypesSchema::flag, "flag");
    assert_eq!(AllBasicTypesSchema::text, "text");

    let col_names = AllBasicTypesSchema::column_names();
    assert_eq!(col_names.len(), 6);
    assert!(col_names.contains(&"tiny_int"));
    assert!(col_names.contains(&"text"));
}
