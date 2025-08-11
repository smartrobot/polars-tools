#![allow(non_upper_case_globals)]
use polars_tools::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct TestSchema {
    id: i64,
    name: String,
    age: i32,
    active: bool,
}

#[test]
fn test_missing_column_error() {
    let df = df![
        "id" => [1i64, 2i64],
        "name" => ["Alice", "Bob"],
        // Missing "age" and "active" columns
    ]
    .unwrap();

    let result = TestSchema::validate(&df);
    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        ValidationError::MissingColumn { column_name } => {
            assert_eq!(column_name, "age");
        }
        _ => panic!("Expected MissingColumn error, got: {:?}", err),
    }
}

#[test]
fn test_type_mismatch_error() {
    let df = df![
        "id" => [1i64, 2i64],
        "name" => ["Alice", "Bob"],
        "age" => ["25", "30"], // String instead of i32
        "active" => [true, false],
    ]
    .unwrap();

    let result = TestSchema::validate(&df);
    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        ValidationError::TypeMismatch {
            column_name,
            actual_type,
            expected_type,
        } => {
            assert_eq!(column_name, "age");
            assert!(actual_type.contains("String"));
            assert!(expected_type.contains("Int32"));
        }
        _ => panic!("Expected TypeMismatch error, got: {:?}", err),
    }
}

#[test]
fn test_column_count_mismatch_error() {
    let df = df![
        "id" => [1i64, 2i64],
        "name" => ["Alice", "Bob"],
        "age" => [25, 30],
        "active" => [true, false],
        "extra_column" => ["extra1", "extra2"], // Extra column
    ]
    .unwrap();

    // Standard validation should pass (allows extra columns)
    assert!(TestSchema::validate(&df).is_ok());

    // Strict validation should fail
    let result = TestSchema::validate_strict(&df);
    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        ValidationError::ColumnCountMismatch { expected, actual } => {
            assert_eq!(expected.len(), 4);
            assert_eq!(actual.len(), 5);
            assert!(expected.contains("id"));
            assert!(expected.contains("name"));
            assert!(expected.contains("age"));
            assert!(expected.contains("active"));
            assert!(actual.contains("extra_column"));
        }
        _ => panic!("Expected ColumnCountMismatch error, got: {:?}", err),
    }
}

#[test]
fn test_all_supported_integer_types() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct IntegerTypes {
        i32_col: i32,
        i64_col: i64,
        u32_col: u32,
        u64_col: u64,
    }

    let df = df![
        "i32_col" => [1i32, 2i32],
        "i64_col" => [1i64, 2i64],
        "u32_col" => [1u32, 2u32],
        "u64_col" => [1u64, 2u64],
    ]
    .unwrap();

    assert!(IntegerTypes::validate(&df).is_ok());
    assert!(IntegerTypes::validate_strict(&df).is_ok());
}

#[test]
fn test_smaller_integer_types_support() {
    // Test that the derive macro properly supports smaller integer types in type mapping
    // Note: While the types are supported in validation, creating test data requires casting
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct SmallIntTypes {
        i8_val: i8,
        i16_val: i16,
    }

    // Create series with supported types and cast
    let i8_data: Vec<i8> = vec![1i8, 2i8];
    let i16_data: Vec<i16> = vec![1i16, 2i16];

    let s1 = Series::new("i8_val".into(), i8_data);
    let s2 = Series::new("i16_val".into(), i16_data);

    let df = DataFrame::new(vec![s1, s2]).unwrap();

    assert!(SmallIntTypes::validate(&df).is_ok());
    assert!(SmallIntTypes::validate_strict(&df).is_ok());
}

#[test]
fn test_integer_type_mismatch_errors() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct TestI32 {
        value: i32,
    }

    // Test with wrong integer type
    let df_wrong = df![
        "value" => [1i64, 2i64], // i64 instead of i32
    ]
    .unwrap();

    let result = TestI32::validate(&df_wrong);
    assert!(result.is_err());

    match result.unwrap_err() {
        ValidationError::TypeMismatch {
            column_name,
            actual_type,
            expected_type,
        } => {
            assert_eq!(column_name, "value");
            assert!(actual_type.contains("Int64"));
            assert!(expected_type.contains("Int32"));
        }
        _ => panic!("Expected TypeMismatch error"),
    }
}

#[test]
fn test_float_types() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct FloatTypes {
        f32_col: f32,
        f64_col: f64,
    }

    let df = df![
        "f32_col" => [1.0f32, 2.0f32],
        "f64_col" => [1.0f64, 2.0f64],
    ]
    .unwrap();

    assert!(FloatTypes::validate(&df).is_ok());
    assert!(FloatTypes::validate_strict(&df).is_ok());
}

#[test]
fn test_boolean_and_string_types() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct MixedTypes {
        flag: bool,
        text: String,
    }

    let df = df![
        "flag" => [true, false, true],
        "text" => ["hello", "world", "test"],
    ]
    .unwrap();

    assert!(MixedTypes::validate(&df).is_ok());
    assert!(MixedTypes::validate_strict(&df).is_ok());
}

#[test]
fn test_error_display_formatting() {
    let missing_err = ValidationError::MissingColumn {
        column_name: "test_column".to_string(),
    };
    let error_msg = format!("{}", missing_err);
    assert!(error_msg.contains("Missing required column: test_column"));

    let type_err = ValidationError::TypeMismatch {
        column_name: "age".to_string(),
        actual_type: "String".to_string(),
        expected_type: "Int32".to_string(),
    };
    let type_msg = format!("{}", type_err);
    assert!(type_msg.contains("Column 'age' has type"));
    assert!(type_msg.contains("String"));
    assert!(type_msg.contains("Int32"));
}

#[test]
fn test_multiple_validation_errors() {
    // Test that validation stops at first error (as expected)
    let df = df![
        "id" => [1i64, 2i64],
        // Missing "name", "age", "active" - should fail on first missing column
    ]
    .unwrap();

    let result = TestSchema::validate(&df);
    assert!(result.is_err());

    // Should get the first missing column error
    match result.unwrap_err() {
        ValidationError::MissingColumn { .. } => {
            // Expected - validation stops on first error
        }
        _ => panic!("Expected MissingColumn error"),
    }
}

#[cfg(feature = "chrono")]
#[test]
fn test_chrono_type_validation() {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    struct ChronoTypes {
        date_col: NaiveDate,
        datetime_col: NaiveDateTime,
        utc_datetime_col: DateTime<Utc>,
    }

    let df = df![
        "date_col" => [NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()],
        "datetime_col" => [NaiveDate::from_ymd_opt(2023, 1, 1).unwrap().and_hms_opt(12, 0, 0).unwrap()],
        "utc_datetime_col" => [DateTime::from_timestamp(1672531200, 0).unwrap()], // 2023-01-01 00:00:00 UTC
    ].unwrap();

    assert!(ChronoTypes::validate(&df).is_ok());
}
