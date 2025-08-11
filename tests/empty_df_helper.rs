#![allow(non_upper_case_globals)]
use polars_tools::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct User {
    id: i64,
    name: String,
    age: i32,
    active: bool,
}

#[derive(Debug, Serialize, Deserialize, PolarsColumns)]
#[allow(dead_code, non_upper_case_globals)]
struct Product {
    product_id: i32,
    product_name: String,
    price: f64,
}

#[test]
fn test_empty_dataframe_creation_with_polars_schema() {
    let empty_df = User::df().unwrap();

    // Should have correct number of columns
    assert_eq!(empty_df.width(), 4);

    // Should have 0 rows
    assert_eq!(empty_df.height(), 0);

    // Should have correct column names
    let column_names = empty_df.get_column_names();
    assert_eq!(column_names.len(), 4);
    assert!(column_names.iter().any(|&s| s == "id"));
    assert!(column_names.iter().any(|&s| s == "name"));
    assert!(column_names.iter().any(|&s| s == "age"));
    assert!(column_names.iter().any(|&s| s == "active"));

    // Should have correct data types
    let schema = empty_df.schema();
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("name"), Some(&DataType::String));
    assert_eq!(schema.get("age"), Some(&DataType::Int32));
    assert_eq!(schema.get("active"), Some(&DataType::Boolean));

    // Should pass validation
    assert!(User::validate(&empty_df).is_ok());
    assert!(User::validate_strict(&empty_df).is_ok());
}

#[test]
fn test_empty_dataframe_creation_with_polars_columns() {
    let empty_df = Product::df().unwrap();

    // Should have correct structure
    assert_eq!(empty_df.width(), 3);
    assert_eq!(empty_df.height(), 0);

    // Should have correct column names
    let column_names = empty_df.get_column_names();
    assert_eq!(column_names.len(), 3);
    assert!(column_names.iter().any(|&s| s == "product_id"));
    assert!(column_names.iter().any(|&s| s == "product_name"));
    assert!(column_names.iter().any(|&s| s == "price"));

    // Should have correct data types
    let schema = empty_df.schema();
    assert_eq!(schema.get("product_id"), Some(&DataType::Int32));
    assert_eq!(schema.get("product_name"), Some(&DataType::String));
    assert_eq!(schema.get("price"), Some(&DataType::Float64));
}

#[test]
fn test_empty_dataframe_can_be_extended() {
    let empty_df = User::df().unwrap();

    // Add some data
    let new_data = df![
        "id" => [1i64, 2i64],
        "name" => ["Alice", "Bob"],
        "age" => [25, 30],
        "active" => [true, false],
    ]
    .unwrap();

    // Verify they have the same schema
    assert_eq!(empty_df.schema(), new_data.schema());

    // Can use vstack to combine them
    let combined = empty_df.vstack(&new_data).unwrap();

    assert_eq!(combined.height(), 2); // Should have the new rows
    assert_eq!(combined.width(), 4); // Same columns

    // Should still pass validation
    assert!(User::validate(&combined).is_ok());
    assert!(User::validate_strict(&combined).is_ok());
}

#[test]
fn test_empty_dataframe_with_all_types() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct AllTypes {
        int32_field: i32,
        int64_field: i64,
        float32_field: f32,
        float64_field: f64,
        bool_field: bool,
        string_field: String,
    }

    let empty_df = AllTypes::df().unwrap();

    assert_eq!(empty_df.width(), 6);
    assert_eq!(empty_df.height(), 0);

    let schema = empty_df.schema();
    assert_eq!(schema.get("int32_field"), Some(&DataType::Int32));
    assert_eq!(schema.get("int64_field"), Some(&DataType::Int64));
    assert_eq!(schema.get("float32_field"), Some(&DataType::Float32));
    assert_eq!(schema.get("float64_field"), Some(&DataType::Float64));
    assert_eq!(schema.get("bool_field"), Some(&DataType::Boolean));
    assert_eq!(schema.get("string_field"), Some(&DataType::String));

    assert!(AllTypes::validate(&empty_df).is_ok());
    assert!(AllTypes::validate_strict(&empty_df).is_ok());
}

#[test]
fn test_empty_dataframe_with_optional_fields() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct OptionalFields {
        required_field: String,
        optional_int: Option<i32>,
        optional_string: Option<String>,
    }

    let empty_df = OptionalFields::df().unwrap();

    assert_eq!(empty_df.width(), 3);
    assert_eq!(empty_df.height(), 0);

    // Optional fields should have the base type (Option<T> maps to T in Polars)
    let schema = empty_df.schema();
    let column_names = empty_df.get_column_names();

    // Verify column names are present
    assert!(column_names.iter().any(|&s| s == "required_field"));
    assert!(column_names.iter().any(|&s| s == "optional_int"));
    assert!(column_names.iter().any(|&s| s == "optional_string"));

    // Check data types for each field
    assert_eq!(schema.get("required_field"), Some(&DataType::String));
    assert_eq!(schema.get("optional_int"), Some(&DataType::Int32));
    assert_eq!(schema.get("optional_string"), Some(&DataType::String));

    assert!(OptionalFields::validate(&empty_df).is_ok());
    assert!(OptionalFields::validate_strict(&empty_df).is_ok());
}

#[test]
fn test_empty_dataframe_practical_usage() {
    // Test a real-world scenario where you start with an empty DataFrame
    let mut results_df = User::df().unwrap();

    // Simulate processing batches of data
    for batch_num in 1..=3 {
        let batch_data = df![
            "id" => [batch_num as i64],
            "name" => [format!("User{}", batch_num)],
            "age" => [20 + batch_num],
            "active" => [batch_num % 2 == 1],
        ]
        .unwrap();

        // Combine with existing results using vstack
        results_df = results_df.vstack(&batch_data).unwrap();
    }

    // Final result should have all data
    assert_eq!(results_df.height(), 3);
    assert_eq!(results_df.width(), 4);

    // Should still pass validation
    assert!(User::validate(&results_df).is_ok());
    assert!(User::validate_strict(&results_df).is_ok());

    // Can use column helpers on the result
    let active_users = results_df
        .lazy()
        .filter(User::expr.active().eq(lit(true)))
        .select(User::all_cols())
        .collect()
        .unwrap();

    assert_eq!(active_users.height(), 2); // User1 and User3 are active
}

#[test]
fn test_single_field_empty_dataframe() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct SingleField {
        value: i64,
    }

    let empty_df = SingleField::df().unwrap();

    assert_eq!(empty_df.width(), 1);
    assert_eq!(empty_df.height(), 0);

    let schema = empty_df.schema();
    assert_eq!(schema.get("value"), Some(&DataType::Int64));

    assert!(SingleField::validate(&empty_df).is_ok());
    assert!(SingleField::validate_strict(&empty_df).is_ok());
}

#[test]
fn test_empty_dataframe_column_order() {
    // Test that the empty DataFrame has columns in the same order as struct fields
    let empty_df = User::df().unwrap();

    let column_names = empty_df.get_column_names();
    let expected_order = ["id", "name", "age", "active"];

    assert_eq!(column_names.len(), expected_order.len());
    for (actual, expected) in column_names.iter().zip(expected_order.iter()) {
        assert_eq!(actual, expected);
    }
}

#[cfg(feature = "chrono")]
#[test]
fn test_empty_dataframe_with_chrono_types() {
    use chrono::{NaiveDate, NaiveDateTime};

    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    #[allow(dead_code, non_upper_case_globals)]
    struct ChronoTypes {
        date_field: NaiveDate,
        datetime_field: NaiveDateTime,
    }

    let empty_df = ChronoTypes::df().unwrap();

    assert_eq!(empty_df.width(), 2);
    assert_eq!(empty_df.height(), 0);

    let schema = empty_df.schema();
    assert_eq!(schema.get("date_field"), Some(&DataType::Date));
    // DateTime should be Datetime with microsecond precision
    assert!(matches!(
        schema.get("datetime_field"),
        Some(&DataType::Datetime(_, _))
    ));

    assert!(ChronoTypes::validate(&empty_df).is_ok());
    assert!(ChronoTypes::validate_strict(&empty_df).is_ok());
}
