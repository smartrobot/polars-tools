#![allow(non_upper_case_globals)]
use polars_tools::*;

// Test enum to validate against
#[derive(Debug, Clone, PartialEq)]
enum Status {
    Active,
    Inactive,
    Pending,
}

// Implement ValidatableEnum for our test enum
impl ValidatableEnum for Status {
    fn valid_values() -> Vec<&'static str> {
        vec!["Active", "Inactive", "Pending"]
    }
    
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "Active" => Ok(Status::Active),
            "Inactive" => Ok(Status::Inactive),
            "Pending" => Ok(Status::Pending),
            _ => Err(ValidationError::InvalidEnumValue {
                field: "Status".to_string(),
                value: value.to_string(),
                valid_values: Self::valid_values().into_iter().map(|s| s.to_string()).collect(),
            }),
        }
    }
    
    fn to_str(&self) -> &'static str {
        match self {
            Status::Active => "Active",
            Status::Inactive => "Inactive", 
            Status::Pending => "Pending",
        }
    }
}

// Test struct using PolarsColumns only (no serde)
#[derive(Debug, PolarsColumns)]
#[allow(dead_code, non_upper_case_globals)]
struct UserWithEnum {
    id: i64,
    name: String,
    status: Status,  // Enum field - should be mapped to String in Polars
    score: f64,
}

#[test]
fn test_enum_field_detected_as_string() {
    // Test that enum fields are mapped to String DataType
    let types = UserWithEnum::all_types();
    
    // id should be Int64
    assert_eq!(types[0], DataType::Int64);
    // name should be String  
    assert_eq!(types[1], DataType::String);
    // status (enum) should be mapped to String
    assert_eq!(types[2], DataType::String);
    // score should be Float64
    assert_eq!(types[3], DataType::Float64);
    
    // Test individual type constants
    assert_eq!(UserWithEnum::id_type, DataType::Int64);
    assert_eq!(UserWithEnum::name_type, DataType::String);
    assert_eq!(UserWithEnum::status_type, DataType::String);  // Enum -> String
    assert_eq!(UserWithEnum::score_type, DataType::Float64);
}

#[test]
fn test_empty_dataframe_with_enum() {
    // Test that df() method works with enum fields (mapped as strings)
    let empty_df = UserWithEnum::df().unwrap();
    
    assert_eq!(empty_df.height(), 0);  // 0 rows
    assert_eq!(empty_df.width(), 4);   // 4 columns
    
    // Verify schema types
    let schema = empty_df.schema();
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("name"), Some(&DataType::String));
    assert_eq!(schema.get("status"), Some(&DataType::String));  // Enum mapped to String
    assert_eq!(schema.get("score"), Some(&DataType::Float64));
}

#[test]
fn test_enum_field_column_helpers() {
    // Test that column helpers work with enum fields
    assert_eq!(UserWithEnum::id, "id");
    assert_eq!(UserWithEnum::name, "name");
    assert_eq!(UserWithEnum::status, "status");  // Enum field name
    assert_eq!(UserWithEnum::score, "score");
    
    // Test all_columns() includes enum field
    let all_cols = UserWithEnum::all_columns();
    assert_eq!(all_cols.len(), 4);
    assert!(all_cols.contains(&"status"));
}

#[test]
fn test_enum_validation_trait() {
    // Test ValidatableEnum trait functionality
    let valid_values = Status::valid_values();
    assert_eq!(valid_values, vec!["Active", "Inactive", "Pending"]);
    
    // Test valid conversions
    assert_eq!(Status::from_str("Active").unwrap(), Status::Active);
    assert_eq!(Status::from_str("Inactive").unwrap(), Status::Inactive);
    assert_eq!(Status::from_str("Pending").unwrap(), Status::Pending);
    
    // Test invalid conversion
    let result = Status::from_str("Invalid");
    assert!(result.is_err());
    match result.unwrap_err() {
        ValidationError::InvalidEnumValue { field, value, valid_values } => {
            assert_eq!(field, "Status");
            assert_eq!(value, "Invalid");
            assert_eq!(valid_values, vec!["Active", "Inactive", "Pending"]);
        }
        _ => panic!("Expected InvalidEnumValue error"),
    }
    
    // Test to_str conversion
    assert_eq!(Status::Active.to_str(), "Active");
    assert_eq!(Status::Inactive.to_str(), "Inactive");
    assert_eq!(Status::Pending.to_str(), "Pending");
    
    // Test is_valid helper
    assert!(Status::is_valid("Active"));
    assert!(Status::is_valid("Pending"));
    assert!(!Status::is_valid("Invalid"));
}

#[test]
fn test_enum_dataframe_validation_concept() {
    // This test demonstrates how enum validation would work with actual DataFrames
    
    // Create a DataFrame with valid enum values
    let valid_df = df![
        "id" => [1i64, 2i64, 3i64],
        "name" => ["Alice", "Bob", "Charlie"],
        "status" => ["Active", "Inactive", "Pending"],  // Valid enum values as strings
        "score" => [85.5f64, 92.0, 78.5],
    ].unwrap();
    
    // Test that we can validate enum values manually
    let status_col = valid_df.column("status").unwrap();
    let string_values = status_col.str().unwrap();
    
    for value_opt in string_values.into_iter() {
        if let Some(value) = value_opt {
            assert!(Status::is_valid(value), "Value '{}' should be valid for Status enum", value);
        }
    }
    
    // Create a DataFrame with invalid enum values
    let invalid_df = df![
        "id" => [1i64, 2i64],
        "name" => ["Alice", "Bob"],
        "status" => ["Active", "InvalidStatus"],  // Contains invalid enum value
        "score" => [85.5f64, 92.0],
    ].unwrap();
    
    // Test that we can detect invalid enum values
    let status_col = invalid_df.column("status").unwrap();
    let string_values = status_col.str().unwrap();
    
    let mut found_invalid = false;
    for value_opt in string_values.into_iter() {
        if let Some(value) = value_opt {
            if !Status::is_valid(value) {
                found_invalid = true;
                assert_eq!(value, "InvalidStatus");
            }
        }
    }
    assert!(found_invalid, "Should have found invalid enum value");
}