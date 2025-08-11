#![allow(non_upper_case_globals)]
use polars_tools::*;

// Test enum for schema validation
#[derive(Debug, Clone, PartialEq)]
enum Priority {
    Low,
    Medium,
    High,
    Critical,
}

// Implement ValidatableEnum for Priority
impl ValidatableEnum for Priority {
    fn valid_values() -> Vec<&'static str> {
        vec!["Low", "Medium", "High", "Critical"]
    }
    
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "Low" => Ok(Priority::Low),
            "Medium" => Ok(Priority::Medium),
            "High" => Ok(Priority::High),
            "Critical" => Ok(Priority::Critical),
            _ => Err(ValidationError::InvalidEnumValue {
                field: "Priority".to_string(),
                value: value.to_string(),
                valid_values: Self::valid_values().into_iter().map(|s| s.to_string()).collect(),
            }),
        }
    }
    
    fn to_str(&self) -> &'static str {
        match self {
            Priority::Low => "Low",
            Priority::Medium => "Medium",
            Priority::High => "High",
            Priority::Critical => "Critical",
        }
    }
}

// Test struct using PolarsSchema with enum field
#[derive(Debug, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct Task {
    id: i64,
    title: String,
    priority: Priority,  // Enum field - should be mapped to String in Polars
    completed: bool,
    created_at: String,
}

#[test]
fn test_enum_schema_type_mapping() {
    // Test that enum fields are mapped to String DataType in PolarsSchema
    let types = Task::all_types();
    
    assert_eq!(types[0], DataType::Int64);     // id
    assert_eq!(types[1], DataType::String);    // title
    assert_eq!(types[2], DataType::String);    // priority (enum -> String)
    assert_eq!(types[3], DataType::Boolean);   // completed
    assert_eq!(types[4], DataType::String);    // created_at
    
    // Test individual type constants
    assert_eq!(Task::id_type, DataType::Int64);
    assert_eq!(Task::title_type, DataType::String);
    assert_eq!(Task::priority_type, DataType::String);  // Enum mapped to String
    assert_eq!(Task::completed_type, DataType::Boolean);
    assert_eq!(Task::created_at_type, DataType::String);
}

#[test]
fn test_enum_schema_empty_dataframe() {
    // Test that df() method works with enum fields in PolarsSchema
    let empty_df = Task::df().unwrap();
    
    assert_eq!(empty_df.height(), 0);  // 0 rows
    assert_eq!(empty_df.width(), 5);   // 5 columns
    
    // Verify schema types
    let schema = empty_df.schema();
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("title"), Some(&DataType::String));
    assert_eq!(schema.get("priority"), Some(&DataType::String));  // Enum mapped to String
    assert_eq!(schema.get("completed"), Some(&DataType::Boolean));
    assert_eq!(schema.get("created_at"), Some(&DataType::String));
}

#[test]
fn test_enum_schema_validation_with_valid_data() {
    // Test schema validation with valid enum values
    let valid_df = df![
        "id" => [1i64, 2i64, 3i64],
        "title" => ["Fix bug", "Add feature", "Write docs"],
        "priority" => ["High", "Medium", "Low"],  // Valid enum values as strings
        "completed" => [false, false, true],
        "created_at" => ["2023-01-01", "2023-01-02", "2023-01-03"],
    ].unwrap();
    
    // Basic schema validation should pass
    assert!(Task::validate(&valid_df).is_ok());
    assert!(Task::validate_strict(&valid_df).is_ok());
    
    // Test manual enum validation on the priority column
    let priority_col = valid_df.column("priority").unwrap();
    let string_values = priority_col.str().unwrap();
    
    for value_opt in string_values.into_iter() {
        if let Some(value) = value_opt {
            assert!(Priority::is_valid(value), "Value '{}' should be valid for Priority enum", value);
        }
    }
}

#[test]
fn test_enum_schema_validation_with_invalid_data() {
    // Test with DataFrame containing invalid enum values
    let invalid_df = df![
        "id" => [1i64, 2i64],
        "title" => ["Fix bug", "Add feature"],
        "priority" => ["High", "SuperUrgent"],  // "SuperUrgent" is invalid for Priority enum
        "completed" => [false, false],
        "created_at" => ["2023-01-01", "2023-01-02"],
    ].unwrap();
    
    // Basic schema validation should still pass (types are correct)
    assert!(Task::validate(&invalid_df).is_ok());
    assert!(Task::validate_strict(&invalid_df).is_ok());
    
    // But enum validation should detect the invalid value
    let priority_col = invalid_df.column("priority").unwrap();
    let string_values = priority_col.str().unwrap();
    
    let mut found_invalid = false;
    for value_opt in string_values.into_iter() {
        if let Some(value) = value_opt {
            if !Priority::is_valid(value) {
                found_invalid = true;
                assert_eq!(value, "SuperUrgent");
                
                // Test that from_str gives a proper error
                let result = Priority::from_str(value);
                assert!(result.is_err());
                match result.unwrap_err() {
                    ValidationError::InvalidEnumValue { field, value, valid_values } => {
                        assert_eq!(field, "Priority");
                        assert_eq!(value, "SuperUrgent");
                        assert_eq!(valid_values, vec!["Low", "Medium", "High", "Critical"]);
                    }
                    _ => panic!("Expected InvalidEnumValue error"),
                }
            }
        }
    }
    assert!(found_invalid, "Should have found invalid enum value");
}

#[test]
fn test_enum_all_column_helpers() {
    // Test that column helpers work correctly with enum fields
    assert_eq!(Task::id, "id");
    assert_eq!(Task::title, "title"); 
    assert_eq!(Task::priority, "priority");  // Enum field name
    assert_eq!(Task::completed, "completed");
    assert_eq!(Task::created_at, "created_at");
    
    // Test all_columns() includes enum field
    let all_cols = Task::all_columns();
    assert_eq!(all_cols.len(), 5);
    assert!(all_cols.contains(&"priority"));
    
    // Test column names function
    let col_names = Task::column_names();
    assert_eq!(col_names, vec!["id", "title", "priority", "completed", "created_at"]);
    assert!(col_names.contains(&"priority"));
}

#[test]
fn test_enum_expressions() {
    // Test that expression helpers work with enum fields
    let _id_expr = Task::expr.id();
    let _title_expr = Task::expr.title();
    let _priority_expr = Task::expr.priority();  // Should work for enum field
    let _completed_expr = Task::expr.completed();
    let _created_at_expr = Task::expr.created_at();
    
    // Test all_cols expressions
    let all_exprs = Task::all_cols();
    assert_eq!(all_exprs.len(), 5);
    
    let expr_all_exprs = Task::expr.all_cols();
    assert_eq!(expr_all_exprs.len(), 5);
}