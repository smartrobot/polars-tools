#![allow(non_upper_case_globals)]
use polars_tools::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct TestSchema {
    user_id: i64,
    username: String,
    email: String,
    age: i32,
    is_active: bool,
}

#[derive(Debug, Serialize, Deserialize, PolarsColumns)]
struct ColumnOnlySchema {
    product_id: i32,
    product_name: String,
    price: f64,
}

#[test]
fn test_all_columns_with_polars_schema() {
    let df = df![
        "user_id" => [1i64, 2i64, 3i64],
        "username" => ["alice", "bob", "charlie"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "age" => [25, 30, 35],
        "is_active" => [true, false, true],
        "extra_column" => ["extra1", "extra2", "extra3"], // Extra column
    ]
    .unwrap();

    // Test that all_columns() contains all expected columns
    let all_columns = TestSchema::all_columns();
    assert_eq!(all_columns.len(), 5);
    assert!(all_columns.contains(&"user_id"));
    assert!(all_columns.contains(&"username"));
    assert!(all_columns.contains(&"email"));
    assert!(all_columns.contains(&"age"));
    assert!(all_columns.contains(&"is_active"));

    // Test using ALL for selection
    let selected_df = df.select(TestSchema::all_columns()).unwrap();

    assert_eq!(selected_df.width(), 5); // Should have exactly 5 columns
    assert_eq!(selected_df.height(), 3); // Should have 3 rows

    // Verify the selected columns match our schema
    let column_names = selected_df.get_column_names();
    assert_eq!(column_names.len(), 5);
    assert!(column_names.iter().any(|&s| s == "user_id"));
    assert!(column_names.iter().any(|&s| s == "username"));
    assert!(column_names.iter().any(|&s| s == "email"));
    assert!(column_names.iter().any(|&s| s == "age"));
    assert!(column_names.iter().any(|&s| s == "is_active"));

    // Should NOT contain the extra column
    assert!(!column_names.iter().any(|&s| s == "extra_column"));
}

#[test]
fn test_all_columns_with_polars_columns_only() {
    let df = df![
        "product_id" => [101, 102, 103],
        "product_name" => ["Widget A", "Widget B", "Widget C"],
        "price" => [19.99, 29.99, 39.99],
        "category" => ["Electronics", "Hardware", "Software"], // Extra column
    ]
    .unwrap();

    // Test that all_columns() contains all expected columns
    let all_columns = ColumnOnlySchema::all_columns();
    assert_eq!(all_columns.len(), 3);
    assert!(all_columns.contains(&"product_id"));
    assert!(all_columns.contains(&"product_name"));
    assert!(all_columns.contains(&"price"));

    // Test using ALL for selection
    let selected_df = df.select(ColumnOnlySchema::all_columns()).unwrap();

    assert_eq!(selected_df.width(), 3); // Should have exactly 3 columns
    assert_eq!(selected_df.height(), 3); // Should have 3 rows

    // Verify the selected columns
    let column_names = selected_df.get_column_names();
    assert_eq!(column_names.len(), 3);
    assert!(column_names.iter().any(|&s| s == "product_id"));
    assert!(column_names.iter().any(|&s| s == "product_name"));
    assert!(column_names.iter().any(|&s| s == "price"));

    // Should NOT contain the extra column
    assert!(!column_names.iter().any(|&s| s == "category"));
}

#[test]
fn test_all_columns_in_lazy_operations() {
    let df = df![
        "user_id" => [1i64, 2i64, 3i64, 4i64],
        "username" => ["alice", "bob", "charlie", "diana"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com", "diana@test.com"],
        "age" => [25, 30, 35, 28],
        "is_active" => [true, false, true, false],
        "extra_field" => ["a", "b", "c", "d"], // Extra field to filter out
    ]
    .unwrap();

    // Use all_cols() in lazy operations
    let result = df
        .clone()
        .lazy()
        .select(TestSchema::all_cols()) // Select only schema columns as expressions
        .filter(TestSchema::expr.is_active().eq(lit(true)))
        .collect()
        .unwrap();

    assert_eq!(result.width(), 5); // All schema columns
    assert_eq!(result.height(), 2); // Only active users (alice, charlie)

    // Verify no extra columns
    let column_names = result.get_column_names();
    assert!(!column_names.iter().any(|&s| s == "extra_field"));
}

#[test]
fn test_all_columns_order_matches_struct_definition() {
    // The order in all_columns() should match the order of fields in the struct definition
    let expected_order = vec!["user_id", "username", "email", "age", "is_active"];
    assert_eq!(TestSchema::all_columns(), expected_order);

    let expected_product_order = vec!["product_id", "product_name", "price"];
    assert_eq!(ColumnOnlySchema::all_columns(), expected_product_order);
}

#[test]
fn test_all_columns_with_single_field_struct() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    struct SingleField {
        id: i64,
    }

    let all_columns = SingleField::all_columns();
    assert_eq!(all_columns.len(), 1);
    assert_eq!(all_columns[0], "id");

    let df = df!["id" => [1i64, 2i64], "extra" => ["a", "b"]].unwrap();
    let selected = df.select(SingleField::all_columns()).unwrap();

    assert_eq!(selected.width(), 1);
    let column_names = selected.get_column_names();
    assert_eq!(column_names.len(), 1);
    assert!(column_names.iter().any(|&s| s == "id"));
}

#[test]
fn test_all_columns_real_world_example() {
    #[derive(PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
    struct CustomerRecord {
        customer_id: i64,
        name: String,
        email: String,
        signup_date: String,
        total_orders: i32,
        lifetime_value: f64,
    }

    // Simulate messy raw data with extra columns
    let raw_data = df![
        "customer_id" => [1i64, 2i64, 3i64],
        "name" => ["John Doe", "Jane Smith", "Bob Johnson"],
        "email" => ["john@example.com", "jane@example.com", "bob@example.com"],
        "signup_date" => ["2023-01-15", "2023-02-20", "2023-03-10"],
        "total_orders" => [5, 12, 3],
        "lifetime_value" => [450.75, 1250.50, 180.25],
        // Extra columns that we don't want
        "internal_id" => [12345, 67890, 11111],
        "last_login" => ["2023-12-01", "2023-12-02", "2023-12-03"],
        "marketing_segment" => ["A", "B", "C"],
    ]
    .unwrap();

    // Clean the data by selecting only the columns we care about
    let clean_data = raw_data.select(CustomerRecord::all_columns()).unwrap();

    // Verify we got exactly what we wanted
    assert_eq!(clean_data.width(), 6);
    CustomerRecord::validate(&clean_data).unwrap();
    CustomerRecord::validate_strict(&clean_data).unwrap();

    // Process with lazy operations
    let summary = clean_data
        .lazy()
        .filter(CustomerRecord::expr.total_orders().gt(lit(5)))
        .select([
            CustomerRecord::expr.name(),
            CustomerRecord::expr.lifetime_value(),
            CustomerRecord::expr.total_orders(),
        ])
        .collect()
        .unwrap();

    assert_eq!(summary.height(), 1); // Only Jane Smith has > 5 orders
    assert_eq!(summary.width(), 3);
}
