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

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct SingleField {
    value: String,
}

#[test]
fn test_column_constants() {
    assert_eq!(TestSchema::user_id, "user_id");
    assert_eq!(TestSchema::username, "username");
    assert_eq!(TestSchema::email, "email");
    assert_eq!(TestSchema::age, "age");
    assert_eq!(TestSchema::is_active, "is_active");
}

#[test]
fn test_column_names_function() {
    let expected = vec!["user_id", "username", "email", "age", "is_active"];
    let actual = TestSchema::column_names();
    assert_eq!(actual, expected);
}

#[test]
fn test_column_name_at() {
    assert_eq!(TestSchema::column_name_at(0), Some("user_id"));
    assert_eq!(TestSchema::column_name_at(1), Some("username"));
    assert_eq!(TestSchema::column_name_at(2), Some("email"));
    assert_eq!(TestSchema::column_name_at(3), Some("age"));
    assert_eq!(TestSchema::column_name_at(4), Some("is_active"));
    assert_eq!(TestSchema::column_name_at(5), None);
}

#[test]
fn test_col_expr() {
    let user_id_expr = TestSchema::col_expr("user_id");
    assert!(user_id_expr.is_some());

    let username_expr = TestSchema::col_expr("username");
    assert!(username_expr.is_some());

    let nonexistent_expr = TestSchema::col_expr("nonexistent");
    assert!(nonexistent_expr.is_none());
}

#[test]
fn test_columns_trait() {
    let expected = vec!["user_id", "username", "email", "age", "is_active"];
    let actual = TestSchema::columns();
    assert_eq!(actual, expected);
}

#[test]
fn test_expression_helpers() {
    // Test that expr helper struct exists and methods work
    let _user_id_expr = TestSchema::expr.user_id();
    let _username_expr = TestSchema::expr.username();
    let _email_expr = TestSchema::expr.email();
    let _age_expr = TestSchema::expr.age();
    let _is_active_expr = TestSchema::expr.is_active();

    // These should not panic and should create valid expressions
    // The actual Expr type doesn't have easy equality testing,
    // so we just verify they can be created
}

#[test]
fn test_single_field_schema() {
    assert_eq!(SingleField::value, "value");

    let cols = SingleField::column_names();
    assert_eq!(cols, vec!["value"]);

    assert_eq!(SingleField::column_name_at(0), Some("value"));
    assert_eq!(SingleField::column_name_at(1), None);
}

#[test]
fn test_column_helpers_with_dataframe() {
    let df = df![
        "user_id" => [1i64, 2i64, 3i64],
        "username" => ["alice", "bob", "charlie"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "age" => [25, 30, 35],
        "is_active" => [true, false, true],
    ]
    .unwrap();

    // Test that we can use column constants for selection
    let selected = df
        .clone()
        .lazy()
        .select([
            col(TestSchema::user_id),
            col(TestSchema::username),
            col(TestSchema::email),
        ])
        .collect()
        .unwrap();

    assert_eq!(selected.width(), 3);
    assert_eq!(selected.height(), 3);

    // Test using expression helpers
    let with_expr = df
        .clone()
        .lazy()
        .select([
            TestSchema::expr.user_id(),
            TestSchema::expr.username().alias("name"),
            TestSchema::expr.age(),
        ])
        .collect()
        .unwrap();

    assert_eq!(with_expr.width(), 3);
    assert_eq!(with_expr.height(), 3);
    let column_names: Vec<&str> = with_expr
        .get_column_names()
        .iter()
        .map(|s| s.as_str())
        .collect();
    assert!(column_names.contains(&"name"));
}

#[test]
fn test_column_helpers_with_filters() {
    let df = df![
        "user_id" => [1i64, 2i64, 3i64, 4i64],
        "username" => ["alice", "bob", "charlie", "david"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com", "david@test.com"],
        "age" => [25, 30, 35, 20],
        "is_active" => [true, false, true, true],
    ]
    .unwrap();

    // Filter using column helpers
    let filtered = df
        .clone()
        .lazy()
        .filter(TestSchema::expr.age().gt(lit(25)))
        .filter(TestSchema::expr.is_active().eq(lit(true)))
        .select([TestSchema::expr.username(), TestSchema::expr.age()])
        .collect()
        .unwrap();

    assert_eq!(filtered.height(), 1); // Only charlie matches both conditions
    assert_eq!(filtered.width(), 2);
}

#[test]
fn test_column_helpers_with_aggregations() {
    let df = df![
        "user_id" => [1i64, 2i64, 3i64, 4i64],
        "username" => ["alice", "bob", "charlie", "david"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com", "david@test.com"],
        "age" => [25, 30, 35, 20],
        "is_active" => [true, false, true, true],
    ]
    .unwrap();

    // Aggregation using column helpers
    let agg_result = df
        .clone()
        .lazy()
        .select([
            TestSchema::expr.age().mean().alias("avg_age"),
            TestSchema::expr.age().max().alias("max_age"),
            TestSchema::expr.age().min().alias("min_age"),
            TestSchema::expr.user_id().count().alias("count"),
        ])
        .collect()
        .unwrap();

    assert_eq!(agg_result.height(), 1);
    assert_eq!(agg_result.width(), 4);
}

#[test]
fn test_expr_all_cols() {
    // Test the new expr.all_cols() method for API consistency
    let df = df![
        "user_id" => [1i64, 2i64, 3i64],
        "username" => ["alice", "bob", "charlie"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "age" => [25, 30, 35],
        "is_active" => [true, false, true],
    ]
    .unwrap();

    // Test that expr.all_cols() returns Vec<Expr> and works in lazy operations
    let result = df
        .clone()
        .lazy()
        .select(TestSchema::expr.all_cols()) // Use the new method
        .filter(TestSchema::expr.age().gt(lit(25)))
        .collect()
        .unwrap();

    assert_eq!(result.width(), 5); // All columns
    assert_eq!(result.height(), 2); // Filtered rows (age > 25)
    
    // Verify column names match expected schema
    let column_names = result.get_column_names();
    let expected = ["user_id", "username", "email", "age", "is_active"];
    for expected_col in expected {
        assert!(column_names.iter().any(|&col| col == expected_col));
    }
    
    // Test that both methods return equivalent results
    let with_struct_method = df.clone().lazy().select(TestSchema::all_cols()).collect().unwrap();
    let with_expr_method = df.clone().lazy().select(TestSchema::expr.all_cols()).collect().unwrap();
    
    // Both should have same structure
    assert_eq!(with_struct_method.width(), with_expr_method.width());
    assert_eq!(with_struct_method.height(), with_expr_method.height());
    assert_eq!(with_struct_method.get_column_names(), with_expr_method.get_column_names());
}
