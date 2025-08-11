#![allow(non_upper_case_globals)]
use polars_tools::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct UserProfile {
    user_id: i64,
    username: String,
    email: String,
    age: i32,
    is_premium: bool,
    account_balance: f64,
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct ProductSales {
    product_id: i32,
    product_name: String,
    price: f64,
    quantity_sold: i32,
    is_discontinued: bool,
}

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct OptionalUserData {
    user_id: i64,
    username: String,
    middle_name: Option<String>,
    phone: Option<String>,
    birth_year: Option<i32>,
}

#[test]
fn test_complete_user_workflow() {
    // Create a realistic user dataset
    let df = df![
        "user_id" => [1i64, 2i64, 3i64, 4i64, 5i64],
        "username" => ["alice", "bob", "charlie", "diana", "eve"],
        "email" => [
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
            "diana@example.com",
            "eve@example.com"
        ],
        "age" => [25, 30, 35, 28, 32],
        "is_premium" => [true, false, true, false, true],
        "account_balance" => [150.50, 0.0, 250.75, 89.99, 500.00],
    ]
    .unwrap();

    // 1. Validate schema
    assert!(UserProfile::validate(&df).is_ok());
    assert!(UserProfile::validate_strict(&df).is_ok());

    // 2. Use column helpers for data analysis
    let premium_users = df
        .clone()
        .lazy()
        .filter(UserProfile::expr.is_premium().eq(lit(true)))
        .select([
            UserProfile::expr.username(),
            UserProfile::expr.account_balance(),
            UserProfile::expr.age(),
        ])
        .collect()
        .unwrap();

    assert_eq!(premium_users.height(), 3); // alice, charlie, eve
    assert_eq!(premium_users.width(), 3);

    // 3. Aggregation with column helpers
    let stats = df
        .clone()
        .lazy()
        .select([
            UserProfile::expr.age().mean().alias("avg_age"),
            UserProfile::expr
                .account_balance()
                .sum()
                .alias("total_balance"),
            UserProfile::expr.user_id().count().alias("user_count"),
            UserProfile::expr.is_premium().sum().alias("premium_count"),
        ])
        .collect()
        .unwrap();

    assert_eq!(stats.height(), 1);
    assert_eq!(stats.width(), 4);

    // 4. Complex filtering and transformation
    let analysis = df
        .clone()
        .lazy()
        .filter(UserProfile::expr.age().gt(lit(30)))
        .filter(UserProfile::expr.account_balance().gt(lit(100.0)))
        .with_columns([UserProfile::expr
            .account_balance()
            .gt(lit(200.0))
            .alias("high_balance")])
        .select([
            UserProfile::expr.username(),
            UserProfile::expr.age(),
            col("high_balance"),
        ])
        .collect()
        .unwrap();

    assert_eq!(analysis.width(), 3);
}

#[test]
fn test_product_sales_analysis() {
    let df = df![
        "product_id" => [101, 102, 103, 104, 105],
        "product_name" => ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"],
        "price" => [899.99, 25.99, 79.99, 299.99, 149.99],
        "quantity_sold" => [50, 200, 150, 75, 100],
        "is_discontinued" => [false, false, true, false, false],
    ]
    .unwrap();

    assert!(ProductSales::validate(&df).is_ok());

    // Calculate revenue using column helpers
    let revenue_analysis = df
        .clone()
        .lazy()
        .with_columns([(ProductSales::expr.price()
            * ProductSales::expr.quantity_sold().cast(DataType::Float64))
        .alias("total_revenue")])
        .filter(ProductSales::expr.is_discontinued().eq(lit(false)))
        .select([
            ProductSales::expr.product_name(),
            ProductSales::expr.price(),
            ProductSales::expr.quantity_sold(),
            col("total_revenue"),
        ])
        .sort(
            ["total_revenue"],
            SortMultipleOptions::default().with_order_descending(true),
        )
        .collect()
        .unwrap();

    assert_eq!(revenue_analysis.width(), 4);
    assert_eq!(revenue_analysis.height(), 4); // 4 non-discontinued products
}

#[test]
fn test_schema_evolution_compatibility() {
    // Test that adding extra columns doesn't break existing code
    let df_with_extra = df![
        "user_id" => [1i64, 2i64],
        "username" => ["alice", "bob"],
        "email" => ["alice@example.com", "bob@example.com"],
        "age" => [25, 30],
        "is_premium" => [true, false],
        "account_balance" => [150.50, 0.0],
        "extra_field1" => ["extra1", "extra2"],
        "extra_field2" => [100, 200],
    ]
    .unwrap();

    // Standard validation should pass (allows extra columns)
    assert!(UserProfile::validate(&df_with_extra).is_ok());

    // Strict validation should fail (no extra columns allowed)
    assert!(UserProfile::validate_strict(&df_with_extra).is_err());

    // Column helpers should still work
    let selected = df_with_extra
        .clone()
        .lazy()
        .select([UserProfile::expr.username(), UserProfile::expr.email()])
        .collect()
        .unwrap();

    assert_eq!(selected.width(), 2);
    assert_eq!(selected.height(), 2);
}

#[test]
fn test_optional_fields_workflow() {
    // Test that column helpers work even for optional field schemas
    // (Note: Full Option<T> validation requires more complex type analysis)
    let column_names = OptionalUserData::column_names();
    assert_eq!(column_names.len(), 5);
    assert!(column_names.contains(&"user_id"));
    assert!(column_names.contains(&"username"));
    assert!(column_names.contains(&"middle_name"));
    assert!(column_names.contains(&"phone"));
    assert!(column_names.contains(&"birth_year"));

    // Test that constants work
    assert_eq!(OptionalUserData::user_id, "user_id");
    assert_eq!(OptionalUserData::middle_name, "middle_name");
}

#[test]
fn test_large_dataset_performance() {
    // Test with a larger dataset to ensure performance is reasonable
    let size = 10_000;
    let user_ids: Vec<i64> = (1..=size).collect();
    let usernames: Vec<String> = (1..=size).map(|i| format!("user_{}", i)).collect();
    let emails: Vec<String> = (1..=size).map(|i| format!("user_{}@test.com", i)).collect();
    let ages: Vec<i32> = (1..=size).map(|i| 20 + ((i % 50) as i32)).collect();
    let is_premium: Vec<bool> = (1..=size).map(|i| i % 3 == 0).collect();
    let balances: Vec<f64> = (1..=size).map(|i| (i as f64) * 10.5).collect();

    let large_df = df![
        "user_id" => user_ids,
        "username" => usernames,
        "email" => emails,
        "age" => ages,
        "is_premium" => is_premium,
        "account_balance" => balances,
    ]
    .unwrap();

    // Validation should still be fast
    assert!(UserProfile::validate(&large_df).is_ok());
    assert!(UserProfile::validate_strict(&large_df).is_ok());

    // Column helpers should work with large datasets
    let summary = large_df
        .clone()
        .lazy()
        .filter(UserProfile::expr.is_premium().eq(lit(true)))
        .select([
            UserProfile::expr.age().mean().alias("avg_age"),
            UserProfile::expr
                .account_balance()
                .sum()
                .alias("total_balance"),
            UserProfile::expr.user_id().count().alias("premium_count"),
        ])
        .collect()
        .unwrap();

    assert_eq!(summary.height(), 1);
    assert_eq!(summary.width(), 3);
}

#[test]
fn test_cross_schema_operations() {
    // Test using multiple schemas together
    let users_df = df![
        "user_id" => [1i64, 2i64, 3i64],
        "username" => ["alice", "bob", "charlie"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "age" => [25, 30, 35],
        "is_premium" => [true, false, true],
        "account_balance" => [150.0, 50.0, 300.0],
    ]
    .unwrap();

    let products_df = df![
        "product_id" => [101, 102, 103],
        "product_name" => ["Widget A", "Widget B", "Widget C"],
        "price" => [19.99, 29.99, 39.99],
        "quantity_sold" => [100, 150, 200],
        "is_discontinued" => [false, false, true],
    ]
    .unwrap();

    assert!(UserProfile::validate(&users_df).is_ok());
    assert!(ProductSales::validate(&products_df).is_ok());

    // Use different schema column helpers in the same operation
    let user_summary = users_df
        .clone()
        .lazy()
        .select([UserProfile::expr.username(), UserProfile::expr.is_premium()])
        .collect()
        .unwrap();

    let product_summary = products_df
        .clone()
        .lazy()
        .select([
            ProductSales::expr.product_name(),
            ProductSales::expr.price(),
        ])
        .collect()
        .unwrap();

    assert_eq!(user_summary.width(), 2);
    assert_eq!(product_summary.width(), 2);
}

#[test]
fn test_real_world_data_pipeline() {
    // Simulate a complete data pipeline with validation at each step

    // Step 1: Raw data ingestion
    let raw_df = df![
        "user_id" => [1i64, 2i64, 3i64, 4i64],
        "username" => ["alice", "bob", "charlie", "diana"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com", "diana@test.com"],
        "age" => [25, 30, 35, 28],
        "is_premium" => [true, false, true, false],
        "account_balance" => [150.50, -10.0, 250.75, 89.99], // Note: negative balance
    ]
    .unwrap();

    // Step 2: Validate incoming data
    assert!(UserProfile::validate(&raw_df).is_ok());

    // Step 3: Data cleaning and transformation
    let cleaned_df = raw_df
        .clone()
        .lazy()
        .with_columns([
            // Clean negative balances - simplified approach
            when(UserProfile::expr.account_balance().lt(lit(0.0)))
                .then(lit(0.0))
                .otherwise(UserProfile::expr.account_balance())
                .alias("cleaned_balance"),
        ])
        .select([
            UserProfile::expr.user_id(),
            UserProfile::expr.username(),
            UserProfile::expr.email(),
            UserProfile::expr.age(),
            UserProfile::expr.is_premium(),
            col("cleaned_balance").alias(UserProfile::account_balance),
        ])
        .collect()
        .unwrap();

    // Step 4: Validate cleaned data
    assert!(UserProfile::validate(&cleaned_df).is_ok());
    assert!(UserProfile::validate_strict(&cleaned_df).is_ok());

    // Step 5: Business logic with validated data
    let business_metrics = cleaned_df
        .clone()
        .lazy()
        .select([
            UserProfile::expr.age().mean().alias("avg_customer_age"),
            UserProfile::expr
                .account_balance()
                .mean()
                .alias("avg_balance"),
            UserProfile::expr.is_premium().mean().alias("premium_rate"),
            UserProfile::expr.user_id().count().alias("total_users"),
        ])
        .collect()
        .unwrap();

    assert_eq!(business_metrics.height(), 1);
    assert_eq!(business_metrics.width(), 4);
}
