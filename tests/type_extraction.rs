#![allow(non_upper_case_globals)]
use polars_tools::*;
use polars::prelude::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
#[allow(dead_code, non_upper_case_globals)]
struct TestTypeExtraction {
    id: i64,
    name: String,
    age: i32,
    score: f64,
    is_active: bool,
    count: u32,
}

#[derive(Debug, Serialize, Deserialize, PolarsColumns)]
#[allow(dead_code, non_upper_case_globals)]
struct TestColumnTypeExtraction {
    product_id: i32,
    product_name: String,
    price: f64,
    in_stock: bool,
    quantity: u16,
}

#[test]
fn test_individual_field_type_constants_polars_schema() {
    // Test that individual type constants are generated correctly for PolarsSchema
    assert_eq!(TestTypeExtraction::id_type, DataType::Int64);
    assert_eq!(TestTypeExtraction::name_type, DataType::String);
    assert_eq!(TestTypeExtraction::age_type, DataType::Int32);
    assert_eq!(TestTypeExtraction::score_type, DataType::Float64);
    assert_eq!(TestTypeExtraction::is_active_type, DataType::Boolean);
    assert_eq!(TestTypeExtraction::count_type, DataType::UInt32);
}

#[test]
fn test_individual_field_type_constants_polars_columns() {
    // Test that individual type constants are generated correctly for PolarsColumns
    assert_eq!(TestColumnTypeExtraction::product_id_type, DataType::Int32);
    assert_eq!(TestColumnTypeExtraction::product_name_type, DataType::String);
    assert_eq!(TestColumnTypeExtraction::price_type, DataType::Float64);
    assert_eq!(TestColumnTypeExtraction::in_stock_type, DataType::Boolean);
    assert_eq!(TestColumnTypeExtraction::quantity_type, DataType::UInt16);
}

#[test]
fn test_all_types_method_polars_schema() {
    // Test the all_types() method instead of constant array
    let types = TestTypeExtraction::all_types();
    assert_eq!(types.len(), 6);
    assert_eq!(types[0], DataType::Int64);    // id
    assert_eq!(types[1], DataType::String);   // name
    assert_eq!(types[2], DataType::Int32);    // age
    assert_eq!(types[3], DataType::Float64);  // score
    assert_eq!(types[4], DataType::Boolean);  // is_active
    assert_eq!(types[5], DataType::UInt32);   // count
}

#[test]
fn test_all_types_method_polars_columns() {
    // Test the all_types() method instead of constant array
    let types = TestColumnTypeExtraction::all_types();
    assert_eq!(types.len(), 5);
    assert_eq!(types[0], DataType::Int32);    // product_id
    assert_eq!(types[1], DataType::String);   // product_name
    assert_eq!(types[2], DataType::Float64);  // price
    assert_eq!(types[3], DataType::Boolean);  // in_stock
    assert_eq!(types[4], DataType::UInt16);   // quantity
}

#[test]
fn test_all_types_method_schema_duplicate() {
    // Test the all_types() method returns Vec<DataType>
    let types = TestTypeExtraction::all_types();
    assert_eq!(types.len(), 6);
    assert_eq!(types[0], DataType::Int64);    // id
    assert_eq!(types[1], DataType::String);   // name
    assert_eq!(types[2], DataType::Int32);    // age
    assert_eq!(types[3], DataType::Float64);  // score
    assert_eq!(types[4], DataType::Boolean);  // is_active
    assert_eq!(types[5], DataType::UInt32);   // count
}

#[test]
fn test_all_types_method_columns_duplicate() {
    // Test the all_types() method returns Vec<DataType>
    let types = TestColumnTypeExtraction::all_types();
    assert_eq!(types.len(), 5);
    assert_eq!(types[0], DataType::Int32);    // product_id
    assert_eq!(types[1], DataType::String);   // product_name
    assert_eq!(types[2], DataType::Float64);  // price
    assert_eq!(types[3], DataType::Boolean);  // in_stock
    assert_eq!(types[4], DataType::UInt16);   // quantity
}

#[test]
fn test_type_at_method_polars_schema() {
    // Test the type_at(index) method
    assert_eq!(TestTypeExtraction::type_at(0), Some(DataType::Int64));     // id
    assert_eq!(TestTypeExtraction::type_at(1), Some(DataType::String));    // name
    assert_eq!(TestTypeExtraction::type_at(2), Some(DataType::Int32));     // age
    assert_eq!(TestTypeExtraction::type_at(3), Some(DataType::Float64));   // score
    assert_eq!(TestTypeExtraction::type_at(4), Some(DataType::Boolean));   // is_active
    assert_eq!(TestTypeExtraction::type_at(5), Some(DataType::UInt32));    // count
    assert_eq!(TestTypeExtraction::type_at(6), None);                      // out of bounds
}

#[test]
fn test_type_at_method_polars_columns() {
    // Test the type_at(index) method
    assert_eq!(TestColumnTypeExtraction::type_at(0), Some(DataType::Int32));    // product_id
    assert_eq!(TestColumnTypeExtraction::type_at(1), Some(DataType::String));   // product_name
    assert_eq!(TestColumnTypeExtraction::type_at(2), Some(DataType::Float64));  // price
    assert_eq!(TestColumnTypeExtraction::type_at(3), Some(DataType::Boolean));  // in_stock
    assert_eq!(TestColumnTypeExtraction::type_at(4), Some(DataType::UInt16));   // quantity
    assert_eq!(TestColumnTypeExtraction::type_at(5), None);                     // out of bounds
}

#[test]
fn test_type_extraction_with_optional_fields() {
    #[derive(PolarsSchema)]
    #[allow(dead_code, non_upper_case_globals)]
    struct WithOptionalFields {
        required_id: i64,
        optional_name: Option<String>,
        optional_age: Option<i32>,
        required_active: bool,
    }

    // Test that Option<T> maps to the base type T
    assert_eq!(WithOptionalFields::required_id_type, DataType::Int64);
    assert_eq!(WithOptionalFields::optional_name_type, DataType::String);    // Option<String> -> String
    assert_eq!(WithOptionalFields::optional_age_type, DataType::Int32);      // Option<i32> -> Int32
    assert_eq!(WithOptionalFields::required_active_type, DataType::Boolean);

    let types = WithOptionalFields::all_types();
    assert_eq!(types.len(), 4);
    assert_eq!(types[0], DataType::Int64);    // required_id
    assert_eq!(types[1], DataType::String);   // optional_name (maps to String, not Option<String>)
    assert_eq!(types[2], DataType::Int32);    // optional_age (maps to Int32, not Option<i32>)
    assert_eq!(types[3], DataType::Boolean);  // required_active
}

#[test]
fn test_type_extraction_with_empty_dataframe() {
    // Test that type extraction works with the df() method
    let empty_df = TestTypeExtraction::df().unwrap();
    
    // Verify schema types match our extracted types
    let schema = empty_df.schema();
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("name"), Some(&DataType::String));
    assert_eq!(schema.get("age"), Some(&DataType::Int32));
    assert_eq!(schema.get("score"), Some(&DataType::Float64));
    assert_eq!(schema.get("is_active"), Some(&DataType::Boolean));
    assert_eq!(schema.get("count"), Some(&DataType::UInt32));

    // Verify our type extraction methods match the schema
    let extracted_types = TestTypeExtraction::all_types();
    assert_eq!(extracted_types.len(), schema.len());
    
    for (i, (field_name, _)) in schema.iter().enumerate() {
        let expected_type = extracted_types[i].clone();
        let actual_type = schema.get(field_name).unwrap();
        assert_eq!(actual_type, &expected_type);
    }
}

#[test]
fn test_type_consistency_between_methods() {
    // Test that all different methods return consistent type information
    
    // Get types via method
    let method_types = TestTypeExtraction::all_types();
    
    // Individual constants should match positions in all_types()
    assert_eq!(TestTypeExtraction::id_type, method_types[0]);
    assert_eq!(TestTypeExtraction::name_type, method_types[1]);
    assert_eq!(TestTypeExtraction::age_type, method_types[2]);
    assert_eq!(TestTypeExtraction::score_type, method_types[3]);
    assert_eq!(TestTypeExtraction::is_active_type, method_types[4]);
    assert_eq!(TestTypeExtraction::count_type, method_types[5]);

    // type_at() should match all_types()
    for (i, expected_type) in method_types.iter().enumerate() {
        assert_eq!(TestTypeExtraction::type_at(i), Some(expected_type.clone()));
    }
}

#[test]
fn test_real_world_type_usage_example() {
    #[derive(PolarsColumns)]
    #[allow(dead_code, non_upper_case_globals)]
    struct SalesRecord {
        transaction_id: i64,
        customer_name: String,
        amount: f64,
        discount_percent: f32,
        item_count: i32,
        is_refund: bool,
    }

    // Create a DataFrame with correct schema using type information
    let df = df![
        SalesRecord::transaction_id => [1001i64, 1002i64, 1003i64],
        SalesRecord::customer_name => ["Alice", "Bob", "Charlie"],
        SalesRecord::amount => [199.99, 89.50, 299.95],
        SalesRecord::discount_percent => [0.10f32, 0.05f32, 0.15f32],
        SalesRecord::item_count => [2, 1, 3],
        SalesRecord::is_refund => [false, false, true],
    ].unwrap();

    // Verify the DataFrame has the expected types
    let schema = df.schema();
    let expected_types = SalesRecord::all_types();
    
    for (i, (field_name, actual_type)) in schema.iter().enumerate() {
        assert_eq!(actual_type, &expected_types[i], 
                  "Type mismatch for field '{}': expected {:?}, got {:?}",
                  field_name, expected_types[i], actual_type);
    }

    // Use type extraction for validation
    assert_eq!(SalesRecord::transaction_id_type, DataType::Int64);
    assert_eq!(SalesRecord::amount_type, DataType::Float64);
    assert_eq!(SalesRecord::discount_percent_type, DataType::Float32);
    assert_eq!(SalesRecord::is_refund_type, DataType::Boolean);
}

#[test]
fn test_single_field_struct_type_extraction() {
    #[derive(PolarsSchema)]
    #[allow(dead_code, non_upper_case_globals)]
    struct SingleFieldStruct {
        value: String,
    }

    assert_eq!(SingleFieldStruct::value_type, DataType::String);
    let types = SingleFieldStruct::all_types();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0], DataType::String);
    assert_eq!(SingleFieldStruct::all_types(), vec![DataType::String]);
    assert_eq!(SingleFieldStruct::type_at(0), Some(DataType::String));
    assert_eq!(SingleFieldStruct::type_at(1), None);
}