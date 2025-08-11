# polars-tools

Schema validation and column helpers for Polars DataFrames.

This library provides two main features:
1. **Schema Validation** - Type-safe DataFrame validation using derive macros
2. **Column Helpers** - Generate column constants and expressions from struct definitions

## Features

- ✅ **Schema Validation** - Validate DataFrame schemas with `#[derive(PolarsSchema)]`
- ✅ **Column Helpers** - Generate column constants and expressions with `#[derive(PolarsColumns)]`
- ✅ **Type Safety** - Compile-time schema generation from Rust structs
- ✅ **Comprehensive Type Support** - All primitive types (i8-u64, f32/f64, bool, String)
- ✅ **Flexible Validation** - Both standard (allows extra columns) and strict validation modes
- ✅ **Error Handling** - Clear error messages for validation failures

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
polars-tools = { path = "path/to/polars-tools" }
polars = "0.43"
serde = { version = "1.0", features = ["derive"] }
```

## Quick Start

```rust
use polars_tools::*;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, PolarsSchema)]
struct User {
    id: i64,
    name: String,
    email: String,
    age: i32,
    is_active: bool,
}

fn main() -> Result<()> {
    let df = df![
        "id" => [1i64, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "email" => ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "age" => [25, 30, 35],
        "is_active" => [true, false, true],
    ]?;
    
    // Schema validation
    User::validate(&df)?;
    User::validate_strict(&df)?;
    
    // Column constants
    println!("ID column: {}", User::id); // "id"
    println!("Name column: {}", User::name); // "name"
    
    // Column expressions for data operations
    let active_users = df.clone()
        .lazy()
        .filter(User::expr.is_active().eq(lit(true)))
        .select([
            User::expr.name(),
            User::expr.email(),
            User::expr.age(),
        ])
        .collect()?;
    
    println!("Active users: {}", active_users);
    Ok(())
}
```

## Schema Validation

### Basic Validation

```rust
#[derive(PolarsSchema)]
struct Product {
    id: i32,
    name: String,
    price: f64,
    in_stock: bool,
}

// Validate DataFrame matches schema
Product::validate(&df)?;

// Strict validation (no extra columns allowed)
Product::validate_strict(&df)?;
```

### Validation Modes

- **Standard Validation** (`validate()`): Ensures all required columns exist with correct types. Allows extra columns.
- **Strict Validation** (`validate_strict()`): Ensures DataFrame has exactly the columns defined in the schema.

### Supported Types

| Rust Type | Polars DataType |
|-----------|----------------|
| `i8`, `i16`, `i32`, `i64` | `Int8`, `Int16`, `Int32`, `Int64` |
| `u8`, `u16`, `u32`, `u64` | `UInt8`, `UInt16`, `UInt32`, `UInt64` |
| `f32`, `f64` | `Float32`, `Float64` |
| `bool` | `Boolean` |
| `String` | `String` |

### Temporal Types (with `chrono` feature)

Enable the `chrono` feature to support temporal types:

```toml
[dependencies]
polars-tools = { path = "path/to/polars-tools", features = ["chrono"] }
chrono = "0.4"
```

| Rust Type (with chrono) | Polars DataType |
|-------------------------|----------------|
| `chrono::NaiveDate` | `Date` |
| `chrono::NaiveDateTime` | `Datetime` |
| `chrono::NaiveTime` | `Time` |
| `chrono::DateTime<Utc>` | `Datetime` (with UTC timezone) |

## Column Helpers

### Column Constants

Access column names as compile-time constants:

```rust
#[derive(PolarsColumns)]
struct Sales {
    product_id: i32,
    quantity: i32,
    revenue: f64,
}

// Use constants for column selection
let df = df.select([Sales::product_id, Sales::revenue])?;

// Or select all columns at once
let df = df.select(Sales::all_columns())?;
```

### Column Expressions

Generate Polars expressions for data operations:

```rust
#[derive(PolarsColumns)]
struct Transaction {
    amount: f64,
    status: String,
    created_at: String,
}

// Filter and aggregate using generated expressions
let summary = df.lazy()
    .filter(Transaction::expr.status().eq(lit("completed")))
    .select([
        Transaction::expr.amount().sum().alias("total_amount"),
        Transaction::expr.amount().mean().alias("avg_amount"),
        Transaction::expr.amount().count().alias("transaction_count"),
    ])
    .collect()?;
```

### Select All Columns

The library provides multiple ways to select all columns from a schema:

```rust
#[derive(PolarsSchema)]
struct User {
    id: i64,
    name: String,
    email: String,
}

// Method 1: Using all_columns() for DataFrame.select()
let clean_data = messy_df.select(User::all_columns())?;

// Method 2: Using all_columns() function for individual access
println!("All columns: {:?}", User::all_columns()); // ["id", "name", "email"]

// Method 3: Using all_cols() for lazy operations  
let result = df.lazy()
    .select(User::all_cols()) // Returns Vec<Expr>
    .filter(User::expr.id().gt(lit(100)))
    .collect()?;

// Method 4: Using expr.all_cols() for consistent API
let result = df.lazy()
    .select(User::expr.all_cols()) // Consistent with expr.field_name() pattern
    .filter(User::expr.id().gt(lit(100)))
    .collect()?;
```

**Use Cases:**
- **Data Cleaning**: Remove unwanted columns from messy datasets
- **Schema Enforcement**: Ensure DataFrames contain only expected columns  
- **Pipeline Operations**: Select schema columns in lazy evaluation chains

### Available Helper Methods

| Method | Description |
|--------|-------------|
| `MyStruct::field_name` | Column name as `&'static str` |
| `MyStruct::field_name_type` | Column type as `DataType` constant |
| `MyStruct::expr.field_name()` | Column expression (`Expr`) |
| `MyStruct::expr.all_cols()` | All column expressions as `Vec<Expr>` for lazy operations |
| `MyStruct::all_columns()` | All column names as `Vec<&'static str>` for `df.select()` |
| `MyStruct::all_types()` | All column types as `Vec<DataType>` |
| `MyStruct::all_cols()` | All column expressions as `Vec<Expr>` for lazy operations |
| `MyStruct::column_names()` | All column names as `Vec<&'static str>` |
| `MyStruct::column_name_at(index)` | Column name at index |
| `MyStruct::type_at(index)` | Column type at index |
| `MyStruct::col_expr(name)` | Get expression by field name |
| `MyStruct::df()` | Create empty DataFrame with correct schema |

## Type Extraction

Extract Polars DataTypes from struct definitions at compile time:

```rust
#[derive(PolarsSchema)]
struct UserData {
    user_id: i64,
    username: String,
    age: i32,
    score: f64,
    is_active: bool,
}

// Individual field type constants
assert_eq!(UserData::user_id_type, DataType::Int64);
assert_eq!(UserData::username_type, DataType::String);
assert_eq!(UserData::age_type, DataType::Int32);
assert_eq!(UserData::score_type, DataType::Float64);
assert_eq!(UserData::is_active_type, DataType::Boolean);

// All types as Vec from function
let all_types = UserData::all_types();
assert_eq!(all_types.len(), 5);
assert_eq!(all_types[0], DataType::Int64);    // user_id
assert_eq!(all_types[1], DataType::String);   // username
assert_eq!(all_types[2], DataType::Int32);    // age
assert_eq!(all_types[3], DataType::Float64);  // score
assert_eq!(all_types[4], DataType::Boolean);  // is_active

// Get all types as Vec for dynamic operations
let types_vec = UserData::all_types();
assert_eq!(types_vec, vec![
    DataType::Int64,    // user_id
    DataType::String,   // username  
    DataType::Int32,    // age
    DataType::Float64,  // score
    DataType::Boolean,  // is_active
]);

// Get type at specific index
assert_eq!(UserData::type_at(0), Some(DataType::Int64));    // user_id
assert_eq!(UserData::type_at(1), Some(DataType::String));   // username
assert_eq!(UserData::type_at(5), None);                     // out of bounds
```

### Type Extraction Use Cases

**Schema Validation and Comparison**
```rust
// Compare DataFrame schema with expected types
let df = create_user_dataframe()?;
let expected_types = UserData::all_types();

for (i, (field_name, actual_type)) in df.schema().iter().enumerate() {
    assert_eq!(actual_type, &expected_types[i], 
              "Type mismatch for field '{}': expected {:?}, got {:?}",
              field_name, expected_types[i], actual_type);
}
```

**Dynamic Schema Construction**
```rust
// Build Polars schema programmatically
use polars::prelude::Schema;

let field_names = UserData::all_columns();
let field_types = UserData::all_types();
let schema: Schema = field_names
    .into_iter()
    .zip(field_types.into_iter())
    .collect();
```

**Type-Safe DataFrame Creation**
```rust
// Ensure DataFrame columns have correct types
let df = df![
    UserData::user_id => [1i64, 2, 3],           // Must be i64
    UserData::username => ["alice", "bob", "charlie"],  // Must be String
    UserData::age => [25i32, 30, 35],            // Must be i32 
    UserData::score => [85.5f64, 92.0, 78.5],   // Must be f64
    UserData::is_active => [true, false, true],   // Must be bool
]?;

// Verify types match expectations
UserData::validate(&df)?;
```

**Option Type Handling**
```rust
#[derive(PolarsSchema)]
struct OptionalFields {
    required_id: i64,
    optional_name: Option<String>,    // Maps to String in Polars
    optional_age: Option<i32>,        // Maps to Int32 in Polars
}

// Option<T> fields map to base type T in DataType
assert_eq!(OptionalFields::required_id_type, DataType::Int64);
assert_eq!(OptionalFields::optional_name_type, DataType::String);  // Not Option<String>
assert_eq!(OptionalFields::optional_age_type, DataType::Int32);    // Not Option<i32>
```

**Complete API Pattern**

The library provides a consistent three-tier API:

```rust
#[derive(PolarsSchema)]  
struct MyData {
    field1: i64,
    field2: String,
}

// Names (for column selection)
MyData::field1              // &str
MyData::all_columns()       // Vec<&str>

// Expressions (for lazy operations)  
MyData::expr.field1()       // Expr
MyData::expr.all_cols()     // Vec<Expr>
MyData::all_cols()          // Vec<Expr>

// Types (for schema operations)
MyData::field1_type         // DataType
MyData::all_types()         // Vec<DataType>
MyData::type_at(index)      // Option<DataType>
```

## Enum Validation

Support Rust enums as field types with automatic validation and type mapping:

```rust
use polars_tools::*;

// Define your enum
#[derive(Debug, Clone, PartialEq)]
enum Status {
    Active,
    Inactive,
    Pending,
}

// Implement ValidatableEnum trait for validation
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

// Use enum directly in struct definition
#[derive(PolarsSchema)]
struct User {
    id: i64,
    name: String,
    status: Status,  // Enum field - stored as String in Polars
    score: f64,
}
```

### Automatic Type Mapping

Enum fields are automatically mapped to `DataType::String` in Polars:

```rust
// Type extraction works with enums
assert_eq!(User::id_type, DataType::Int64);
assert_eq!(User::name_type, DataType::String);
assert_eq!(User::status_type, DataType::String);  // Enum mapped to String
assert_eq!(User::score_type, DataType::Float64);

// Empty DataFrame creation includes enum fields as strings
let empty_df = User::df()?;
let schema = empty_df.schema();
assert_eq!(schema.get("status"), Some(&DataType::String));
```

### Enum Validation

Validate DataFrame enum values using the `ValidatableEnum` trait:

```rust
// Create DataFrame with valid enum values
let valid_df = df![
    "id" => [1i64, 2i64, 3i64],
    "name" => ["Alice", "Bob", "Charlie"],
    "status" => ["Active", "Inactive", "Pending"],  // Valid enum values
    "score" => [85.5f64, 92.0, 78.5],
]?;

// Basic schema validation passes
User::validate(&valid_df)?;

// Manual enum validation
let status_col = valid_df.column("status")?;
let string_values = status_col.str()?;

for value_opt in string_values.into_iter() {
    if let Some(value) = value_opt {
        assert!(Status::is_valid(value));  // All values are valid
    }
}
```

### Error Handling for Invalid Enum Values

```rust
// Create DataFrame with invalid enum value
let invalid_df = df![
    "id" => [1i64, 2i64],
    "name" => ["Alice", "Bob"],
    "status" => ["Active", "InvalidStatus"],  // Invalid enum value
    "score" => [85.5f64, 92.0],
]?;

// Detect invalid enum values
let status_col = invalid_df.column("status")?;
for value_opt in status_col.str()?.into_iter() {
    if let Some(value) = value_opt {
        if !Status::is_valid(value) {
            // Handle invalid enum value
            let error = Status::from_str(value);
            match error {
                Err(ValidationError::InvalidEnumValue { field, value, valid_values }) => {
                    println!("Invalid {} value '{}'. Valid values: {:?}", 
                            field, value, valid_values);
                }
                _ => {}
            }
        }
    }
}
```

### Use Cases for Enum Validation

**Data Quality Assurance**
```rust
// Ensure all status values are valid before processing
fn process_user_data(df: DataFrame) -> Result<DataFrame> {
    // First validate schema
    User::validate(&df)?;
    
    // Then validate enum values
    let status_col = df.column("status")?;
    for value_opt in status_col.str()?.into_iter() {
        if let Some(value) = value_opt {
            Status::from_str(value)?;  // Will error on invalid values
        }
    }
    
    // Process with confidence that all enum values are valid
    Ok(df)
}
```

**Type-Safe Conversions**
```rust
// Convert DataFrame enum strings back to Rust enums
fn extract_statuses(df: &DataFrame) -> Result<Vec<Status>> {
    let status_col = df.column("status")?;
    let mut statuses = Vec::new();
    
    for value_opt in status_col.str()?.into_iter() {
        if let Some(value) = value_opt {
            statuses.push(Status::from_str(value)?);
        }
    }
    
    Ok(statuses)
}
```

**Enum Operations with Polars**
```rust
// Filter by enum values
let active_users = df.lazy()
    .filter(User::expr.status().eq(lit("Active")))  // Use string representation
    .collect()?;

// Group by enum values
let status_counts = df.lazy()
    .group_by([User::expr.status()])
    .agg([
        User::expr.id().count().alias("count"),
        User::expr.score().mean().alias("avg_score"),
    ])
    .collect()?;
```

### ValidatableEnum Trait

The `ValidatableEnum` trait provides the interface for enum validation:

```rust
pub trait ValidatableEnum {
    /// Get all valid string representations of this enum
    fn valid_values() -> Vec<&'static str>;
    
    /// Check if a string value is valid for this enum
    fn is_valid(value: &str) -> bool {
        Self::valid_values().contains(&value)
    }
    
    /// Convert string to enum if valid, otherwise return error
    fn from_str(value: &str) -> Result<Self> where Self: Sized;
    
    /// Convert enum to string representation
    fn to_str(&self) -> &'static str;
}
```

**Benefits:**
- **Type Safety**: Rust enum types in application code
- **Polars Compatibility**: Stored as strings, works with all Polars operations
- **Validation**: Runtime validation of enum values in DataFrames
- **Performance**: Zero-cost abstractions, validation only when needed
- **Integration**: Works seamlessly with existing schema validation

## Empty DataFrame Helper

Create empty DataFrames with the correct schema for data initialization and pipeline operations:

```rust
#[derive(PolarsSchema)]
struct Customer {
    id: i64,
    name: String,
    email: String,
    age: i32,
    is_active: bool,
}

// Create an empty DataFrame with the correct schema
let empty_df = Customer::df()?;

// The DataFrame has 0 rows but correct column types and names
assert_eq!(empty_df.height(), 0);  // 0 rows
assert_eq!(empty_df.width(), 5);   // 5 columns

// Schema matches the struct definition
let schema = empty_df.schema();
assert_eq!(schema.get("id"), Some(&DataType::Int64));
assert_eq!(schema.get("name"), Some(&DataType::String));
assert_eq!(schema.get("age"), Some(&DataType::Int32));
```

### Use Cases for Empty DataFrames

**Data Pipeline Initialization**
```rust
// Start with empty DataFrame, then append batches
let mut results = Customer::df()?;

for batch in data_batches {
    let batch_df = process_batch(batch)?;
    results = results.vstack(&batch_df)?;
}
```

**Schema Templates**
```rust
// Use empty DataFrame as template for data operations
let template = Customer::df()?;
let processed_data = input_data
    .lazy()
    .select(template.get_column_names())  // Ensure same columns
    .collect()?;

// Verify compatibility
assert_eq!(template.schema(), processed_data.schema());
```

**Optional Field Support**
```rust
#[derive(PolarsSchema)]
struct UserProfile {
    user_id: i64,
    name: String,
    phone: Option<String>,      // Optional fields supported
    birth_year: Option<i32>,    // Maps to base type in schema
}

let empty_df = UserProfile::df()?;
// optional_phone: String, birth_year: Int32 (not Option types in Polars)
```

## Real-World Examples

### Data Pipeline with Validation

```rust
#[derive(PolarsSchema)]
struct CustomerData {
    customer_id: i64,
    name: String,
    email: String,
    signup_date: String,
    total_orders: i32,
    lifetime_value: f64,
}

fn process_customer_data(df: DataFrame) -> Result<DataFrame> {
    // Validate incoming data
    CustomerData::validate(&df)?;
    
    // Process using column helpers
    let processed = df.lazy()
        .filter(CustomerData::expr.total_orders().gt(lit(0)))
        .with_columns([
            CustomerData::expr.lifetime_value()
                .gt(lit(1000.0))
                .alias("high_value_customer")
        ])
        .select([
            CustomerData::expr.customer_id(),
            CustomerData::expr.name(),
            CustomerData::expr.lifetime_value(),
            col("high_value_customer"),
        ])
        .collect()?;
    
    Ok(processed)
}
```

### Multiple Schema Validation

```rust
#[derive(PolarsSchema)]
struct OrderHeader {
    order_id: i64,
    customer_id: i64,
    order_date: String,
    total_amount: f64,
}

#[derive(PolarsSchema)]
struct OrderLine {
    line_id: i64,
    order_id: i64,
    product_id: i32,
    quantity: i32,
    unit_price: f64,
}

fn validate_order_data(
    headers_df: &DataFrame,
    lines_df: &DataFrame
) -> Result<()> {
    OrderHeader::validate_strict(headers_df)?;
    OrderLine::validate_strict(lines_df)?;
    println!("All order data validated successfully!");
    Ok(())
}
```

## Error Handling

The library provides clear error messages for validation failures:

```rust
// Missing column error
Error: Missing required column: customer_id

// Type mismatch error  
Error: Column 'age' has type String, expected Int32

// Extra columns in strict mode
Error: Column count mismatch. Expected: {"id", "name"}, Found: {"id", "name", "extra"}
```

## Testing

Run the test suite:

```bash
cargo test
```

The library includes comprehensive tests covering:
- Schema validation (85+ tests total)
- Column helper functionality  
- Error handling scenarios
- Real-world integration examples
- Performance with large datasets

## License

MIT
