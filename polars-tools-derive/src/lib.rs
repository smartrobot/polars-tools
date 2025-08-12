//! # Polars Columns Derive
//!
//! This crate provides the `PolarsColumns` derive macro for generating column access helpers
//! for Polars DataFrames.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Helper function to determine if a type is likely an enum (not a known primitive)
fn is_likely_enum_type(type_str: &str) -> bool {
    // Known primitive types that should NOT be treated as enums
    let primitives = [
        // Integers
        "i8", "i16", "i32", "i64", "i128", "isize",
        "u8", "u16", "u32", "u64", "u128", "usize",
        // Floats  
        "f32", "f64",
        // Other primitives
        "bool", "String", "str", "&str",
        // Option wrapped primitives
        "Option < i8 >", "Option < i16 >", "Option < i32 >", "Option < i64 >",
        "Option < u8 >", "Option < u16 >", "Option < u32 >", "Option < u64 >",
        "Option < f32 >", "Option < f64 >", "Option < bool >", "Option < String >",
        // Chrono types
        "chrono :: NaiveDate", "chrono :: NaiveDateTime", "chrono :: NaiveTime",
        "chrono :: DateTime < chrono :: Utc >",
    ];
    
    // Check if it's a known primitive
    if primitives.contains(&type_str) {
        return false;
    }
    
    // Check if it's an Option<SomeCustomType> - extract inner type
    if type_str.contains("Option") && type_str.contains("<") && type_str.contains(">") {
        let start = type_str.find('<').unwrap_or(0) + 1;
        let end = type_str.rfind('>').unwrap_or(type_str.len());
        let inner = type_str[start..end].trim();
        // If inner type is not primitive, then it's likely an enum
        return !primitives.iter().any(|p| p == &inner);
    }
    
    // If it's not a primitive and not an option of a primitive, likely enum
    true
}

/// Derive macro for generating Polars column access helpers.
///
/// This macro generates:
/// - `StructName::field_name` constants for column names
/// - `StructName::expr.field_name()` methods for column expressions
/// - Implementations of `PolarsColumns` and `PolarsColumnsExt` traits
#[proc_macro_derive(PolarsColumns)]
pub fn polars_columns_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(data_struct) => match data_struct.fields {
            Fields::Named(fields_named) => fields_named.named,
            _ => panic!("PolarsColumns only supports structs with named fields"),
        },
        _ => panic!("PolarsColumns only supports structs"),
    };

    let field_names: Vec<_> = fields.iter().map(|f| &f.ident).collect();
    let field_name_strs: Vec<_> = field_names
        .iter()
        .map(|f| f.as_ref().unwrap().to_string())
        .collect();
    let _field_names_count = field_names.len();

    // Collect enum field information for validation generation
    let _enum_fields: Vec<_> = fields.iter()
        .filter_map(|f| {
            let field_type = &f.ty;
            let type_str = quote!(#field_type).to_string();
            let field_name = f.ident.as_ref().unwrap();
            
            if is_likely_enum_type(&type_str) {
                Some((field_name.clone(), field_type.clone()))
            } else {
                None
            }
        })
        .collect();

    // Generate polars data types for empty DataFrame creation
    let polars_types: Vec<_> = fields.iter().map(|f| {
        let field_type = &f.ty;
        let type_str = quote!(#field_type).to_string();
        
        // If it's likely an enum, map it to String
        if is_likely_enum_type(&type_str) {
            return quote!(polars::prelude::DataType::String);
        }
        
        match type_str.as_str() {
            // Handle Option<T> types - exact match for all supported types
            "Option < i8 >" => quote!(polars::prelude::DataType::Int8),
            "Option < i16 >" => quote!(polars::prelude::DataType::Int16),
            "Option < i32 >" => quote!(polars::prelude::DataType::Int32),
            "Option < i64 >" => quote!(polars::prelude::DataType::Int64),
            "Option < u8 >" => quote!(polars::prelude::DataType::UInt8),
            "Option < u16 >" => quote!(polars::prelude::DataType::UInt16),
            "Option < u32 >" => quote!(polars::prelude::DataType::UInt32),
            "Option < u64 >" => quote!(polars::prelude::DataType::UInt64),
            "Option < f32 >" => quote!(polars::prelude::DataType::Float32),
            "Option < f64 >" => quote!(polars::prelude::DataType::Float64),
            "Option < bool >" => quote!(polars::prelude::DataType::Boolean),
            "Option < String >" => quote!(polars::prelude::DataType::String),
            // Signed integers
            "i8" => quote!(polars::prelude::DataType::Int8),
            "i16" => quote!(polars::prelude::DataType::Int16),
            "i32" => quote!(polars::prelude::DataType::Int32),
            "i64" => quote!(polars::prelude::DataType::Int64),
            // Unsigned integers
            "u8" => quote!(polars::prelude::DataType::UInt8),
            "u16" => quote!(polars::prelude::DataType::UInt16),
            "u32" => quote!(polars::prelude::DataType::UInt32),
            "u64" => quote!(polars::prelude::DataType::UInt64),
            // Floats
            "f32" => quote!(polars::prelude::DataType::Float32),
            "f64" => quote!(polars::prelude::DataType::Float64),
            // Boolean and String
            "bool" => quote!(polars::prelude::DataType::Boolean),
            "String" => quote!(polars::prelude::DataType::String),
            // Handle Option<T> types - fallback pattern
            s if s.contains("Option") && s.contains("<") && s.contains(">") => {
                // Extract everything between < and >
                let start = s.find('<').unwrap_or(0) + 1;
                let end = s.rfind('>').unwrap_or(s.len());
                let inner = s[start..end].trim();
                match inner {
                    "i8" => quote!(polars::prelude::DataType::Int8),
                    "i16" => quote!(polars::prelude::DataType::Int16),
                    "i32" => quote!(polars::prelude::DataType::Int32),
                    "i64" => quote!(polars::prelude::DataType::Int64),
                    "u8" => quote!(polars::prelude::DataType::UInt8),
                    "u16" => quote!(polars::prelude::DataType::UInt16),
                    "u32" => quote!(polars::prelude::DataType::UInt32),
                    "u64" => quote!(polars::prelude::DataType::UInt64),
                    "f32" => quote!(polars::prelude::DataType::Float32),
                    "f64" => quote!(polars::prelude::DataType::Float64),
                    "bool" => quote!(polars::prelude::DataType::Boolean),
                    "String" => quote!(polars::prelude::DataType::String),
                    _ => quote!(polars::prelude::DataType::String),
                }
            }
            // Chrono temporal types
            "chrono :: NaiveDate" => quote!(polars::prelude::DataType::Date),
            "chrono :: NaiveDateTime" => quote!(polars::prelude::DataType::Datetime(
                polars::prelude::TimeUnit::Microseconds,
                None
            )),
            "chrono :: NaiveTime" => quote!(polars::prelude::DataType::Time),
            "chrono :: DateTime < chrono :: Utc >" => quote!(polars::prelude::DataType::Datetime(
                polars::prelude::TimeUnit::Microseconds,
                Some("UTC".into())
            )),
            _ => quote!(polars::prelude::DataType::String), // Default fallback
        }
    }).collect();

    let const_impls = fields.iter().map(|f| {
        let field_name = &f.ident;
        let field_name_str = field_name.as_ref().unwrap().to_string();
        quote! {
            #[allow(non_upper_case_globals)]
            pub const #field_name: &'static str = #field_name_str;
        }
    });

    let type_const_impls = fields.iter().zip(polars_types.clone()).map(|(f, polars_type)| {
        let field_name = &f.ident;
        let type_const_name = syn::Ident::new(
            &format!("{}_type", field_name.as_ref().unwrap()),
            proc_macro2::Span::call_site(),
        );
        quote! {
            #[allow(non_upper_case_globals)]
            pub const #type_const_name: polars::prelude::DataType = #polars_type;
        }
    });

    let col_func_impls = fields.iter().map(|f| {
        let field_name = &f.ident;
        let func_name = syn::Ident::new(
            &format!("{}_col", field_name.as_ref().unwrap()),
            proc_macro2::Span::call_site(),
        );
        let field_name_str = field_name.as_ref().unwrap().to_string();
        quote! {
            pub fn #func_name() -> polars::prelude::Expr {
                polars::prelude::col(#field_name_str)
            }
        }
    });

    // Generate expr helper struct name
    let expr_struct_name =
        syn::Ident::new(&format!("ExprFor{}", name), proc_macro2::Span::call_site());

    let expanded = quote! {
        impl #name {
            #(#const_impls)*
            #(#type_const_impls)*
            #(#col_func_impls)*

            /// Get all column names as Vec<&str> for use with df.select()
            pub fn all_columns() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }

            /// Get all column types as Vec<DataType>
            pub fn all_types() -> Vec<polars::prelude::DataType> {
                vec![#(#polars_types),*]
            }

            /// Get column type at specific index
            pub fn type_at(index: usize) -> Option<polars::prelude::DataType> {
                let types = [#(#polars_types),*];
                types.get(index).cloned()
            }

            /// Get all column names as expressions for lazy operations
            pub fn all_cols() -> Vec<polars::prelude::Expr> {
                vec![#(polars::prelude::col(#field_name_strs)),*]
            }

            /// Create an empty DataFrame with the correct schema
            pub fn df() -> std::result::Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
                let columns = vec![
                    #(
                        polars::prelude::Column::new(#field_name_strs.into(), polars::prelude::Series::new_empty(#field_name_strs.into(), &#polars_types))
                    ),*
                ];
                polars::prelude::DataFrame::new(columns)
            }
        }

        pub struct #expr_struct_name;

        impl #expr_struct_name {
            #(
                pub fn #field_names(&self) -> polars::prelude::Expr {
                    polars::prelude::col(#field_name_strs)
                }
            )*
            
            /// Get all column expressions as Vec<Expr> for lazy operations
            pub fn all_cols(&self) -> Vec<polars::prelude::Expr> {
                vec![#(polars::prelude::col(#field_name_strs)),*]
            }
        }

        impl #name {
            pub const expr: #expr_struct_name = #expr_struct_name;
        }

        // Implement the trait methods directly without trait bounds to avoid import issues
        impl #name {
            /// Implementation of PolarsColumnsExt::columns() 
            pub fn columns() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }

            /// Implementation of PolarsColumns::column_names()
            pub fn column_names() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }

            /// Implementation of PolarsColumns::column_name_at()
            pub fn column_name_at(index: usize) -> Option<&'static str> {
                let names = [#(#field_name_strs),*];
                names.get(index).copied()
            }

            /// Implementation of PolarsColumns::col_expr()
            pub fn col_expr(field_name: &str) -> Option<polars::prelude::Expr> {
                match field_name {
                    #(#field_name_strs => Some(polars::prelude::col(#field_name_strs)),)*
                    _ => None,
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for generating schema validation using a struct definition
#[proc_macro_derive(PolarsSchema)]
pub fn polars_schema_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let fields = match input.data {
        Data::Struct(data_struct) => match data_struct.fields {
            Fields::Named(fields_named) => fields_named.named,
            _ => panic!("PolarsSchema only supports structs with named fields"),
        },
        _ => panic!("PolarsSchema only supports structs"),
    };

    // Collect enum field information for validation generation
    let _enum_fields_for_schema: Vec<_> = fields.iter()
        .filter_map(|f| {
            let field_type = &f.ty;
            let type_str = quote!(#field_type).to_string();
            let field_name = f.ident.as_ref().unwrap();
            
            if is_likely_enum_type(&type_str) {
                Some((field_name.clone(), field_type.clone()))
            } else {
                None
            }
        })
        .collect();

    // Collect the polars types for DataFrame creation
    let polars_types_for_df: Vec<_> = fields
        .iter()
        .map(|f| {
            let field_type = &f.ty;
            let type_str = quote!(#field_type).to_string();
            
            // If it's likely an enum, map it to String
            if is_likely_enum_type(&type_str) {
                return quote!(polars::prelude::DataType::String);
            }
            
            match type_str.as_str() {
                // Handle Option<T> types - exact match for all supported types
                "Option < i8 >" => quote!(polars::prelude::DataType::Int8),
                "Option < i16 >" => quote!(polars::prelude::DataType::Int16),
                "Option < i32 >" => quote!(polars::prelude::DataType::Int32),
                "Option < i64 >" => quote!(polars::prelude::DataType::Int64),
                "Option < u8 >" => quote!(polars::prelude::DataType::UInt8),
                "Option < u16 >" => quote!(polars::prelude::DataType::UInt16),
                "Option < u32 >" => quote!(polars::prelude::DataType::UInt32),
                "Option < u64 >" => quote!(polars::prelude::DataType::UInt64),
                "Option < f32 >" => quote!(polars::prelude::DataType::Float32),
                "Option < f64 >" => quote!(polars::prelude::DataType::Float64),
                "Option < bool >" => quote!(polars::prelude::DataType::Boolean),
                "Option < String >" => quote!(polars::prelude::DataType::String),
                // Signed integers
                "i8" => quote!(polars::prelude::DataType::Int8),
                "i16" => quote!(polars::prelude::DataType::Int16),
                "i32" => quote!(polars::prelude::DataType::Int32),
                "i64" => quote!(polars::prelude::DataType::Int64),
                // Unsigned integers
                "u8" => quote!(polars::prelude::DataType::UInt8),
                "u16" => quote!(polars::prelude::DataType::UInt16),
                "u32" => quote!(polars::prelude::DataType::UInt32),
                "u64" => quote!(polars::prelude::DataType::UInt64),
                // Floats
                "f32" => quote!(polars::prelude::DataType::Float32),
                "f64" => quote!(polars::prelude::DataType::Float64),
                // Boolean and String
                "bool" => quote!(polars::prelude::DataType::Boolean),
                "String" => quote!(polars::prelude::DataType::String),
                // Handle Option<T> types
                s if s.starts_with("Option <") || s.starts_with("std :: option :: Option <") => {
                    let inner = if s.starts_with("Option <") {
                        s.trim_start_matches("Option <").trim_end_matches(">")
                    } else {
                        s.trim_start_matches("std :: option :: Option <")
                            .trim_end_matches(">")
                    };
                    match inner {
                        "i8" => quote!(polars::prelude::DataType::Int8),
                        "i16" => quote!(polars::prelude::DataType::Int16),
                        "i32" => quote!(polars::prelude::DataType::Int32),
                        "i64" => quote!(polars::prelude::DataType::Int64),
                        "u8" => quote!(polars::prelude::DataType::UInt8),
                        "u16" => quote!(polars::prelude::DataType::UInt16),
                        "u32" => quote!(polars::prelude::DataType::UInt32),
                        "u64" => quote!(polars::prelude::DataType::UInt64),
                        "f32" => quote!(polars::prelude::DataType::Float32),
                        "f64" => quote!(polars::prelude::DataType::Float64),
                        "bool" => quote!(polars::prelude::DataType::Boolean),
                        "String" => quote!(polars::prelude::DataType::String),
                        _ => quote!(polars::prelude::DataType::String),
                    }
                }
                // Chrono temporal types
                "chrono :: NaiveDate" => quote!(polars::prelude::DataType::Date),
                "chrono :: NaiveDateTime" => quote!(polars::prelude::DataType::Datetime(
                    polars::prelude::TimeUnit::Microseconds,
                    None
                )),
                "chrono :: NaiveTime" => quote!(polars::prelude::DataType::Time),
                "chrono :: DateTime < chrono :: Utc >" => {
                    quote!(polars::prelude::DataType::Datetime(
                        polars::prelude::TimeUnit::Microseconds,
                        Some("UTC".into())
                    ))
                }
                _ => quote!(polars::prelude::DataType::String), // Default fallback
            }
        })
        .collect();

    let field_validations =
        fields
            .iter()
            .zip(polars_types_for_df.iter())
            .map(|(f, _polars_type)| {
                let field_name = f.ident.as_ref().unwrap().to_string();
                let field_type = &f.ty;

                // Map Rust types to Polars DataTypes
                let type_str = quote!(#field_type).to_string();
                let polars_type = match type_str.as_str() {
                    // Signed integers
                    "i8" => quote!(polars::prelude::DataType::Int8),
                    "i16" => quote!(polars::prelude::DataType::Int16),
                    "i32" => quote!(polars::prelude::DataType::Int32),
                    "i64" => quote!(polars::prelude::DataType::Int64),
                    // Unsigned integers
                    "u8" => quote!(polars::prelude::DataType::UInt8),
                    "u16" => quote!(polars::prelude::DataType::UInt16),
                    "u32" => quote!(polars::prelude::DataType::UInt32),
                    "u64" => quote!(polars::prelude::DataType::UInt64),
                    // Floats
                    "f32" => quote!(polars::prelude::DataType::Float32),
                    "f64" => quote!(polars::prelude::DataType::Float64),
                    // Handle Option<T> types - simplified exact match FIRST to ensure priority
                    "Option < i32 >" => quote!(polars::prelude::DataType::Int32),
                    "Option < String >" => quote!(polars::prelude::DataType::String),
                    // Boolean and String
                    "bool" => quote!(polars::prelude::DataType::Boolean),
                    "String" => quote!(polars::prelude::DataType::String),
                    // Handle Option<T> types (nullable columns) - fallback pattern
                    s if s.contains("Option") && s.contains("<") && s.contains(">") => {
                        // Extract everything between < and >
                        let start = s.find('<').unwrap_or(0) + 1;
                        let end = s.rfind('>').unwrap_or(s.len());
                        let inner = s[start..end].trim();
                        match inner {
                            "i8" => quote!(polars::prelude::DataType::Int8),
                            "i16" => quote!(polars::prelude::DataType::Int16),
                            "i32" => quote!(polars::prelude::DataType::Int32),
                            "i64" => quote!(polars::prelude::DataType::Int64),
                            "u8" => quote!(polars::prelude::DataType::UInt8),
                            "u16" => quote!(polars::prelude::DataType::UInt16),
                            "u32" => quote!(polars::prelude::DataType::UInt32),
                            "u64" => quote!(polars::prelude::DataType::UInt64),
                            "f32" => quote!(polars::prelude::DataType::Float32),
                            "f64" => quote!(polars::prelude::DataType::Float64),
                            "bool" => quote!(polars::prelude::DataType::Boolean),
                            "String" => quote!(polars::prelude::DataType::String),
                            _ => quote!(polars::prelude::DataType::String),
                        }
                    }
                    // Chrono temporal types
                    "chrono :: NaiveDate" => quote!(polars::prelude::DataType::Date),
                    "chrono :: NaiveDateTime" => quote!(polars::prelude::DataType::Datetime(
                        polars::prelude::TimeUnit::Microseconds,
                        None
                    )),
                    "chrono :: NaiveTime" => quote!(polars::prelude::DataType::Time),
                    "chrono :: DateTime < chrono :: Utc >" => {
                        quote!(polars::prelude::DataType::Datetime(
                            polars::prelude::TimeUnit::Microseconds,
                            Some("UTC".into())
                        ))
                    }
                    _ => quote!(polars::prelude::DataType::String), // Default fallback
                };

                quote! {
                    let col = df.column(#field_name)
                        .map_err(|_| ::polars_tools::ValidationError::MissingColumn {
                            column_name: #field_name.to_string()
                        })?;

                    if col.dtype() != &#polars_type {
                        return Err(::polars_tools::ValidationError::TypeMismatch {
                            column_name: #field_name.to_string(),
                            actual_type: format!("{:?}", col.dtype()),
                            expected_type: format!("{:?}", #polars_type),
                        });
                    }
                }
            });

    let field_names: Vec<_> = fields.iter().map(|f| &f.ident).collect();
    let field_name_strs: Vec<_> = fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap().to_string())
        .collect();
    let _field_names_count = field_names.len();

    // Generate const impls and expr helper (same as PolarsColumns macro)
    let const_impls = fields.iter().map(|f| {
        let field_name = &f.ident;
        let field_name_str = field_name.as_ref().unwrap().to_string();
        quote! {
            #[allow(non_upper_case_globals)]
            pub const #field_name: &'static str = #field_name_str;
        }
    });

    let type_const_impls = fields.iter().zip(polars_types_for_df.clone()).map(|(f, polars_type)| {
        let field_name = &f.ident;
        let type_const_name = syn::Ident::new(
            &format!("{}_type", field_name.as_ref().unwrap()),
            proc_macro2::Span::call_site(),
        );
        quote! {
            #[allow(non_upper_case_globals)]
            pub const #type_const_name: polars::prelude::DataType = #polars_type;
        }
    });

    let col_func_impls = fields.iter().map(|f| {
        let field_name = &f.ident;
        let func_name = syn::Ident::new(
            &format!("{}_col", field_name.as_ref().unwrap()),
            proc_macro2::Span::call_site(),
        );
        let field_name_str = field_name.as_ref().unwrap().to_string();
        quote! {
            pub fn #func_name() -> polars::prelude::Expr {
                polars::prelude::col(#field_name_str)
            }
        }
    });

    // Generate expr helper struct name
    let expr_struct_name =
        syn::Ident::new(&format!("ExprFor{}", name), proc_macro2::Span::call_site());

    let expanded = quote! {
        impl #name {
            #(#const_impls)*
            #(#type_const_impls)*
            #(#col_func_impls)*

            /// Get all column names as Vec<&str> for use with df.select()
            pub fn all_columns() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }

            /// Get all column types as Vec<DataType>
            pub fn all_types() -> Vec<polars::prelude::DataType> {
                vec![#(#polars_types_for_df),*]
            }

            /// Get column type at specific index
            pub fn type_at(index: usize) -> Option<polars::prelude::DataType> {
                let types = [#(#polars_types_for_df),*];
                types.get(index).cloned()
            }

            /// Get all column names as expressions for lazy operations
            pub fn all_cols() -> Vec<polars::prelude::Expr> {
                vec![#(polars::prelude::col(#field_name_strs)),*]
            }

            /// Create an empty DataFrame with the correct schema
            pub fn df() -> std::result::Result<polars::prelude::DataFrame, polars::prelude::PolarsError> {
                let columns = vec![
                    #(
                        polars::prelude::Column::new(#field_name_strs.into(), polars::prelude::Series::new_empty(#field_name_strs.into(), &#polars_types_for_df))
                    ),*
                ];
                polars::prelude::DataFrame::new(columns)
            }

            pub fn validate(df: &polars::prelude::DataFrame) -> ::polars_tools::Result<()> {
                #(#field_validations)*
                Ok(())
            }

            pub fn validate_strict(df: &polars::prelude::DataFrame) -> ::polars_tools::Result<()> {
                Self::validate(df)?;

                let expected_columns: std::collections::HashSet<_> =
                    Self::column_names().into_iter().collect();
                let actual_columns: std::collections::HashSet<_> =
                    df.get_column_names().into_iter().map(|s| s.as_str()).collect();

                if expected_columns != actual_columns {
                    return Err(::polars_tools::ValidationError::ColumnCountMismatch {
                        expected: expected_columns.into_iter().map(|s| s.to_string()).collect(),
                        actual: actual_columns.into_iter().map(|s| s.to_string()).collect(),
                    });
                }

                Ok(())
            }
        }

        pub struct #expr_struct_name;

        impl #expr_struct_name {
            #(
                pub fn #field_names(&self) -> polars::prelude::Expr {
                    polars::prelude::col(#field_name_strs)
                }
            )*
            
            /// Get all column expressions as Vec<Expr> for lazy operations
            pub fn all_cols(&self) -> Vec<polars::prelude::Expr> {
                vec![#(polars::prelude::col(#field_name_strs)),*]
            }
        }

        impl #name {
            pub const expr: #expr_struct_name = #expr_struct_name;
        }

        // Implement the trait methods directly without trait bounds to avoid import issues
        impl #name {
            /// Implementation of PolarsColumnsExt::columns() 
            pub fn columns() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }

            /// Implementation of PolarsColumns::column_names()
            pub fn column_names() -> Vec<&'static str> {
                vec![#(#field_name_strs),*]
            }

            /// Implementation of PolarsColumns::column_name_at()
            pub fn column_name_at(index: usize) -> Option<&'static str> {
                let names = [#(#field_name_strs),*];
                names.get(index).copied()
            }

            /// Implementation of PolarsColumns::col_expr()
            pub fn col_expr(field_name: &str) -> Option<polars::prelude::Expr> {
                match field_name {
                    #(#field_name_strs => Some(polars::prelude::col(#field_name_strs)),)*
                    _ => None,
                }
            }
        }
    };

    TokenStream::from(expanded)
}
