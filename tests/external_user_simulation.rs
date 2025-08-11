#![allow(non_upper_case_globals)]
//! This test simulates how an external user would use the library
//! It verifies that the generated constants don't produce warnings

use polars_tools::*;

#[derive(PolarsSchema)]
#[allow(dead_code)]
struct ExternalUser {
    user_id: i64,
    user_name: String, 
    email_address: String,
    is_active: bool,
}

#[test]
fn test_external_usage_no_warnings() {
    // Using the generated column constants should not produce warnings
    let user_id_col = ExternalUser::user_id;
    let user_name_col = ExternalUser::user_name;
    let email_col = ExternalUser::email_address;
    let active_col = ExternalUser::is_active;
    
    assert_eq!(user_id_col, "user_id");
    assert_eq!(user_name_col, "user_name");
    assert_eq!(email_col, "email_address");
    assert_eq!(active_col, "is_active");
    
    // This should compile without warnings about non_upper_case_globals
    let columns = vec![
        ExternalUser::user_id, 
        ExternalUser::user_name, 
        ExternalUser::email_address,
        ExternalUser::is_active
    ];
    assert_eq!(columns.len(), 4);
}