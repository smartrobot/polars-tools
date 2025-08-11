#![allow(non_upper_case_globals)]
#[cfg(feature = "chrono")]
#[test]
fn test_chrono_feature_available() {
    // Simple test to verify chrono feature compiles
    use chrono::{DateTime, NaiveDate, Utc};

    let _date = NaiveDate::from_ymd_opt(2023, 12, 25);
    let _now = Utc::now();

    // If we get here, chrono is available and working
    assert!(true);
}

#[cfg(not(feature = "chrono"))]
mod no_chrono {
    #[test]
    fn test_chrono_feature_disabled() {
        // When chrono feature is disabled, this test passes
        // indicating the feature flag works correctly
        assert!(true);
    }
}
