mod utils;
use eyre::Result;
use ion_binary_rs::IonValue;
use std::collections::HashMap;
use utils::create_type_test;

#[async_std::test]
async fn qldb_type_datetime() -> Result<()> {
    create_type_test(get_value_to_insert_dates(), |values| {
        assert_eq!(
            values.get("2011-01-01T00:00:00+00:00").unwrap(),
            &IonValue::DateTime(
                chrono::DateTime::parse_from_rfc3339("2011-01-01T00:00:00+00:00").unwrap()
            )
        );
        assert_eq!(
            values.get("2011-02-01T00:00:00+00:00").unwrap(),
            &IonValue::DateTime(
                chrono::DateTime::parse_from_rfc3339("2011-02-01T00:00:00+00:00").unwrap()
            )
        );
        assert_eq!(
            values.get("2011-02-20T00:00:00+00:00").unwrap(),
            &IonValue::DateTime(
                chrono::DateTime::parse_from_rfc3339("2011-02-20T00:00:00+00:00").unwrap()
            )
        );
        assert_eq!(
            values.get("2011-02-20T11:30:59.100-08:00").unwrap(),
            &IonValue::DateTime(
                chrono::DateTime::parse_from_rfc3339("2011-02-20T11:30:59.100-08:00").unwrap()
            )
        );
    })
    .await
}

fn get_value_to_insert_dates() -> IonValue {
    let mut map = HashMap::new();
    map.insert(
        "2011-01-01T00:00:00+00:00".into(),
        IonValue::DateTime(
            chrono::DateTime::parse_from_rfc3339("2011-01-01T00:00:00+00:00").unwrap(),
        ),
    );
    map.insert(
        "2011-02-01T00:00:00+00:00".into(),
        IonValue::DateTime(
            chrono::DateTime::parse_from_rfc3339("2011-02-01T00:00:00+00:00").unwrap(),
        ),
    );
    map.insert(
        "2011-02-20T00:00:00+00:00".into(),
        IonValue::DateTime(
            chrono::DateTime::parse_from_rfc3339("2011-02-20T00:00:00+00:00").unwrap(),
        ),
    );
    map.insert(
        "2011-02-20T11:30:59.100-08:00".into(),
        IonValue::DateTime(
            chrono::DateTime::parse_from_rfc3339("2011-02-20T11:30:59.100-08:00").unwrap(),
        ),
    );
    IonValue::Struct(map)
}
