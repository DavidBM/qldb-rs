mod utils;
use eyre::Result;
use ion_binary_rs::{IonValue, NullIonValue};
use std::collections::HashMap;
use utils::create_type_test;

#[async_std::test]
async fn qldb_type_nulls() -> Result<()> {
    create_type_test(get_value_to_insert_nulls(), |values| {
        assert_eq!(values.get("Null").unwrap(), &IonValue::Null(NullIonValue::Null));
        assert_eq!(values.get("Bool").unwrap(), &IonValue::Null(NullIonValue::Bool));
        assert_eq!(values.get("Integer").unwrap(), &IonValue::Null(NullIonValue::Integer));
        assert_eq!(values.get("Float").unwrap(), &IonValue::Null(NullIonValue::Float));
        assert_eq!(values.get("Decimal").unwrap(), &IonValue::Null(NullIonValue::Decimal));
        assert_eq!(values.get("DateTime").unwrap(), &IonValue::Null(NullIonValue::DateTime));
        assert_eq!(values.get("String").unwrap(), &IonValue::Null(NullIonValue::String));
        assert_eq!(values.get("Symbol").unwrap(), &IonValue::Null(NullIonValue::Symbol));
        assert_eq!(values.get("Clob").unwrap(), &IonValue::Null(NullIonValue::Clob));
        assert_eq!(values.get("Blob").unwrap(), &IonValue::Null(NullIonValue::Blob));
        assert_eq!(values.get("List").unwrap(), &IonValue::Null(NullIonValue::List));
        assert_eq!(values.get("SExpr").unwrap(), &IonValue::Null(NullIonValue::SExpr));
        assert_eq!(values.get("Struct").unwrap(), &IonValue::Null(NullIonValue::Struct));
    })
    .await
}

fn get_value_to_insert_nulls() -> IonValue {
    let mut map = HashMap::new();
    map.insert("Null".into(), IonValue::Null(NullIonValue::Null));
    map.insert("Bool".into(), IonValue::Null(NullIonValue::Bool));
    map.insert("Integer".into(), IonValue::Null(NullIonValue::Integer));
    map.insert("Float".into(), IonValue::Null(NullIonValue::Float));
    map.insert("Decimal".into(), IonValue::Null(NullIonValue::Decimal));
    map.insert("DateTime".into(), IonValue::Null(NullIonValue::DateTime));
    map.insert("String".into(), IonValue::Null(NullIonValue::String));
    map.insert("Symbol".into(), IonValue::Null(NullIonValue::Symbol));
    map.insert("Clob".into(), IonValue::Null(NullIonValue::Clob));
    map.insert("Blob".into(), IonValue::Null(NullIonValue::Blob));
    map.insert("List".into(), IonValue::Null(NullIonValue::List));
    map.insert("SExpr".into(), IonValue::Null(NullIonValue::SExpr));
    map.insert("Struct".into(), IonValue::Null(NullIonValue::Struct));
    // Null annotation here would be illegal
    //map.insert("Annotation".into(), IonValue::Null(NullIonValue::Annotation));
    IonValue::Struct(map)
}
