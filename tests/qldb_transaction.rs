use eyre::Result;
use ion_binary_rs::{IonValue, NullIonValue};
use qldb::QLDBClient;
use qldb::QLDBError::SendCommandError;
use rusoto_core::RusotoError::Service;
use rusoto_qldb_session::SendCommandError::OccConflict;
use std::collections::HashMap;

// In order to check internal steps of this transaction the JS
// implementation was taken as reference. It can be found in
// test/reference_impl/qldb_transaction.js
#[async_std::test]
async fn qldb_transaction() -> Result<()> {
    let client = QLDBClient::default("rust-crate-test").await?;

    let test_table = ensure_test_table(&client).await;

    client
        .transaction_within(|client| async move {
            let result = client
                .query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table), &[])
                .await;

            println!("{:?}", result);

            let count = result?;

            let first_count = match &count[0] {
                IonValue::Struct(data) => match data.get("_1") {
                    Some(IonValue::Integer(count)) => count,
                    _ => panic!("First count returned a non integer"),
                },
                _ => panic!("First count returned a non integer"),
            };

            let result = client
                .query(
                    &format!("INSERT INTO {} VALUE ?", test_table),
                    &[get_value_to_insert()],
                )
                .await;

            println!("{:?}", result);

            let count = client
                .query(&format!("SELECT COUNT(*) FROM {}", test_table), &[])
                .await?;

            match &count[0] {
                IonValue::Struct(data) => match data.get("_1") {
                    Some(IonValue::Integer(count)) => assert_eq!(*count, first_count + 1),
                    _ => panic!("Second count returned a non integer"),
                },
                _ => panic!("Second count returned a non integer"),
            };

            Ok(())
        })
        .await?;

    Ok(())
}

#[async_std::test]
async fn qldb_transaction_rollback() -> Result<()> {
    let client = QLDBClient::default("rust-crate-test").await?;

    let test_table = ensure_test_table(&client).await;

    let first_count = client
        .transaction_within(|client| {
            let test_table = test_table.clone();
            async move {
                let result = client
                    .query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table), &[])
                    .await;

                let count = result?;

                let count = match &count[0] {
                    IonValue::Struct(data) => match data.get("_1") {
                        Some(IonValue::Integer(count)) => count,
                        _ => panic!("First count returned a non integer"),
                    },
                    _ => panic!("First count returned a non integer"),
                };

                Ok(*count)
            }
        })
        .await?;

    client
        .transaction_within(|client| {
            let test_table = test_table.clone();
            async move {
                let _ = client
                    .query(
                        &format!("INSERT INTO {} VALUE ?", test_table),
                        &[get_value_to_insert()],
                    )
                    .await;

                client.rollback().await
            }
        })
        .await?;

    let second_count = client
        .transaction_within(|client| {
            let test_table = test_table.clone();
            async move {
                let count = client
                    .query(&format!("SELECT COUNT(*) FROM {}", test_table), &[])
                    .await?;

                let count = match &count[0] {
                    IonValue::Struct(data) => match data.get("_1") {
                        Some(IonValue::Integer(count)) => count,
                        _ => panic!("Second count returned a non integer"),
                    },
                    _ => panic!("Second count returned a non integer"),
                };

                Ok(*count)
            }
        })
        .await?;

    // If the transaction was committed the second count would be different because
    // of the insert in between the two selects.
    assert_eq!(second_count, first_count);

    Ok(())
}

#[async_std::test]
async fn qldb_transaction_occ_conflict() -> Result<()> {
    let client = QLDBClient::default("rust-crate-test").await?;

    let test_table = ensure_test_table(&client).await;

    let future_a = client.transaction_within(|client| {
        let test_table = test_table.clone();
        async move {
            // Select the whole table. This will block everything in it.
            client
                .query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table), &[])
                .await?;

            // Let's wait for the other transaction to change some data
            async_std::task::sleep(std::time::Duration::from_millis(500)).await;

            Ok(())

            // At this point the data should have changed so the transaction
            // should fail when committed. NOTE: In a normal environment, when
            // doing just a SELECT you don't care, but when adding INSERTs
            // after it you want the transaction (INSERTs) to fail.
        }
    });

    let future_b = client.transaction_within(|client| {
        let test_table = test_table.clone();
        async move {
            async_std::task::sleep(std::time::Duration::from_millis(100)).await;

            client
                .query(
                    &format!("INSERT INTO {} VALUE ?", test_table),
                    &[get_value_to_insert()],
                )
                .await?;

            Ok(())
        }
    });

    let result = futures::join!(future_a, future_b);

    if result.1.is_err() {
        panic!("OCC test failed in the wrong transaction.")
    }

    match result.0 {
        Err(SendCommandError(Service(OccConflict(_)))) => {}
        _ => panic!("Non OCC error on the OCC test!"),
    }

    Ok(())
}

#[async_std::test]
async fn qldb_transaction_simple_select() -> Result<()> {
    let client = QLDBClient::default("rust-crate-test").await?;

    let test_table = ensure_test_table(&client).await;

    client
        .select(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table), &[])
        .await
        .unwrap();

    Ok(())
}

#[async_std::test]
async fn qldb_type_nulls() -> Result<()> {
    create_type_test(get_value_to_insert_nulls(), |values| {
        assert_eq!(
            values.get("Null").unwrap(),
            &IonValue::Null(NullIonValue::Null)
        );
        assert_eq!(
            values.get("Bool").unwrap(),
            &IonValue::Null(NullIonValue::Bool)
        );
        assert_eq!(
            values.get("Integer").unwrap(),
            &IonValue::Null(NullIonValue::Integer)
        );
        assert_eq!(
            values.get("BigInteger").unwrap(),
            &IonValue::Null(NullIonValue::Integer)
        );
        assert_eq!(
            values.get("Float").unwrap(),
            &IonValue::Null(NullIonValue::Float)
        );
        assert_eq!(
            values.get("Decimal").unwrap(),
            &IonValue::Null(NullIonValue::Decimal)
        );
        assert_eq!(
            values.get("DateTime").unwrap(),
            &IonValue::Null(NullIonValue::DateTime)
        );
        assert_eq!(
            values.get("String").unwrap(),
            &IonValue::Null(NullIonValue::String)
        );
        assert_eq!(
            values.get("Symbol").unwrap(),
            &IonValue::Null(NullIonValue::Symbol)
        );
        assert_eq!(
            values.get("Clob").unwrap(),
            &IonValue::Null(NullIonValue::Clob)
        );
        assert_eq!(
            values.get("Blob").unwrap(),
            &IonValue::Null(NullIonValue::Blob)
        );
        assert_eq!(
            values.get("List").unwrap(),
            &IonValue::Null(NullIonValue::List)
        );
        assert_eq!(
            values.get("SExpr").unwrap(),
            &IonValue::Null(NullIonValue::SExpr)
        );
        assert_eq!(
            values.get("Struct").unwrap(),
            &IonValue::Null(NullIonValue::Struct)
        );
    })
    .await
}

async fn create_type_test<F: FnOnce(HashMap<String, IonValue>)>(
    insert_data: IonValue,
    test_callback: F,
) -> Result<()> {
    let client = QLDBClient::default("rust-crate-test").await?;

    let test_table = ensure_test_table(&client).await;

    let value = client
        .transaction_within(|client| {
            let test_table = test_table.clone();
            async move {
                let value = client
                    .query(
                        &format!("INSERT INTO {} VALUE ?", test_table),
                        &[insert_data],
                    )
                    .await?;

                Ok(value)
            }
        })
        .await?;

    println!("{:?}", value);

    let document_id = match value.as_slice() {
        [IonValue::Struct(value)] => value.get("documentId").unwrap().clone(),
        _ => panic!("Insert didn't return a document id"),
    };

    let value = client
        .select(
            &format!(
                "SELECT * FROM _ql_committed_{} as r WHERE r.metadata.id = ?",
                test_table
            ),
            &[document_id],
        )
        .await?;

    let values = match value.as_slice() {
        [IonValue::Struct(value)] => match value.get("data").unwrap() {
            IonValue::Struct(value) => value,
            _ => panic!("Select didn't return data"),
        },
        _ => panic!("Select didn't return an struct"),
    };

    println!("{:?}", values);

    test_callback(values.clone());

    Ok(())
}

async fn ensure_test_table(client: &QLDBClient) -> String {
    let _ = client
        .transaction_within(|client| async move {
            let _ = client.query("CREATE TABLE QldbLibRsTest;", &[]).await?;

            Ok(())
        })
        // If the table already exist we ignore the error
        .await;

    "QldbLibRsTest".to_string()
}

fn get_value_to_insert() -> IonValue {
    let mut map = HashMap::new();
    map.insert(
        "test_column".to_string(),
        IonValue::String("test_value".to_string()),
    );
    IonValue::Struct(map)
}

fn get_value_to_insert_nulls() -> IonValue {
    let mut map = HashMap::new();
    map.insert("Null".into(), IonValue::Null(NullIonValue::Null));
    map.insert("Bool".into(), IonValue::Null(NullIonValue::Bool));
    map.insert("Integer".into(), IonValue::Null(NullIonValue::Integer));
    map.insert(
        "BigInteger".into(),
        IonValue::Null(NullIonValue::BigInteger),
    );
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
