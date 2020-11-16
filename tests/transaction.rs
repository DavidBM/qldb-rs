mod utils;
use eyre::Result;
use ion_binary_rs::IonValue;
use qldb::QLDBClient;
use qldb::QLDBError::SendCommandError;
use rusoto_core::RusotoError::Service;
use rusoto_qldb_session::SendCommandError::OccConflict;
use std::collections::HashMap;
use utils::ensure_test_table;

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
                .query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table))
                .execute()
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
                .query(&format!("INSERT INTO {} VALUE ?", test_table))
                .param(get_value_to_insert())
                .execute()
                .await;

            println!("{:?}", result);

            let count = client
                .query(&format!("SELECT COUNT(*) FROM {}", test_table))
                .execute()
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
                    .query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table))
                    .execute()
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
                    .query(&format!("INSERT INTO {} VALUE ?", test_table))
                    .param(get_value_to_insert())
                    .execute()
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
                    .query(&format!("SELECT COUNT(*) FROM {}", test_table))
                    .execute()
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
                .query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table))
                .execute()
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
                .query(&format!("INSERT INTO {} VALUE ?", test_table))
                .param(get_value_to_insert())
                .execute()
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
        .read_query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table))
        .await?
        .execute()
        .await
        .unwrap();

    Ok(())
}

fn get_value_to_insert() -> IonValue {
    let mut map = HashMap::new();
    map.insert(
        "test_column".to_string(),
        IonValue::String("test_value".to_string()),
    );
    IonValue::Struct(map)
}
