use eyre::Result;
use ion_binary_rs::IonValue;
use qldb::QLDBClient;
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

            let value_to_insert = {
                let mut map = HashMap::new();
                map.insert(
                    "test_column".to_string(),
                    IonValue::String("test_value".to_string()),
                );
                IonValue::Struct(map)
            };

            let result = client
                .query(&format!("INSERT INTO {} VALUE ?", test_table), &[value_to_insert])
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

                let value_to_insert = {
                    let mut map = HashMap::new();
                    map.insert(
                        "test_column".to_string(),
                        IonValue::String("test_value".to_string()),
                    );
                    IonValue::Struct(map)
                };

                let _ = client
                    .query(&format!("INSERT INTO {} VALUE ?", test_table), &[value_to_insert])
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

    // If the transaction was commited the second count would be different because
    // of the insert in between the two selects.
    assert_eq!(second_count, first_count);

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
