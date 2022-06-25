mod utils;
use eyre::Result;
use ion_binary_rs::IonValue;
use qldb::QldbClient;
use std::collections::HashMap;
use utils::ensure_test_table;

#[async_std::test]
async fn closing_session_pool() -> Result<()> {
    let mut client = QldbClient::default("rust-crate-test", 200).await?;

    let test_table = ensure_test_table(&client).await;

    client
        .transaction_within(|client| {
            let test_table = test_table.clone();
            async move {
                client
                    .query(&format!(r#"SELECT COUNT(*) FROM {};"#, test_table))
                    .execute()
                    .await?;

                Ok(())
            }
        })
        .await?;

    client.close().await;

    async_std::task::sleep(std::time::Duration::from_millis(100)).await;

    let result_b = client
        .transaction_within(|client| {
            let test_table = test_table.clone();
            async move {
                client
                    .query(&format!("INSERT INTO {} VALUE ?", test_table))
                    .param(get_value_to_insert())
                    .execute()
                    .await?;

                Ok(())
            }
        })
        .await;

    println!("{:?}", result_b);

    if !result_b.is_err() {
        panic!("Close should make the transaction to fail")
    }

    Ok(())
}

fn get_value_to_insert() -> IonValue {
    let mut map = HashMap::new();
    map.insert("test_column".to_string(), IonValue::String("test_value".to_string()));
    IonValue::Struct(map)
}
