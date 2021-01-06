use eyre::Result;
use ion_binary_rs::IonValue;
use qldb::QLDBClient;
use std::collections::HashMap;

#[allow(dead_code)]
pub async fn create_type_test<F: FnOnce(HashMap<String, IonValue>)>(
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
                    .query(&format!("INSERT INTO {} VALUE ?", test_table))
                    .param(insert_data)
                    .execute()
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
        .read_query(&format!(
            "SELECT * FROM _ql_committed_{} as r WHERE r.metadata.id = ?",
            test_table
        ))
        .await?
        .param(document_id)
        .execute()
        .await?;

    println!("{:?}", value);

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

pub async fn ensure_test_table(client: &QLDBClient) -> String {
    let result = client
        .transaction_within(|client| async move {
            let _ = client
                .query("CREATE TABLE QldbLibRsTest;")
                .execute()
                .await?;

            Ok(())
        })
        // If the table already exist we ignore the error
        .await;

    println!("Table created: {:?}", result);

    "QldbLibRsTest".to_string()
}
