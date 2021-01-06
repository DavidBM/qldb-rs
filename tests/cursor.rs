mod utils;

use ion_binary_rs::IonValue;
use qldb::QLDBClient;
use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;
use utils::ensure_test_table;

#[async_std::test]
async fn cursor_800_documents() {
    let client = QLDBClient::default("rust-crate-test").await.unwrap();

    let test_table = ensure_test_table(&client).await;

    let documents_model = format!("cursor_800_documents{}", rand_string());

    for _ in 0..20 {
        let table = test_table.clone();
        let model = documents_model.clone();

        client.transaction_within(|tx| async move {

            let mut query = tx.query(&format!("INSERT INTO {} << ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? >>", &table));

            for _ in 0..40 {
                query = query.param(get_qldb_struct(&model))
            }

            query.execute().await.unwrap();

            Ok(())
        }).await.unwrap();
    }

    let table = test_table.clone();
    let model = documents_model.clone();

    client
        .transaction_within(|tx| async move {
            let mut cursor = tx
                .query(&format!("SELECT * FROM {} WHERE Model = ?", &table))
                .param(model)
                .get_cursor()
                .unwrap();

            let mut counter: u64 = 0;

            let mut result: Vec<IonValue> = vec![];

            while let Some(mut values) = cursor.load_more().await.unwrap() {
                counter += 1;
                result.append(&mut values);
            }

            assert!(counter > 1);
            assert_eq!(result.len(), 800);

            Ok(())
        })
        .await
        .unwrap();

    let table = test_table.clone();
    let model = documents_model.clone();

    client
        .transaction_within(|tx| async move {
            let result = tx
                .query(&format!("SELECT * FROM {} WHERE Model = ?", &table))
                .param(model)
                .execute()
                .await
                .unwrap();

            assert_eq!(result.len(), 800);

            Ok(())
        })
        .await
        .unwrap();
}

fn get_qldb_struct(name: &str) -> IonValue {
    IonValue::Struct(hashmap!(
        "Model".to_string() => IonValue::String(name.to_string()),
        "Type".to_string() => IonValue::String("Sedan".to_string()),
        "Color".to_string() => IonValue::String("White".to_string()),
        "VIN".to_string() => IonValue::String("1C4RJFAG0FC625797".to_string()),
        "Make".to_string() => IonValue::String("Mercedes".to_string()),
        "Year".to_string() => IonValue::Integer(2019)
    ))
}

#[macro_export]
macro_rules! hashmap(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);

fn rand_string() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect()
}
