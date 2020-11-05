use crate as qldb;

#[async_std::test]
async fn test() {
    let client = qldb::create_client_from_env(None).await.unwrap();

    let test_var = "test_var";

    let result = client
        .transaction_within(|client| async move {
            client.query("", vec![]).await?;
            println!("{:?}", test_var);
            Ok("hol")
        })
        .await
        .unwrap();

    println!("{:?}", result);
}
