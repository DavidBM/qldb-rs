mod utils;

use qldb::QldbClient;
use std::sync::Arc;
use utils::cursor_utils::cursor_800_documents_with_client;

#[tokio::test]
async fn cursor_800_documents_10_times_parallel_tokio() {
    let client = QldbClient::default_with_spawner(
        "rust-crate-test",
        200,
        Arc::new(move |fut| {
            tokio::spawn(Box::pin(fut));
        }),
    )
    .await
    .unwrap();

    let tasks = [
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
        cursor_800_documents_with_client(client.clone()),
    ];

    futures::future::join_all(tasks).await;
}
