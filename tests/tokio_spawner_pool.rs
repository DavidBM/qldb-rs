mod utils;

use std::sync::Arc;
use qldb::QldbClient;
use utils::cursor_utils::cursor_800_documents_with_client;

#[tokio::test]
async fn cursor_800_documents() {
	let client = QldbClient::default_with_spawner(
		"rust-crate-test", 
		200, 
		Arc::new(move |fut| {tokio::spawn(Box::pin(fut));})
	)
	.await
	.unwrap();

    cursor_800_documents_with_client(client).await;
}
