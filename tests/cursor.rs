mod utils;

use qldb::QldbClient;
use utils::cursor_utils::cursor_800_documents_with_client;

#[async_std::test]
async fn cursor_800_documents() {
    cursor_800_documents_with_client(QldbClient::default("rust-crate-test", 200).await.unwrap()).await;
}
