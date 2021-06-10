//! # Amazon's QLDB Driver
//!
//! Driver for Amazon's QLDB Database implemented in pure rust.
//!
//! [![Documentation](https://docs.rs/qldb/badge.svg)](https://docs.rs/qldb)
//! [![Crates.io](https://img.shields.io/crates/v/qldb)](https://crates.io/crates/qldb)
//!
//! The driver is fairly tested and should be ready to test in real projects.
//! We are using it internally, so we will keep it updated.
//!
//! ## Example
//!
//! ```rust,no_run
//! use qldb::QldbClient;
//! use std::collections::HashMap;
//! # use eyre::Result;
//!
//! # async fn test() -> Result<()> {
//! let client = QldbClient::default("rust-crate-test", 200).await?;
//!
//! let mut value_to_insert = HashMap::new();
//! // This will insert a documents with a key "test_column"
//! // with the value "IonValue::String(test_value)"
//! value_to_insert.insert("test_column", "test_value");
//!
//! client
//!     .transaction_within(|client| async move {   
//!         client
//!             .query("INSERT INTO TestTable VALUE ?")
//!             .param(value_to_insert)
//!             .execute()
//!             .await?;
//!         Ok(())
//!     })
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Test
//!
//! For tests you will need to have some AWS credentials in your
//! PC (as env variables or in ~/.aws/credentials). There needs
//! to be a QLDB database with the name "rust-crate-test" in the
//! aws account. The tests need to be run sequentially, so in order
//! to run the tests please run the following command:
//!
//! ```sh
//! RUST_TEST_THREADS=1 cargo test
//! ```

mod client;
mod cursor;
mod document;
mod document_collection;
mod query_builder;
mod session_pool;
mod transaction;
mod types;

pub use client::QldbClient;
pub use cursor::Cursor;
pub use document::Document;
pub use document_collection::DocumentCollection;
pub use ion_binary_rs as ion;
pub use query_builder::QueryBuilder;
pub use rusoto_core::Region;
pub use transaction::Transaction;
pub use types::{QldbError, QldbResult};
pub use types::{QldbExtractError, QldbExtractResult};
