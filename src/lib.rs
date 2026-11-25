//! # Amazon's QLDB Driver
//!
//! Driver for Amazon's QLDB Database implemented in pure rust.
//!
//! [![Documentation](https://docs.rs/qldb/badge.svg)](https://docs.rs/qldb)
//! [![Crates.io](https://img.shields.io/crates/v/qldb)](https://crates.io/crates/qldb)
//! [![Rust](https://github.com/DavidBM/qldb-rs/actions/workflows/rust.yml/badge.svg)](https://github.com/DavidBM/qldb-rs/actions/workflows/rust.yml)
//!
//! **Service retired.** AWS shut down Amazon QLDB on July 31, 2025. This driver is retained as a reference for existing QLDB code. See the [AWS shutdown notice](https://docs.aws.amazon.com/general/latest/gr/elastictranscoder.html) and [AWS migration sample for Aurora PostgreSQL](https://github.com/aws-samples/example-qldb-ledger-migration).
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
//! # Session Pool
//!
//! The driver has a session pool. The second parameter in the
//! QldbClient::default is the maximum size of the connection pool.
//!
//! The pool will be auto-populated as parallel transaction are being
//! requested until it reaches the provided maximum.
//!
//! The pool uses one independent thread with a single-threaded
//! executor ([async-executor](https://crates.io/crates/async-executor))
//! in order to be able to spawn tasks after the session has been returned.
//!
//! ## Alternative session Pool
//!
//! There is an alternative session pool that will require an spawner
//! function to be provided. It allows to have the pool running by using
//! the spawn function of the executor you use. We tested async-std and
//! tokio, but others should work as well.
//!
//! This pool will spawn two internal tasks handling the pool.
//!
//! Use this if you want for this driver to not create a new thread.
//!
//! Example with async-std:
//!
//! ```rust,no_run
//! use qldb::QldbClient;
//! use std::sync::Arc;
//! # use eyre::Result;
//!
//! # async fn test() -> Result<()> {
//!     let client = QldbClient::default_with_spawner(
//!         "rust-crate-test",
//!         200,
//!         Arc::new(move |fut| {async_std::task::spawn(Box::pin(fut));})
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Or, with tokio:
//!
//! ```rust,no_run
//! use qldb::QldbClient;
//! use std::sync::Arc;
//! # use eyre::Result;
//!
//! # async fn test() -> Result<()> {
//!     let client = QldbClient::default_with_spawner(
//!         "rust-crate-test",
//!         200,
//!         Arc::new(move |fut| {tokio::spawn(Box::pin(fut));})
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Select the pool you want to use
//!
//! By default, both pools are available by using the methods `QldbClient::default`
//! and `QldbClient::default_with_spawner`. If you don't want the pool to be available
//! in runtime, you can disable by removing the default features. Still, you will
//! need to add at least one feature to enable one pool.
//!
//! This will only enable the default pool, the one that uses one thread.
//! ```toml,no_code
//! qldb = { version = "3", default_features = false, features = ["internal_pool_with_thread"]}
//! ```
//!
//! This will only enable the alternative pool, the one that requires an spawner
//! ```toml,no_code
//! qldb = { version = "3", default_features = false, features = ["internal_pool_with_spawner"]}
//! ```
//!
//! # Underlying Ion Format Implementation
//!
//! The library uses [ion-binary-rs](https://crates.io/crates/ion-binary-rs),
//! which is our own, pure rust, implementation of the format. It is very
//! well tested and ready to use in production too.
//!
//! # Test
//!
//! Run `cargo test --lib` and `cargo test --doc` for offline checks. The
//! integration tests in `tests/` depend on a live QLDB ledger and are retained
//! as reference alongside the driver.

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
