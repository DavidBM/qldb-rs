//! # Amazon's QLDB Driver
//!
//! Amazon's QLDB driver implemented in pure rust.
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
//! 

mod client;
mod transaction;
mod types;

pub use client::QLDBClient;
pub use rusoto_core::Region;
pub use transaction::QLDBTransaction;
pub use types::{QLDBError, QLDBResult};
