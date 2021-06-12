<!-- cargo-sync-readme start -->

# Amazon's QLDB Driver

Driver for Amazon's QLDB Database implemented in pure rust.

[![Documentation](https://docs.rs/qldb/badge.svg)](https://docs.rs/qldb)
[![Crates.io](https://img.shields.io/crates/v/qldb)](https://crates.io/crates/qldb)

The driver is fairly tested and should be ready to test in real projects.
We are using it internally, so we will keep it updated.

## Example

```rust,no_run
use qldb::QldbClient;
use std::collections::HashMap;

let client = QldbClient::default("rust-crate-test", 200).await?;

let mut value_to_insert = HashMap::new();
// This will insert a documents with a key "test_column"
// with the value "IonValue::String(test_value)"
value_to_insert.insert("test_column", "test_value");

client
    .transaction_within(|client| async move {   
        client
            .query("INSERT INTO TestTable VALUE ?")
            .param(value_to_insert)
            .execute()
            .await?;
        Ok(())
    })
    .await?;
```

# Session Pool

The driver has a session pool. The second parameter in the
QldbClient::default is the maximun size of the connection pool.

The pool will be auto-populated as parallel transaction are being 
requested until it reaches the provided maximum. 

# Test

For tests you will need to have some AWS credentials in your
PC (as env variables or in ~/.aws/credentials). There needs
to be a QLDB database with the name "rust-crate-test" in the
aws account. The tests need to be run sequentially, so in order
to run the tests please run the following command:

```sh
RUST_TEST_THREADS=1 cargo test
```

<!-- cargo-sync-readme end -->
