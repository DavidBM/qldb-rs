# Amazon QLDB driver for Rust (deprecated)

AWS shut down Amazon Quantum Ledger Database (QLDB) on July 31, 2025. This driver is deprecated and its source and documentation are available for historical reference. [AWS shutdown notice](https://docs.aws.amazon.com/general/latest/gr/full_shutdown_services.html)

All published versions of `qldb` are yanked from crates.io. Cargo will skip them when resolving new dependencies. Builds that already lock a published version can still download it. [Crates.io versions](https://crates.io/crates/qldb/versions) · [Cargo yank documentation](https://doc.rust-lang.org/cargo/commands/cargo-yank.html)

## Migration and reference

- For information on moving QLDB data to Aurora PostgreSQL, see the [AWS migration sample](https://github.com/aws-samples/example-qldb-ledger-migration).
- For historical API usage and configuration examples, see the [original driver documentation](https://github.com/DavidBM/qldb-rs/blob/4944f942b0bd8c8c5fa76584699e1271ef4d5f5d/README.md).
