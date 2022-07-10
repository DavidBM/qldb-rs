use crate::{session_pool::{ThreadedSessionPool, SessionPool}, QldbError, QldbResult, QueryBuilder, Transaction};
use rusoto_core::{credential::ChainProvider, request::HttpClient, Region};
use rusoto_qldb_session::QldbSessionClient;
use std::future::Future;
use std::sync::Arc;
#[cfg(feature = "internal_pool_with_spawner")]
use crate::session_pool::{SpawnerFnMonoMultithread, SpawnerSessionPool};

/// It allows to start transactions. In QLDB all queries are transactions.
/// So you always need to create a transaction for every query.
///
/// The recommended method is `transaction_within`.
#[derive(Clone)]
pub struct QldbClient {
    client: Arc<QldbSessionClient>,
    _ledger_name: String,
    session_pool: Arc<dyn SessionPool>,
}

impl QldbClient {
    /// Creates a new QldbClient.
    /// 
    /// It will spawn one thread for the session pool.
    ///
    /// This function will take the credentials from several locations in this order:
    ///
    ///  - Environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    ///  - credential_process command in the AWS config file, usually located at ~/.aws/config.
    ///  - AWS credentials file. Usually located at ~/.aws/credentials.
    ///  - IAM instance profile. Will only work if running on an EC2 instance with an instance profile/role.
    ///
    /// [https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ChainProvider.html](https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ChainProvider.html)
    ///
    /// For the region it will will attempt to read the AWS_DEFAULT_REGION or AWS_REGION
    /// environment variable. If it is malformed, it will fall back to Region::UsEast1.
    /// If it is not present it will fallback on the value associated with the current
    /// profile in ~/.aws/config or the file specified by the AWS_CONFIG_FILE environment
    /// variable. If that is malformed of absent it will fall back on Region::UsEast1
    pub async fn default(ledger_name: &str, max_sessions: u16) -> QldbResult<QldbClient> {
        let region = Region::default();

        let credentials = ChainProvider::default();

        // TODO: Map error correctly
        let http_client = HttpClient::new()?;

        let client = Arc::new(QldbSessionClient::new_with(http_client, credentials, region));

        let session_pool = Arc::new(ThreadedSessionPool::new(client.clone(), ledger_name, max_sessions));

        Ok(QldbClient {
            client,
            _ledger_name: ledger_name.to_string(),
            session_pool,
        })
    }

    /// Creates a new QldbClient.
    /// 
    /// This function won't spawn a thread for the session pool, but it will require 
    /// to be given an spawn function so it can start 2 green threads for the session
    /// pool.
    ///
    /// This function will take the credentials from several locations in this order:
    ///
    ///  - Environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    ///  - credential_process command in the AWS config file, usually located at ~/.aws/config.
    ///  - AWS credentials file. Usually located at ~/.aws/credentials.
    ///  - IAM instance profile. Will only work if running on an EC2 instance with an instance profile/role.
    ///
    /// [https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ChainProvider.html](https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ChainProvider.html)
    ///
    /// For the region it will will attempt to read the AWS_DEFAULT_REGION or AWS_REGION
    /// environment variable. If it is malformed, it will fall back to Region::UsEast1.
    /// If it is not present it will fallback on the value associated with the current
    /// profile in ~/.aws/config or the file specified by the AWS_CONFIG_FILE environment
    /// variable. If that is malformed of absent it will fall back on Region::UsEast1
    #[cfg(feature = "internal_pool_with_spawner")]
    pub async fn default_with_spawner(ledger_name: &str, max_sessions: u16, spawner: SpawnerFnMonoMultithread) -> QldbResult<QldbClient> {
        let region = Region::default();

        let credentials = ChainProvider::default();

        // TODO: Map error correctly
        let http_client = HttpClient::new()?;

        let client = Arc::new(QldbSessionClient::new_with(http_client, credentials, region));

        let session_pool = Arc::new(SpawnerSessionPool::new(client.clone(), ledger_name, max_sessions, spawner));

        Ok(QldbClient {
            client,
            _ledger_name: ledger_name.to_string(),
            session_pool,
        })
    }

    /// Shorthand method that creates a transaction and executes a query.
    /// Currently it doesn't filter by statements, so any statement can be
    /// sent but it won't have effect as it will rollback any change. This
    /// allows to read big quantities of data without failing other
    /// transactions that may be reading that data at the same time.
    ///
    /// This is a good option when you want to execute an isolated non-ACID
    /// SELECT/COUNT statement.
    pub async fn read_query(&self, statement: &str) -> QldbResult<QueryBuilder> {
        let transaction = self.auto_rollback_transaction().await?;

        Ok(transaction.query(statement))
    }

    /// Starts a transaction and returns you the transaction handler. When
    /// using this method the transaction won't automatically commit or rollback
    /// when finished. If they are left open they will be canceled when the
    /// transaction times out on the DB side (30 seconds).
    ///
    /// Use this method if you really need to use the transaction handler
    /// directly. If not, you may be better off using the method
    /// `transaction_within`.
    pub async fn transaction(&self) -> QldbResult<Transaction> {
        let session = self.session_pool.get().await.map_err(QldbError::SessionPoolClosed)?;

        Transaction::new(self.client.clone(), self.session_pool.clone(), session, false).await
    }

    pub(crate) async fn auto_rollback_transaction(&self) -> QldbResult<Transaction> {
        let session = self.session_pool.get().await.map_err(QldbError::SessionPoolClosed)?;

        Transaction::new(self.client.clone(), self.session_pool.clone(), session, true).await
    }

    /// It closes the session pool. Current transaction which already have a
    /// session can work as normal, but new transaction (requiring a new session
    /// id) will return error.
    ///
    /// Call this method only when you are sure that all important work is
    /// already commited to QLDB.
    pub async fn close(&mut self) {
        self.session_pool.close().await;
    }

    /// It call the closure providing an already made transaction. Once the
    /// closure finishes it will call commit or rollback if any error.
    pub async fn transaction_within<F, R, FR>(&self, clousure: F) -> QldbResult<R>
    where
        R: std::fmt::Debug,
        FR: Future<Output = QldbResult<R>>,
        F: FnOnce(Transaction) -> FR,
    {
        let transaction = self.transaction().await?;

        let result = clousure(transaction.clone()).await;

        match result {
            Ok(result) => {
                transaction.silent_commit().await?;
                Ok(result)
            }
            Err(error) => {
                transaction.silent_rollback().await?;
                Err(error)
            }
        }
    }
}
