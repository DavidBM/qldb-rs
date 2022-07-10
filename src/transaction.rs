use crate::session_pool::{Session, SessionPool};
use crate::types::{QldbError, QldbResult};
use crate::QueryBuilder;
use futures::lock::Mutex;
use futures::lock::MutexGuard;
use ion_binary_rs::{IonEncoder, IonHash, IonValue};
use rusoto_qldb_session::{
    AbortTransactionRequest, CommitTransactionRequest, QldbSession, QldbSessionClient, SendCommandRequest,
    StartTransactionRequest,
};
use sha2::Sha256;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug)]
enum TransactionStatus {
    Open,
    Rollback,
    Commit,
}

/// Every query in QLDB is within a transaction. Ideally you will interact
/// with this object via the method QLDBClient::transaction_within.
#[derive(Clone)]
pub struct Transaction {
    client: Arc<QldbSessionClient>,
    session_pool: Arc<dyn SessionPool>,
    pub(crate) transaction_id: Arc<String>,
    pub(crate) session: Arc<Session>,
    completed: Arc<Mutex<TransactionStatus>>,
    hasher: Arc<Mutex<IonHash>>,
    auto_rollback: bool,
}

impl Transaction {
    pub(crate) async fn new(
        client: Arc<QldbSessionClient>,
        session_pool: Arc<dyn SessionPool>,
        session: Session,
        auto_rollback: bool,
    ) -> QldbResult<Transaction> {
        let transaction_id = Transaction::get_transaction_id(&client, session.get_session_id()).await?;

        let hasher = IonHash::from_ion_value::<Sha256>(&IonValue::String(transaction_id.clone()));

        Ok(Transaction {
            client,
            session_pool,
            transaction_id: Arc::new(transaction_id),
            session: Arc::new(session),
            completed: Arc::new(Mutex::new(TransactionStatus::Open)),
            hasher: Arc::new(Mutex::new(hasher)),
            auto_rollback,
        })
    }

    /// Sends a query to QLDB. It will return an Array of IonValues
    /// already decoded. Parameters need to be provided using IonValue.
    pub fn query(&self, statement: &str) -> QueryBuilder {
        QueryBuilder::new(self.client.clone(), self.clone(), statement, self.auto_rollback)
    }

    pub async fn commit(&self) -> QldbResult<()> {
        use TransactionStatus::*;

        let is_completed = self.completed.lock().await;

        match *is_completed {
            Commit => return Ok(()),
            Rollback => return Err(QldbError::TransactionAlreadyRollback),
            Open => {
                let commit_digest = self.hasher.lock().await.get().to_owned();

                self.client
                    .send_command(create_commit_command(
                        self.session.get_session_id(),
                        &self.transaction_id,
                        &commit_digest,
                    ))
                    .await?;
            }
        }

        self.complete(is_completed, Commit);

        // TODO: Check the returned CommitDigest with the
        // current hash and fail if they are not equal.

        Ok(())
    }

    pub(crate) async fn silent_commit(&self) -> QldbResult<()> {
        match self.commit().await {
            Ok(_) => Ok(()),
            Err(QldbError::TransactionAlreadyRollback) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Cancels the transaction. Once rollback is called the
    /// transaction becomes invalid. Subsequent calls to rollback or
    /// commit (internally) won't have any effect.
    ///
    /// It fails is the transaction is already committed. For
    /// a rollback that doesn't fail when already committed you can
    /// check the `silent_rollback` method.
    pub async fn rollback(&self) -> QldbResult<()> {
        use TransactionStatus::*;

        let is_completed = self.completed.lock().await;

        match *is_completed {
            Rollback => return Ok(()),
            Commit => return Err(QldbError::TransactionAlreadyCommitted),
            Open => {
                self.client
                    .send_command(create_rollback_command(self.session.get_session_id()))
                    .await?;
            }
        }

        self.complete(is_completed, Rollback);

        Ok(())
    }

    /// Cancels the transaction but it doesn't fails is the transaction
    /// was already committed. This is useful for auto-closing scenarios
    /// where you just want to rollback always when there is a drop or
    /// something similar.
    ///
    /// Once rollback is called the
    /// transaction becomes invalid. Subsequent calls to rollback or
    /// commit (internally) won't have any effect.
    pub async fn silent_rollback(&self) -> QldbResult<()> {
        match self.rollback().await {
            Ok(_) => Ok(()),
            Err(QldbError::TransactionAlreadyCommitted) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn is_completed(&self) -> bool {
        use TransactionStatus::*;

        let is_completed = self.completed.lock().await;

        match *is_completed {
            Commit | Rollback => true,
            Open => false,
        }
    }

    fn complete(&self, mut is_completed: MutexGuard<'_, TransactionStatus>, status: TransactionStatus) {
        *is_completed = status;
        self.session_pool.give_back((*self.session).clone());
    }

    pub(crate) async fn hash_query(&self, statement: &str, params: &[IonValue]) {
        let mut hasher = IonHash::from_ion_value::<Sha256>(&IonValue::String(statement.to_string()));

        for param in params {
            let mut encoder = IonEncoder::new();
            encoder.add(param.clone());

            hasher.add_ion_value(param);
        }

        self.hasher.lock().await.dot(hasher);
    }

    async fn get_transaction_id(client: &Arc<QldbSessionClient>, session: &str) -> QldbResult<String> {
        let response = client.send_command(create_start_transaction_command(session)).await?;

        let token = match response.start_transaction {
            Some(session) => match session.transaction_id {
                Some(token) => token,
                None => return Err(QldbError::QldbReturnedEmptyTransaction),
            },
            None => return Err(QldbError::QldbReturnedEmptyTransaction),
        };

        Ok(token)
    }
}

impl Debug for Transaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Transaction")
            .field("transaction_id", &self.transaction_id)
            .field("session", &self.session)
            .finish()
    }
}

fn create_commit_command(session: &str, transaction_id: &str, commit_digest: &[u8]) -> SendCommandRequest {
    SendCommandRequest {
        session_token: Some(session.to_string()),
        commit_transaction: Some(CommitTransactionRequest {
            transaction_id: transaction_id.to_string(),
            commit_digest: commit_digest.to_owned().into(),
        }),
        ..Default::default()
    }
}

fn create_rollback_command(session: &str) -> SendCommandRequest {
    SendCommandRequest {
        session_token: Some(session.to_string()),
        abort_transaction: Some(AbortTransactionRequest {}),
        ..Default::default()
    }
}

fn create_start_transaction_command(session: &str) -> SendCommandRequest {
    SendCommandRequest {
        session_token: Some(session.to_string()),
        start_transaction: Some(StartTransactionRequest {}),
        ..Default::default()
    }
}
