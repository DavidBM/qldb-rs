use crate::types::{QLDBError, QLDBResult};
use crate::QueryBuilder;
use futures::lock::Mutex;
use ion_binary_rs::{IonEncoder, IonHash, IonValue};
use rusoto_qldb_session::{
    AbortTransactionRequest, CommitTransactionRequest, QldbSession, QldbSessionClient,
    SendCommandRequest, StartTransactionRequest,
};
use sha2::Sha256;
use std::sync::Arc;

/// Every query in QLDB is within a transaction. Ideally you will interact
/// with this object via the method QLDBClient::transaction_within.
#[derive(Clone)]
pub struct Transaction {
    client: Arc<QldbSessionClient>,
    pub(crate) transaction_id: Arc<String>,
    pub(crate) session: Arc<String>,
    completed: Arc<Mutex<bool>>,
    hasher: Arc<Mutex<IonHash>>,
    auto_rollback: bool,
}

impl Transaction {
    pub(crate) async fn new(
        client: Arc<QldbSessionClient>,
        session: &str,
        auto_rollback: bool,
    ) -> QLDBResult<Transaction> {
        let transaction_id = Transaction::get_transaction_id(&client, session).await?;

        // TODO: Add transaction_id to the IonHash
        let hasher = IonHash::from_ion_value::<Sha256>(&IonValue::String(transaction_id.clone()));

        Ok(Transaction {
            client,
            transaction_id: Arc::new(transaction_id),
            session: Arc::new(session.into()),
            completed: Arc::new(Mutex::new(false)),
            hasher: Arc::new(Mutex::new(hasher)),
            auto_rollback,
        })
    }

    /// Sends a query to QLDB. It will return an Array of IonValues
    /// already decoded. Parameters need to be provided using IonValue.
    pub fn query(&self, statement: &str) -> QueryBuilder {
        QueryBuilder::new(
            self.client.clone(),
            self.clone(),
            statement,
            self.auto_rollback,
        )
    }

    pub(crate) async fn commit(&self) -> QLDBResult<()> {
        if self.complete().await {
            return Ok(());
        }

        let commit_digest = self.hasher.lock().await.get().to_owned();

        let result = self
            .client
            .send_command(create_commit_command(
                &self.session,
                &self.transaction_id,
                &commit_digest,
            ))
            .await;

        result?;

        // TODO: Check the returned CommitDigest with the
        // current hash and failt if they are not equal.

        Ok(())
    }

    /// Cancels the transaction. Once rollback is called the
    /// transaction becomes invalid. Subsequent calls to rollback or
    /// commit (internally) won't have any effect.
    pub async fn rollback(&self) -> QLDBResult<()> {
        if self.complete().await {
            return Ok(());
        }

        self.client
            .send_command(create_rollback_command(&self.session))
            .await?;

        Ok(())
    }

    async fn complete(&self) -> bool {
        let mut is_completed = self.completed.lock().await;

        if *is_completed {
            return true;
        }

        *is_completed = true;

        false
    }

    pub(crate) async fn is_completed(&self) -> bool {
        let is_completed = self.completed.lock().await;

        if *is_completed {
            return true;
        }

        false
    }

    pub(crate) async fn hash_query(&self, statement: &str, params: &[IonValue]) {
        let mut hasher =
            IonHash::from_ion_value::<Sha256>(&IonValue::String(statement.to_string()));

        for param in params {
            let mut encoder = IonEncoder::new();
            encoder.add(param.clone());

            hasher.add_ion_value(param);
        }

        self.hasher.lock().await.dot(hasher);
    }

    async fn get_transaction_id(
        client: &Arc<QldbSessionClient>,
        session: &str,
    ) -> QLDBResult<String> {
        let response = client
            .send_command(create_start_transaction_command(session))
            .await?;

        let token = match response.start_transaction {
            Some(session) => match session.transaction_id {
                Some(token) => token,
                None => return Err(QLDBError::QLDBReturnedEmptyTransaction),
            },
            None => return Err(QLDBError::QLDBReturnedEmptyTransaction),
        };

        Ok(token)
    }

    pub(crate) fn get_session(&self) -> &str {
        &self.session
    }
}

fn create_commit_command(
    session: &str,
    transaction_id: &str,
    commit_digest: &[u8],
) -> SendCommandRequest {
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
