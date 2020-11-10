use crate::types::{QLDBError, QLDBResult};
use futures::lock::Mutex;
use ion_binary_rs::{IonEncoder, IonHash, IonParser, IonValue};
use rusoto_qldb_session::{
    AbortTransactionRequest, CommitTransactionRequest, ExecuteStatementRequest, QldbSession,
    QldbSessionClient, SendCommandRequest, StartTransactionRequest, ValueHolder,
};
use sha2::Sha256;
use std::sync::Arc;

/// Every query in QLDB is within a transaction. Ideally you will interact
/// with this object via the method QLDBClient::transaction_within.
#[derive(Clone)]
pub struct QLDBTransaction {
    client: Arc<QldbSessionClient>,
    transaction_id: Arc<String>,
    session: Arc<String>,
    completed: Arc<Mutex<bool>>,
    hasher: Arc<Mutex<IonHash>>,
}

impl QLDBTransaction {
    pub(crate) async fn new(
        client: Arc<QldbSessionClient>,
        session: &str,
    ) -> QLDBResult<QLDBTransaction> {
        let transaction_id = QLDBTransaction::get_transaction_id(&client, session).await?;

        // TODO: Add transaction_id to the IonHash
        let hasher = IonHash::from_ion_value::<Sha256>(&IonValue::String(transaction_id.clone()));

        Ok(QLDBTransaction {
            client,
            transaction_id: Arc::new(transaction_id),
            session: Arc::new(session.into()),
            completed: Arc::new(Mutex::new(false)),
            hasher: Arc::new(Mutex::new(hasher)),
        })
    }

    /// Sends a query to QLDB. It will return an Array of IonValues
    /// already decoded. Parameters need to be provided using IonValue.
    pub async fn query(&self, statement: &str, params: &[IonValue]) -> QLDBResult<Vec<IonValue>> {
        if self.is_completed().await {
            return Err(QLDBError::TransactionCompleted);
        }

        // TODO: hash_query may be an expesive operation, maybe
        // we want to move to a task and execute it in parallel
        // with the waiting of the send_command.
        self.hash_query(statement, &params).await;

        let result = self
            .client
            .send_command(create_send_command(
                &self.session,
                &self.transaction_id,
                statement,
                params.to_vec(),
            ))
            .await?;

        // TODO: If the result is paged, return a object that keeps the page and
        // is able to load the next page and decode the Ion Values.

        let values = result
            .execute_statement
            .and_then(|result| result.first_page)
            // TODO: Store Page::next_page_token from the first_page Page object
            .and_then(|result| result.values)
            // Default of Vec is empty Vec
            .unwrap_or_default();

        let values = valueholders_to_ionvalues(values)?;

        Ok(values)
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
                &commit_digest
            ))
            .await;

        result?;

        // TODO: Check the returned CommitDigest with the
        // current hash and failt if they are not equal.

        Ok(())
    }

    /// Allows to cancel the transaction. Once rollback is called the
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

    async fn is_completed(&self) -> bool {
        let is_completed = self.completed.lock().await;

        if *is_completed {
            return true;
        }

        false
    }

    async fn hash_query(&self, statement: &str, params: &[IonValue]) {
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

fn ionvalue_to_valueholder(value: IonValue) -> ValueHolder {
    // TODO: Add impl From<IonValue> for ValueHolder in ion_binary_rs
    let mut encoder = IonEncoder::new();
    encoder.add(value);
    let bytes = encoder.encode();

    ValueHolder {
        ion_text: None,
        ion_binary: Some(bytes.into()),
    }
}

fn valueholders_to_ionvalues(values: Vec<ValueHolder>) -> QLDBResult<Vec<IonValue>> {
    let mut decoded_values = vec![];

    for value in values {
        let bytes = if let Some(bytes) = value.ion_binary {
            bytes
        } else {
            continue;
        };

        let parsed_values = IonParser::new(&bytes[..])
            .consume_all()
            // TODO: Add impl From<IonParserError> for QLDBError in ion_binary_rs
            .map_err(QLDBError::IonParserError)?;

        for value in parsed_values {
            decoded_values.push(value);
        }
    }

    Ok(decoded_values)
}

fn create_send_command(
    session: &str,
    transaction_id: &str,
    statement: &str,
    params: Vec<IonValue>,
) -> SendCommandRequest {
    SendCommandRequest {
        session_token: Some(session.to_string()),
        execute_statement: Some(ExecuteStatementRequest {
            statement: statement.to_string(),
            parameters: Some(params.into_iter().map(ionvalue_to_valueholder).collect()),
            transaction_id: transaction_id.to_string(),
        }),
        ..Default::default()
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

fn create_rollback_command(
    session: &str,
) -> SendCommandRequest {
    SendCommandRequest {
        session_token: Some(session.to_string()),
        abort_transaction: Some(AbortTransactionRequest {}),
        ..Default::default()
    }
}

fn create_start_transaction_command(
    session: &str,
) -> SendCommandRequest {
    SendCommandRequest {
        session_token: Some(session.to_string()),
        start_transaction: Some(StartTransactionRequest {}),
        ..Default::default()
    }
}
