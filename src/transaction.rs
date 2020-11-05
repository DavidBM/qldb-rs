use crate::types::{QLDBError, QLDBResult};
use futures::lock::Mutex;
use ion_binary_rs::{IonEncoder, IonHash, IonParser, IonValue};
use rusoto_qldb_session::{
    ExecuteStatementRequest, QldbSession, QldbSessionClient, SendCommandRequest,
    StartTransactionRequest, ValueHolder,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct QLDBTransaction {
    client: Arc<QldbSessionClient>,
    transaction_id: Arc<String>,
    session: Arc<String>,
    completed: Arc<Mutex<bool>>,
    hash: Arc<Mutex<IonHash>>,
}

impl QLDBTransaction {
    pub async fn new(client: Arc<QldbSessionClient>, session: &str) -> QLDBResult<QLDBTransaction> {
        let transaction_id = QLDBTransaction::get_transaction_id(&client, session).await?;

        // TODO: Add transaction_id to the IonHash

        Ok(QLDBTransaction {
            client,
            transaction_id: Arc::new(transaction_id),
            session: Arc::new(session.into()),
            completed: Arc::new(Mutex::new(false)),
            hash: Arc::new(Mutex::new(IonHash::new())),
        })
    }

    async fn get_transaction_id(
        client: &Arc<QldbSessionClient>,
        session: &str,
    ) -> QLDBResult<String> {
        let response = client
            .send_command(SendCommandRequest {
                session_token: Some(session.to_string()),
                start_transaction: Some(StartTransactionRequest {}),
                ..Default::default()
            })
            .await?;

        let token = match response.start_session {
            Some(session) => match session.session_token {
                Some(token) => token,
                None => return Err(QLDBError::QLDBReturnedEmptyTransaction),
            },
            None => return Err(QLDBError::QLDBReturnedEmptyTransaction),
        };

        Ok(token)
    }

    pub async fn query(&self, statement: &str, params: Vec<IonValue>) -> QLDBResult<Vec<IonValue>> {
        // TODO: Add _query to the IonHash
        // TODO: Add _params to the IonHash
        // TODO: If the result is paged, return a object that keeps the page and is able to
        //       load the next page and decode the Ion Values

        let result = self
            .client
            .send_command(create_send_command(
                &self.session,
                &self.transaction_id,
                statement,
                params,
            ))
            .await?;

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

    async fn complete(&self) -> bool {
        let mut is_completed = self.completed.lock().await;

        if *is_completed {
            return true;
        }

        *is_completed = true;

        false
    }

    pub async fn commit(&self) -> QLDBResult<()> {
        if self.complete().await {
            return Ok(());
        }

        self.client
            .send_command(SendCommandRequest::default())
            .await?;

        Ok(())
    }

    pub async fn rollback(&self) -> QLDBResult<()> {
        if self.complete().await {
            return Ok(());
        }

        self.client
            .send_command(SendCommandRequest::default())
            .await?;

        Ok(())
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
