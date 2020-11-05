use crate::types::{QLDBError, QLDBResult};
use futures::lock::Mutex;
use ion_binary_rs::IonHash;
use ion_binary_rs::IonValue;
use rusoto_qldb_session::{
    QldbSession, QldbSessionClient, SendCommandRequest, StartTransactionRequest,
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

    pub async fn query(&self, _query: &str, _params: Vec<IonValue>) -> QLDBResult<Vec<IonValue>> {
        // TODO: Add _query to the IonHash
        // TODO: Add _params to the IonHash
        // TODO: Execute query
        // TODO: If the result is paged, return a object that keeps the page and is able to
        //       load the next page and decode the Ion Values
        todo!()
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
