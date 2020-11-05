use crate::{QLDBError, QLDBResult, QLDBTransaction};
use rusoto_core::{credential::EnvironmentProvider, request::HttpClient, Region};
use rusoto_qldb_session::{
    QldbSession, QldbSessionClient, SendCommandRequest, StartSessionRequest,
};
use std::future::Future;
use std::sync::Arc;

/// This function will take the credentials from the environtment variables
/// documented in rusoto:
/// https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.EnvironmentProvider.html
/// and create a new QLDBClient.
pub async fn create_client_from_env(region: Option<Region>) -> QLDBResult<QLDBClient> {
    let region = match region {
        Some(region) => region,
        None => Region::default(),
    };

    let credentials = EnvironmentProvider::default();

    // TODO: Map error correctly
    let http_client = HttpClient::new()?;

    let client = Arc::new(QldbSessionClient::new_with(
        http_client,
        credentials,
        region,
    ));

    Ok(QLDBClient { client })
}

/// It allows to start transactions. In QLDB all queries are transactions.
/// So you always need to create a transaction for every query.
///
/// The recomended methos is `transaction_within`.
#[derive(Clone)]
pub struct QLDBClient {
    client: Arc<QldbSessionClient>,
}

impl QLDBClient {
    pub(crate) async fn transaction(&self) -> QLDBResult<QLDBTransaction> {
        let session = self.get_session().await?;

        Ok(QLDBTransaction::new(self.client.clone(), &session).await?)
    }

    /// It call the clousure providing an alredy made transaction. Once the
    /// clousure finishes it will call commit or rollback if any error.
    pub async fn transaction_within<F, R, FR>(&self, clousure: F) -> QLDBResult<R>
    where
        FR: Future<Output = QLDBResult<R>>,
        F: FnOnce(QLDBTransaction) -> FR,
    {
        let transaction = self.transaction().await?;

        let result = clousure(transaction.clone()).await;

        match result {
            Ok(result) => Ok(result),
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    async fn get_session(&self) -> QLDBResult<String> {
        let response = self
            .client
            .send_command(SendCommandRequest {
                // TODO: Remove hardcoded ledger_name
                start_session: Some(StartSessionRequest {
                    ledger_name: "jumboxs-test".to_string(),
                }),
                ..Default::default()
            })
            .await?;

        let token = match response.start_session {
            Some(session) => match session.session_token {
                Some(token) => token,
                None => return Err(QLDBError::QLDBReturnedEmptySession),
            },
            None => return Err(QLDBError::QLDBReturnedEmptySession),
        };

        Ok(token)
    }
}
