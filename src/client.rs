use ion_binary_rs::IonValue;
use crate::{QLDBError, QLDBResult, QLDBTransaction};
use rusoto_core::{credential::ChainProvider, request::HttpClient, Region};
use rusoto_qldb_session::{
    EndSessionRequest, QldbSession, QldbSessionClient, SendCommandRequest, StartSessionRequest,
};
use std::future::Future;
use std::sync::Arc;

/// It allows to start transactions. In QLDB all queries are transactions.
/// So you always need to create a transaction for every query.
///
/// The recomended methos is `transaction_within`.
#[derive(Clone)]
pub struct QLDBClient {
    client: Arc<QldbSessionClient>,
    ledger_name: String,
}

impl QLDBClient {
    /// Creates a new QLDBClient.
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
    ///
    pub async fn default(ledger_name: &str) -> QLDBResult<QLDBClient> {
        let region = Region::default();

        let credentials = ChainProvider::default();

        // TODO: Map error correctly
        let http_client = HttpClient::new()?;

        let client = Arc::new(QldbSessionClient::new_with(
            http_client,
            credentials,
            region,
        ));

        Ok(QLDBClient {
            client,
            ledger_name: ledger_name.to_string(),
        })
    }

    // Shorthand method that creates a transaction and executes an SELECT.
    // Currently it doesn't filter by statements, so any statement can be
    // sent but it won't have effect as it will rollback any change.
    // 
    // This is a good option when you want to execute an isolated non-ACID
    // SELECT statement.
    pub async fn select(&self, statement: &str, params: &[IonValue]) -> QLDBResult<Vec<IonValue>> {

        let transaction = self.transaction().await?;

        let value = transaction.query(statement, params).await?;

        // We don't care if the rollback fails as we have the result.
        // TODO: We don't need to wait for the rollback to finish, 
        // we should move it to a thread-pool or local executor to 
        // rollback the transaction and return the result earlier.
        let _ = transaction.rollback().await;

        Ok(value)
    }

    pub(crate) async fn transaction(&self) -> QLDBResult<QLDBTransaction> {
        let session = self.get_session().await?;

        Ok(QLDBTransaction::new(self.client.clone(), &session).await?)
    }

    /// It call the closure providing an already made transaction. Once the
    /// closure finishes it will call commit or rollback if any error.
    pub async fn transaction_within<F, R, FR>(&self, clousure: F) -> QLDBResult<R>
    where
        R: std::fmt::Debug,
        FR: Future<Output = QLDBResult<R>>,
        F: FnOnce(QLDBTransaction) -> FR,
    {
        let transaction = self.transaction().await?;

        let result = clousure(transaction.clone()).await;

        match result {
            Ok(result) => {
                transaction.commit().await?;
                self.close_session(transaction.get_session()).await?;
                Ok(result)
            }
            Err(error) => {
                transaction.rollback().await?;
                self.close_session(transaction.get_session()).await?;
                Err(error)
            }
        }
    }

    async fn close_session(&self, session: &str) -> QLDBResult<()> {
        self.client
            .send_command(SendCommandRequest {
                session_token: Some(session.to_string()),
                end_session: Some(EndSessionRequest {}),
                ..Default::default()
            })
            .await?;

        Ok(())
    }

    async fn get_session(&self) -> QLDBResult<String> {
        let response = self
            .client
            .send_command(SendCommandRequest {
                start_session: Some(StartSessionRequest {
                    ledger_name: self.ledger_name.clone(),
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
