use crate::{QLDBError, QLDBResult, Transaction};
use ion_binary_rs::{IonEncoder, IonParser, IonValue};
use rusoto_qldb_session::{
    ExecuteStatementRequest, QldbSession, QldbSessionClient, SendCommandRequest, ValueHolder,
};
use std::sync::Arc;

/// Represents the query being built. It allows to add parameters
/// and to execute the query.
#[derive(Clone)]
pub struct QueryBuilder {
    tx: Transaction,
    client: Arc<QldbSessionClient>,
    statement: Arc<String>,
    params: Vec<IonValue>,
    auto_rollback: bool,
}

impl QueryBuilder {
    pub(crate) fn new(
        client: Arc<QldbSessionClient>,
        tx: Transaction,
        statement: &str,
        auto_rollback: bool,
    ) -> QueryBuilder {
        QueryBuilder {
            client,
            tx,
            statement: Arc::new(statement.to_string()),
            params: vec![],
            auto_rollback,
        }
    }

    /// Adds a param to the query. Params in PartiQL are deoned by
    /// the character `?`. For example, the query:
    /// `SELECT * FROM Autos WHERE id = ? AND model = ?`
    /// will need 2 calls to this method. The first will refer to
    /// the first `?` and the second to the second `?`.
    pub fn param<P: Into<IonValue> + Clone>(&mut self, param: P) -> &mut Self {
        self.params.push(param.into());
        self
    }

    /// Executes the query in QLDBwith the parameter provided by
    /// the `param` method. It will return a Vector of Ion Values,
    /// one for each document returned.
    pub async fn execute(&mut self) -> QLDBResult<Vec<IonValue>> {
        if self.tx.is_completed().await {
            return Err(QLDBError::TransactionCompleted);
        }

        // TODO: hash_query may be an expesive operation, maybe
        // we want to move to a task and execute it in parallel
        // with the waiting of the send_command.
        self.tx.hash_query(&self.statement, &self.params).await;

        let params = std::mem::replace(&mut self.params, vec![]);

        let result = self
            .client
            .send_command(create_send_command(
                &self.tx.session,
                &self.tx.transaction_id,
                &self.statement,
                params,
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

        if self.auto_rollback {
            self.tx.rollback().await?;
        }

        Ok(values)
    }

    /// Sends a query to QLDB that returns a count. Keep in mind that there isn't
    /// any filter to fail is another kind of statement is given.
    ///
    /// Be careful with COUNT statements as they "block" the whole table and other
    /// transactions affecting the same table will return an OCC error when committed.
    ///
    /// If you want to make a simple count, it is better to use the count method
    /// from [Client::count](./struct.QLDBClient.html#method.count)
    ///
    pub async fn count(&mut self) -> QLDBResult<i64> {
        let result = self.execute().await?;

        match result.last() {
            Some(IonValue::Struct(data)) => match data.get("_1") {
                Some(IonValue::Integer(count)) => Ok(*count),
                _ => Err(QLDBError::NonValidCountStatementResult),
            },
            _ => Err(QLDBError::NonValidCountStatementResult),
        }
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
