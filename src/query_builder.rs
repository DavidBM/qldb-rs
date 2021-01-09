use crate::{DocumentCollection, Cursor, QLDBError, QLDBResult, Transaction};
use ion_binary_rs::{IonEncoder, IonParser, IonValue};
use rusoto_qldb_session::{
    ExecuteStatementRequest, FetchPageRequest, QldbSession, QldbSessionClient, SendCommandRequest,
    ValueHolder,
};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;

/// Represents the query being built. It allows to add parameters
/// and to execute the query.
pub struct QueryBuilder {
    tx: Transaction,
    client: Arc<QldbSessionClient>,
    statement: Arc<String>,
    params: Vec<IonValue>,
    auto_rollback: bool,
    is_executed: Arc<AtomicBool>,
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
            is_executed: Arc::new(AtomicBool::from(false)),
        }
    }

    /// Adds a param to the query. Params in PartiQL are deoned by
    /// the character `?`. For example, the query:
    /// `SELECT * FROM Autos WHERE id = ? AND model = ?`
    /// will need 2 calls to this method. The first will refer to
    /// the first `?` and the second to the second `?`.
    pub fn param<P: Into<IonValue> + Clone>(mut self, param: P) -> Self {
        self.params.push(param.into());
        self
    }

    /// Executes the query in QLDBwith the parameter provided by
    /// the `param` method. It will return a Vector of Ion Values,
    /// one for each document returned.
    ///
    /// This method will automatically load all the pages. It may
    /// require to make several HTTP calls to the QLDB Ledger as
    /// each Page contains no more than 200 documents.
    ///
    /// It consumes the QueryBuilder in the process.
    pub async fn execute(self) -> QLDBResult<DocumentCollection> {
        let auto_rollback = self.auto_rollback;
        let tx = self.tx.clone();

        let result = self.get_cursor()?.load_all().await?;

        if auto_rollback {
            tx.rollback().await?;
        }

        Ok(result)
    }

    pub(crate) async fn execute_get_page(
        &mut self,
        page_token: &str,
    ) -> QLDBResult<(Vec<IonValue>, Option<String>)> {
        let result = self
            .client
            .send_command(create_next_page_command(
                &self.tx.session,
                &self.tx.transaction_id,
                page_token,
            ))
            .await?;

        let (values, next_page_token) = result
            .fetch_page
            .and_then(|page| page.page)
            .map(|page| {
                // Default of Vec is empty Vec
                let values = page.values.unwrap_or_default();

                (values, page.next_page_token)
            })
            .unwrap_or((vec![], None));

        let values = valueholders_to_ionvalues(values)?;

        Ok((values, next_page_token))
    }

    pub(crate) async fn execute_statement(
        &mut self,
    ) -> QLDBResult<(Vec<IonValue>, Option<String>)> {
        if self.tx.is_completed().await {
            return Err(QLDBError::TransactionCompleted);
        }

        if self.is_executed.load(Relaxed) {
            return Err(QLDBError::QueryAlreadyExecuted);
        }

        // TODO: hash_query may be an expesive operation, maybe
        // we want to move to a task and execute it in parallel
        // with the waiting of the send_command.
        self.tx.hash_query(&self.statement, &self.params).await;

        let params = std::mem::replace(&mut self.params, vec![]);

        self.is_executed.store(true, Relaxed);

        let result = self
            .client
            .send_command(create_send_command(
                &self.tx.session,
                &self.tx.transaction_id,
                &self.statement,
                params,
            ))
            .await?;

        let (values, next_page_token) = result
            .execute_statement
            .and_then(|result| result.first_page)
            .map(|result| {
                // Default of Vec is empty Vec
                let values = result.values.unwrap_or_default();

                (values, result.next_page_token)
            })
            .unwrap_or((vec![], None));

        let values = valueholders_to_ionvalues(values)?;

        Ok((values, next_page_token))
    }

    /// Creates a cursor for this query, allowing to load values
    /// page by page. Each page in QLDB contains 200 documents.
    pub fn get_cursor(self) -> QLDBResult<Cursor> {
        if self.is_executed.load(Relaxed) {
            return Err(QLDBError::QueryAlreadyExecuted);
        }

        Ok(Cursor::new(self))
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
    /// It consumes the QueryBuilder in the process.
    pub async fn count(self) -> QLDBResult<i64> {
        let result = self.execute().await?;

        match result.into_inner().last() {
            Some(ref doc) => match doc.get("_1") {
                Some(IonValue::Integer(count)) => Ok(*count),
                _ => Err(QLDBError::NonValidCountStatementResult),
            },
            _ => Err(QLDBError::NonValidCountStatementResult),
        }
    }
}

impl Debug for QueryBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Transaction")
            .field("tx", &self.tx)
            .field("statement", &self.statement)
            .field("params", &self.params)
            .field("auto_rollback", &self.auto_rollback)
            .finish()
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

fn create_next_page_command(
    session: &str,
    transaction_id: &str,
    next_page_token: &str,
) -> SendCommandRequest {
    SendCommandRequest {
        session_token: Some(session.to_string()),
        fetch_page: Some(FetchPageRequest {
            transaction_id: transaction_id.to_string(),
            next_page_token: next_page_token.to_string(),
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
