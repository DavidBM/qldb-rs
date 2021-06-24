use eyre::Report;
use ion_binary_rs::IonParserError;
use rusoto_core::{request::TlsError, RusotoError};
use rusoto_qldb_session::SendCommandError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QldbError {
    #[error("The QLDB command returned an error")]
    SendCommandError(#[from] RusotoError<SendCommandError>),
    #[error("We requested a session but QLDB returned nothing")]
    QldbReturnedEmptySession,
    #[error("We requested a transaction id but QLDB returned nothing")]
    QldbReturnedEmptyTransaction,
    #[error("We requested a transaction id but QLDB returned nothing")]
    IonParserError(#[from] IonParserError),
    #[error("Error when creating the HttpClient")]
    TlsError(#[from] TlsError),
    #[error("Transaction has been already commit or rollback")]
    TransactionCompleted,
    #[error("We weren't able to send the result value to ourselves. This is a bug.")]
    InternalChannelSendError,
    #[error("The statement provided to the count method didn't return what a normal SELECT COUNT(... would have returned.")]
    NonValidCountStatementResult,
    #[error("The transaction is already committed, it cannot be rollback")]
    TransactionAlreadyCommitted,
    #[error("The transaction is already rollback, it cannot be committed")]
    TransactionAlreadyRollback,
    #[error(
        "The query was already executed. Trying to get a Cursor or executing it again will fail."
    )]
    QueryAlreadyExecuted,
    #[error("Error extranting the QLDB returned Ion values to the requested type.")]
    QldbExtractError(#[from] QldbExtractError),
    #[error("Cannot get session from session pool. This means that the session pool was closed by calling the `.close()` method.")]
    SessionPoolClosed(Report),
}

pub type QldbResult<T> = Result<T, QldbError>;

#[derive(Debug, Error)]
pub enum QldbExtractError {
    #[error("Cannot convert the IonValue to the requested type.")]
    BadDataType(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Missing property in the QLDB Document {0}")]
    MissingProperty(String),
    #[error("Not a document. QLDB Documents must be an Ion::Struct, this is a: {0:?}")]
    NotADocument(ion_binary_rs::IonValue),
}

pub type QldbExtractResult<T> = Result<T, QldbExtractError>;
