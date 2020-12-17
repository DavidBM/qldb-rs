use ion_binary_rs::IonParserError;
use rusoto_core::{request::TlsError, RusotoError};
use rusoto_qldb_session::SendCommandError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QLDBError {
    #[error("The QLDB command returned an error")]
    SendCommandError(#[from] RusotoError<SendCommandError>),
    #[error("We requested a session but QLDB returned nothing")]
    QLDBReturnedEmptySession,
    #[error("We requested a transaction id but QLDB returned nothing")]
    QLDBReturnedEmptyTransaction,
    // TODO: IonParserError seems to not be compatible
    // with "#[from]". Make it compatible
    #[error("We requested a transaction id but QLDB returned nothing")]
    IonParserError(IonParserError),
    #[error("Error when creating the HttpClient")]
    TlsError(#[from] TlsError),
    #[error("Transaction has been already commit or rollback")]
    TransactionCompleted,
    #[error("We weren't able to send the result value to ourselves. This is a bug.")]
    InternalChannelSendError,
    #[error("The statement provided to the count method didn't return what a normal SELECT COUNT(... would have returned.")]
    NonValidCountStatementResult,
    #[error("The transaction is already commited, it cannot be rollback")]
    TransactionAlreadyCommitted,
    #[error("The transaction is already rollback, it cannot be committed")]
    TransactionAlreadyRollback,
}

pub type QLDBResult<T> = Result<T, QLDBError>;

#[derive(Debug, Error)]
pub enum QLDBExtractError {
    #[error("Bad data type")]
    BadDataType(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Missing property {0}")]
    MissingProperty(String),
    #[error("Not a document")]
    NotADocument(ion_binary_rs::IonValue),
    #[error("Overflow")]
    Overflow,
}

pub type QLDBExtractResult<T> = Result<T, QLDBExtractError>;
