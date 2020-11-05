use ion_binary_rs::IonParserError;
use rusoto_core::RusotoError;
use rusoto_qldb_session::SendCommandError;

#[derive(Debug)]
pub enum QLDBError {
    SendCommandError(RusotoError<SendCommandError>),
    QLDBReturnedEmptySession,
    QLDBReturnedEmptyTransaction,
    IonParserError(IonParserError),
}

impl From<RusotoError<SendCommandError>> for QLDBError {
    fn from(err: RusotoError<SendCommandError>) -> QLDBError {
        QLDBError::SendCommandError(err)
    }
}

pub type QLDBResult<T> = Result<T, QLDBError>;
