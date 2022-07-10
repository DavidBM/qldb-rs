use crate::session_pool::{GetSessionError, Session};
use async_channel::Sender;
use async_compat::CompatExt;
use async_io::Timer;
use log::error;
use rusoto_core::RusotoError;
use rusoto_qldb_session::{EndSessionRequest, QldbSession, QldbSessionClient, SendCommandRequest, StartSessionRequest};
use std::time::Duration;

pub(crate) fn provide_session(sender: &Sender<Session>, session: Session) {
    // This channel should never be full or closed
    if let Err(err) = sender.try_send(session) {
        error!(
            "QLDB driver internal error. Cannot return session due to channel issue: {:?}",
            err
        );
    }
}

pub(crate) async fn create_session(
    qldb_client: &QldbSessionClient,
    ledger_name: &str,
) -> Result<Session, GetSessionError> {
    let mut tries: u32 = 0;

    let session = loop {
        tries = tries.saturating_add(1);

        match qldb_request_session(qldb_client, ledger_name).await {
            Ok(session) => break Ok(session),
            Err(error) if tries > 10 => break Err(error),
            Err(GetSessionError::Recoverable(_)) => {
                Timer::after(Duration::from_millis(
                    tries.saturating_mul(tries).saturating_mul(75).into(),
                ))
                .await;
            }
            err @ Err(GetSessionError::Unrecoverable(_)) => break err,
        }
    }?;

    Ok(Session::new(session))
}

pub(crate) async fn qldb_close_session(qldb_client: &QldbSessionClient, session: &Session) -> Result<(), eyre::Report> {
    qldb_client
        .send_command(SendCommandRequest {
            session_token: Some(session.get_session_id().to_string()),
            end_session: Some(EndSessionRequest {}),
            ..Default::default()
        })
        .await?;

    Ok(())
}

pub(crate) async fn qldb_request_session(
    qldb_client: &QldbSessionClient,
    ledger_name: &str,
) -> Result<String, GetSessionError> {
    match qldb_client
        .send_command(SendCommandRequest {
            start_session: Some(StartSessionRequest {
                ledger_name: ledger_name.to_string(),
            }),
            ..Default::default()
        })
        .compat()
        .await
    {
        Ok(response) => match response.start_session {
            Some(session) => match session.session_token {
                Some(token) => Ok(token),
                None => Err(GetSessionError::Unrecoverable(eyre::eyre!(
                    "No session present on QLDB response"
                ))),
            },
            None => Err(GetSessionError::Unrecoverable(eyre::eyre!(
                "Empty session on QLDB response"
            ))),
        },
        Err(err) => match err {
            RusotoError::Credentials(_) => Err(GetSessionError::Unrecoverable(eyre::eyre!(err))),
            _ => Err(GetSessionError::Recoverable(eyre::eyre!(err))),
        },
    }
}
