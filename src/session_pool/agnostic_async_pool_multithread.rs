use crate::session_pool::{GetSessionError, Session, SpawnerFnMonoMultithread};
use async_channel::Receiver;
use async_channel::Sender;
use async_compat::CompatExt;
use async_io::Timer;
use log::error;
use rusoto_core::RusotoError;
use rusoto_qldb_session::{EndSessionRequest, QldbSession, QldbSessionClient, SendCommandRequest, StartSessionRequest};
use std::collections::VecDeque;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{
    atomic::{AtomicBool, AtomicU16},
    Arc, Mutex,
};
use std::time::Duration;

#[allow(clippy::too_many_arguments)]
pub fn receiver_task(
    spawner: SpawnerFnMonoMultithread,
    max_sessions: u16,
    ledger_name: &str,
    sessions: &Arc<Mutex<VecDeque<Session>>>,
    session_count: &Arc<AtomicU16>,
    qldb_client: &Arc<QldbSessionClient>,
    is_closed: &Arc<AtomicBool>,
    requesting_receiver: Receiver<Sender<Session>>,
    requesting_sender: Sender<Sender<Session>>,
) {
    let is_closed = is_closed.clone();
    let qldb_client = qldb_client.clone();
    let sessions = sessions.clone();
    let session_count = session_count.clone();
    let ledger_name = ledger_name.to_owned();

    spawner.clone()(Box::pin(async move {
        while let Ok(sender) = requesting_receiver.recv().await {
            if is_closed.load(Relaxed) {
                break;
            }

            loop {
                let (session, pooled_sessions_count) = match sessions.lock() {
                    Ok(mut sessions) => (sessions.pop_back(), sessions.len()),
                    Err(err) => {
                        // Means that something went really wrong
                        is_closed.store(true, Relaxed);
                        error!("QLDB driver internal fatal error. Cannot get lock at sessions when requesting a session: {:?}", err);
                        break;
                    }
                };

                if let Some(session) = session {
                    if session.is_valid() {
                        provide_session(&sender, session);
                        break;
                    } else {
                        close_session(spawner.clone(), &qldb_client, session, &session_count);
                        // Continue so we try next available session
                        continue;
                    }
                } else {
                    if pooled_sessions_count < max_sessions.into() {
                        refill_session(&qldb_client.clone(), &ledger_name, &sessions).await;
                        continue;
                    } else {
                        requeue_session_request(&requesting_sender, sender);
                    }
                    break;
                }
            }
        }
    }));
}

pub fn returning_task(
    spawner: SpawnerFnMonoMultithread,
    sessions: &Arc<Mutex<VecDeque<Session>>>,
    session_count: &Arc<AtomicU16>,
    qldb_client: &Arc<QldbSessionClient>,
    is_closed: &Arc<AtomicBool>,
    returning_receiver: Receiver<Session>,
) {
    let is_closed = is_closed.clone();
    let qldb_client = qldb_client.clone();
    let sessions = sessions.clone();
    let session_count = session_count.clone();

    spawner.clone()(Box::pin(async move {
        while let Ok(session) = returning_receiver.recv().await {
            if is_closed.load(Relaxed) {
                break;
            }

            if !session.is_valid() {
                close_session(spawner.clone(), &qldb_client, session, &session_count);
                break;
            }

            match sessions.lock() {
                Ok(mut sessions) => sessions.push_front(session),
                Err(err) => {
                    // Means that something went really wrong
                    is_closed.store(true, Relaxed);
                    error!(
                        "QLDB driver internal fatal error. Cannot get lock at sessions when returning a session: {:?}",
                        err
                    );
                    close_session(spawner.clone(), &qldb_client, session, &session_count);
                    break;
                }
            };
        }
    }));
}

fn close_session(
    spawner: SpawnerFnMonoMultithread,
    qldb_client: &Arc<QldbSessionClient>,
    session: Session,
    session_count: &Arc<AtomicU16>,
) {
    let qldb_client = qldb_client.clone();
    let session_count = session_count.clone();

    spawner(Box::pin(async move {
        let mut tries: u32 = 0;

        loop {
            tries = tries.saturating_add(1);

            match qldb_close_session(&qldb_client, &session).await {
                Ok(_) => break,
                Err(_) if tries > 10 => break,
                Err(_) => {
                    Timer::after(Duration::from_millis(
                        tries.saturating_mul(tries).saturating_mul(75).into(),
                    ))
                    .await;
                }
            }
        }

        session_count.store(session_count.load(Relaxed).saturating_sub(1), Relaxed);
    }));
}

fn provide_session(sender: &Sender<Session>, session: Session) {
    // This channel should never be full or closed
    if let Err(err) = sender.try_send(session) {
        error!(
            "QLDB driver internal error. Cannot return session due to channel issue: {:?}",
            err
        );
    }
}

async fn refill_session(
    qldb_client: &Arc<QldbSessionClient>,
    ledger_name: &str,
    sessions: &Arc<Mutex<VecDeque<Session>>>,
) {
    if let Ok(session) = create_session(&qldb_client.clone(), ledger_name).await {
        if let Ok(mut sessions) = sessions.lock() {
            sessions.push_back(session);
        }
    }
}

fn requeue_session_request(session_requests: &Sender<Sender<Session>>, sender: Sender<Session>) {
    if let Err(err) = session_requests.try_send(sender) {
        error!(
            "QLDB driver internal error. Cannot enqueue session due pool bug: {:?}",
            err
        );
    }
}

async fn create_session(qldb_client: &Arc<QldbSessionClient>, ledger_name: &str) -> Result<Session, GetSessionError> {
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

async fn qldb_close_session(qldb_client: &QldbSessionClient, session: &Session) -> Result<(), eyre::Report> {
    qldb_client
        .send_command(SendCommandRequest {
            session_token: Some(session.get_session_id().to_string()),
            end_session: Some(EndSessionRequest {}),
            ..Default::default()
        })
        .await?;

    Ok(())
}

async fn qldb_request_session(qldb_client: &QldbSessionClient, ledger_name: &str) -> Result<String, GetSessionError> {
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
