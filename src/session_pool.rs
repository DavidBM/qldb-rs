use async_channel::{bounded, unbounded, RecvError, Sender, TrySendError};
use async_compat::CompatExt;
use async_executor::LocalExecutor;
use async_io::Timer;
use futures_lite::future;
use rusoto_core::RusotoError;
use rusoto_qldb_session::{
    EndSessionRequest, QldbSession, QldbSessionClient, SendCommandRequest, StartSessionRequest,
};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct InnerSession {
    created_on_instant: Instant,
    session_id: String,
}

#[derive(Debug, Clone)]
pub struct Session {
    inner: Arc<InnerSession>,
}

impl Session {
    pub fn new(session_id: String) -> Session {
        Session {
            inner: Arc::new(InnerSession {
                created_on_instant: Instant::now(),
                session_id,
            }),
        }
    }

    pub fn get_session_id(&self) -> &str {
        &self.inner.session_id
    }

    pub fn is_valid(&self) -> bool {
        self.inner.created_on_instant.elapsed().as_secs() < 10 * 60
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SessionRequestError {
    #[error("Cannot request a session. {0}")]
    SendError(#[from] TrySendError<PoolCommand>),
    #[error("Cannot receive a session after a successful request. {0}")]
    RecvError(#[from] RecvError),
}

#[derive(Debug, thiserror::Error)]
pub enum SessionReturnError {
    #[error("Cannot return a session. {0}")]
    SendError(#[from] TrySendError<PoolCommand>),
}

#[derive(Debug)]
pub enum PoolCommand {
    Request(Sender<Session>),
    Return(Session),
}

#[derive(Debug, Clone)]
pub struct SessionPool {
    sender: Sender<PoolCommand>,
}

impl SessionPool {
    pub fn new(
        qldb_client: Arc<QldbSessionClient>,
        ledger_name: &str,
        max_sessions: u16,
    ) -> SessionPool {
        let (sender, receiver) = unbounded::<PoolCommand>();

        let sender_delayed = sender.clone();
        let ledger_name = ledger_name.to_owned();

        std::thread::spawn(move || {
            let executor = LocalExecutor::new();

            let task =
                executor.spawn(async move {
                    let mut sessions = VecDeque::<Session>::new();
                    let mut session_requests = VecDeque::<Sender<Session>>::new();
                    let mut active_session_count: u16 = 0;

                    'queue_recv: loop {
                        match receiver.recv().await {
                            Ok(PoolCommand::Return(session)) => {
                                if !session.is_valid() {
                                    close_session(&qldb_client, &session).await;
                                    active_session_count = active_session_count.saturating_sub(1);
                                    continue;
                                }

                                if let Some(sender) = session_requests.pop_back() {
                                    if sender.try_send(session).is_err() {
                                        break;
                                    }
                                } else {
                                    sessions.push_front(session);
                                }
                            }
                            Ok(PoolCommand::Request(sender)) => 'session_pop: loop {
                                match sessions.pop_back() {
                                    Some(session) => {
                                        if !session.is_valid() {
                                            close_session(&qldb_client, &session).await;
                                            continue 'session_pop;
                                        }

                                        if let Err(error) = sender.try_send(session) {
                                            let session = match error {
                                                TrySendError::Full(session) => session,
                                                TrySendError::Closed(session) => session,
                                            };

                                            sessions.push_front(session);

                                            break 'session_pop;
                                        }

                                        break 'session_pop;
                                    }
                                    None => {
                                        if active_session_count < max_sessions {
                                            let session =
                                                match create_session(&qldb_client, &ledger_name)
                                                    .await
                                                {
                                                    Ok(session) => session,
                                                    Err(_) => {
                                                        // If it isn't possible to get a connection, we
                                                        // enqueue the request again hoping for it to be
                                                        // processed later successfully.
                                                        if sender_delayed
                                                            .try_send(PoolCommand::Request(sender))
                                                            .is_err()
                                                        {
                                                            break 'queue_recv;
                                                        } else {
                                                            break 'session_pop;
                                                        }
                                                    }
                                                };

                                            active_session_count =
                                                active_session_count.saturating_add(1);

                                            if let Err(error) = sender.try_send(session) {
                                                let session = match error {
                                                    TrySendError::Full(session) => session,
                                                    TrySendError::Closed(session) => session,
                                                };

                                                sessions.push_front(session);
                                            }

                                            break 'session_pop;
                                        } else {
                                            session_requests.push_front(sender);
                                            break 'session_pop;
                                        }
                                    }
                                }
                            },
                            Err(_) => {
                                break;
                            }
                        }
                    }
                });

            future::block_on(executor.run(task));
        });

        SessionPool { sender }
    }

    pub async fn get(&self) -> Result<Session, SessionRequestError> {
        let (sender, receiver) = bounded::<Session>(1);

        self.sender.try_send(PoolCommand::Request(sender))?;

        let session = receiver.recv().await?;

        Ok(session)
    }

    pub fn give_back(&self, session: Session) -> Result<(), SessionReturnError> {
        self.sender.try_send(PoolCommand::Return(session))?;

        Ok(())
    }
}

async fn create_session(
    qldb_client: &Arc<QldbSessionClient>,
    ledger_name: &str,
) -> Result<Session, GetSessionError> {
    let mut tries: u32 = 0;

    let session = loop {
        tries = tries.saturating_add(1);

        match qldb_request_session(qldb_client, ledger_name).await {
            Ok(session) => break Ok(session),
            Err(error) if tries > 10 => break Err(error),
            Err(GetSessionError::Recoverable(_)) => {
                Timer::after(Duration::from_millis(tries.pow(tries * 10).into())).await;
            }
            Err(GetSessionError::Unrecoverable(error)) => {
                break Err(GetSessionError::Unrecoverable(error))
            }
        }
    }?;

    Ok(Session::new(session))
}

#[derive(Debug, thiserror::Error)]
enum GetSessionError {
    #[error("The QLDB command returned an error")]
    Unrecoverable(eyre::Report),
    #[error("The QLDB command returned an error")]
    Recoverable(eyre::Report),
}

async fn qldb_request_session(
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
                None => {
                    return Err(GetSessionError::Unrecoverable(eyre::eyre!(
                        "No session present on QLDB response"
                    )))
                }
            },
            None => {
                return Err(GetSessionError::Unrecoverable(eyre::eyre!(
                    "Empty session on QLDB response"
                )))
            }
        },
        Err(err) => match err {
            RusotoError::Credentials(_) => Err(GetSessionError::Unrecoverable(eyre::eyre!(err))),
            _ => Err(GetSessionError::Recoverable(eyre::eyre!(err))),
        },
    }
}

async fn close_session(
    qldb_client: &Arc<QldbSessionClient>,
    session: &Session,
) {
    let mut tries: u32 = 0;

    let _ = loop {
        tries = tries.saturating_add(1);

        match qldb_close_session(qldb_client, session).await {
            Ok(session) => break Ok(session),
            Err(error) if tries > 10 => break Err(error),
            Err(_) => {
                Timer::after(Duration::from_millis(tries.pow(tries * 10).into())).await;
            }
        }
    };
}

async fn qldb_close_session(
    qldb_client: &QldbSessionClient,
    session: &Session,
) -> Result<(), eyre::Report> {
    qldb_client
        .send_command(SendCommandRequest {
            session_token: Some(session.get_session_id().to_string()),
            end_session: Some(EndSessionRequest {}),
            ..Default::default()
        })
        .await?;

    Ok(())
}
