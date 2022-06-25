use async_channel::{bounded, unbounded, Sender};
use async_compat::CompatExt;
use async_executor::LocalExecutor;
use async_io::Timer;
use eyre::WrapErr;
use log::error;
use rusoto_core::RusotoError;
use rusoto_qldb_session::{EndSessionRequest, QldbSession, QldbSessionClient, SendCommandRequest, StartSessionRequest};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::{
    atomic::{AtomicU16, Ordering::Relaxed},
    Arc,
};
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

#[derive(Debug, Clone)]
pub struct SessionPool {
    sender_request: Sender<Sender<Session>>,
    sender_return: Sender<Session>,
    is_closed: Arc<AtomicBool>,
}

impl SessionPool {
    pub fn new(qldb_client: Arc<QldbSessionClient>, ledger_name: &str, max_sessions: u16) -> SessionPool {
        let (requesting_sender, requesting_receiver) = unbounded::<Sender<Session>>();
        let (returning_sender, returning_receiver) = unbounded::<Session>();
        let ledger_name = ledger_name.to_owned();

        let is_closed = Arc::new(AtomicBool::from(false));

        let is_closed_return = is_closed.clone();
        let requesting_sender_return = requesting_sender.clone();

        std::thread::spawn(move || {
            let executor = Rc::new(LocalExecutor::new());
            let sessions = Rc::new(RefCell::new(VecDeque::<Session>::with_capacity(max_sessions.into())));
            let session_count = Rc::new(AtomicU16::new(0));

            executor
                .spawn({
                    let executor = executor.clone();
                    let is_closed = is_closed.clone();
                    let qldb_client = qldb_client.clone();
                    let sessions = sessions.clone();
                    let session_count = session_count.clone();
                    let max_sessions = max_sessions;

                    async move {
                        while let Ok(sender) = requesting_receiver.recv().await {
                            if is_closed.load(Relaxed) {
                                break;
                            }

                            loop {
                                let (session, pooled_sessions_count) =
                                    if let Ok(mut sessions) = sessions.try_borrow_mut() {
                                        (sessions.pop_back(), sessions.len())
                                    } else {
                                        // Should never happens as the executor is single thread and
                                        // the sessions should never be borrowed at the same time
                                        requeue_session_request(&requesting_sender, sender);
                                        break;
                                    };

                                if let Some(session) = session {
                                    if session.is_valid() {
                                        provide_session(&sender, session);
                                        break;
                                    } else {
                                        close_session(&executor, &qldb_client, session, &session_count);
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
                    }
                })
                .detach();

            executor
                .spawn({
                    let executor = executor.clone();
                    let sessions = sessions;
                    let session_count = session_count;
                    let is_closed = is_closed.clone();
                    let qldb_client = qldb_client.clone();
                    async move {
                        while let Ok(session) = returning_receiver.recv().await {
                            if is_closed.load(Relaxed) {
                                break;
                            }

                            if !session.is_valid() {
                                close_session(&executor, &qldb_client, session, &session_count);
                            } else if let Ok(mut sessions) = sessions.try_borrow_mut() {
                                sessions.push_front(session);
                            } else {
                                // Should never happens as the executor is single thread and
                                // the sessions should never be borrowed at the same time
                                close_session(&executor, &qldb_client, session, &session_count)
                            }
                        }
                    }
                })
                .detach();

            futures::executor::block_on(executor.run(futures::future::pending::<()>()));
        });

        SessionPool {
            sender_request: requesting_sender_return,
            sender_return: returning_sender,
            is_closed: is_closed_return,
        }
    }

    pub async fn close(&self) {
        self.is_closed.store(true, Relaxed);
    }

    pub async fn get(&self) -> eyre::Result<Session> {
        let (sender, receiver) = bounded::<Session>(1);

        self.sender_request.try_send(sender).wrap_err("Session pool closed")?;

        let session = receiver.recv().await.wrap_err("Session pool closed")?;

        Ok(session)
    }

    pub fn give_back(&self, session: Session) {
        let _ = self.sender_return.try_send(session);
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

#[derive(Debug, thiserror::Error)]
enum GetSessionError {
    #[error("The QLDB command returned an error")]
    Unrecoverable(eyre::Report),
    #[error("The QLDB command returned an error")]
    Recoverable(eyre::Report),
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

async fn refill_session(
    qldb_client: &Arc<QldbSessionClient>,
    ledger_name: &str,
    sessions: &Rc<RefCell<VecDeque<Session>>>,
) {
    if let Ok(session) = create_session(&qldb_client.clone(), ledger_name).await {
        if let Ok(mut sessions) = sessions.try_borrow_mut() {
            sessions.push_back(session);
        }
    }
}

fn close_session(
    executor: &Rc<LocalExecutor<'_>>,
    qldb_client: &Arc<QldbSessionClient>,
    session: Session,
    session_count: &Rc<AtomicU16>,
) {
    let executor = executor.clone();
    let qldb_client = qldb_client.clone();
    let session_count = session_count.clone();

    executor
        .spawn(async move {
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
        })
        .detach();
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

fn requeue_session_request(session_requests: &Sender<Sender<Session>>, sender: Sender<Session>) {
    if let Err(err) = session_requests.try_send(sender) {
        error!(
            "QLDB driver internal error. Cannot enqueue session due pool bug: {:?}",
            err
        );
    }
}
