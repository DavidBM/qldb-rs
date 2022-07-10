use crate::session_pool::{Session, SessionPool, SpawnerFnMonoMultithread, agnostic_async_pool_multithread::{returning_task, receiver_task}};
use rusoto_qldb_session::QldbSessionClient;
use async_channel::{bounded, unbounded, Sender};
use eyre::WrapErr;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct SpawnerSessionPool {
    sender_request: Sender<Sender<Session>>,
    sender_return: Sender<Session>,
    is_closed: Arc<AtomicBool>,
}

impl SpawnerSessionPool {
    pub fn new(qldb_client: Arc<QldbSessionClient>, ledger_name: &str, max_sessions: u16, spawner: SpawnerFnMonoMultithread) -> SpawnerSessionPool {
        let (requesting_sender, requesting_receiver) = unbounded::<Sender<Session>>();
        let (returning_sender, returning_receiver) = unbounded::<Session>();
        let ledger_name = ledger_name.to_owned();

        let is_closed = Arc::new(AtomicBool::from(false));

        let is_closed_return = is_closed.clone();
        let requesting_sender_return = requesting_sender.clone();

        let sessions = Arc::new(Mutex::new(VecDeque::<Session>::with_capacity(max_sessions.into())));
        let session_count = Arc::new(AtomicU16::new(0));

        receiver_task(
            spawner.clone(),
            max_sessions,
            &ledger_name,
            &sessions,
            &session_count,
            &qldb_client,
            &is_closed,
            requesting_receiver,
            requesting_sender
        );

        returning_task(
            spawner,
            &sessions,
            &session_count,
            &qldb_client,
            &is_closed,
            returning_receiver,
        );

        SpawnerSessionPool {
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
        // TODO: We maybe shouldn't be ignoring this error
        let _ = self.sender_return.try_send(session);
    }
}

#[async_trait::async_trait]
impl SessionPool for SpawnerSessionPool {
    async fn close(&self) {
        self.close().await
    }

    async fn get(&self) -> eyre::Result<Session> {
        self.get().await
    }

    fn give_back(&self, session: Session) {
        self.give_back(session)
    }
}
