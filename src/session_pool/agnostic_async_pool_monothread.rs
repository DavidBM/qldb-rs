use crate::session_pool::agnostic_async_pool_shared::{create_session, provide_session, qldb_close_session};
use crate::session_pool::{Session, SpawnerFnMonothread};
use async_channel::Receiver;
use async_channel::Sender;
use async_io::Timer;
use log::error;
use rusoto_qldb_session::QldbSessionClient;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering::Relaxed};
use std::sync::Arc;
use std::{cell::RefCell, rc::Rc, time::Duration};

#[allow(clippy::too_many_arguments)]
pub fn receiver_task(
    spawner: SpawnerFnMonothread,
    max_sessions: u16,
    ledger_name: &str,
    sessions: &Rc<RefCell<VecDeque<Session>>>,
    session_count: &Rc<AtomicU16>,
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
                let (session, pooled_sessions_count) = if let Ok(mut sessions) = sessions.try_borrow_mut() {
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
    spawner: SpawnerFnMonothread,
    sessions: &Rc<RefCell<VecDeque<Session>>>,
    session_count: &Rc<AtomicU16>,
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
            } else if let Ok(mut sessions) = sessions.try_borrow_mut() {
                sessions.push_front(session);
            } else {
                // Should never happens as the executor is single thread and
                // the sessions should never be borrowed at the same time
                close_session(spawner.clone(), &qldb_client, session, &session_count)
            }
        }
    }));
}

fn close_session(
    spawner: SpawnerFnMonothread,
    qldb_client: &Arc<QldbSessionClient>,
    session: Session,
    session_count: &Rc<AtomicU16>,
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

fn requeue_session_request(session_requests: &Sender<Sender<Session>>, sender: Sender<Session>) {
    if let Err(err) = session_requests.try_send(sender) {
        error!(
            "QLDB driver internal error. Cannot enqueue session due pool bug: {:?}",
            err
        );
    }
}
