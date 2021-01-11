use crossbeam_utils::atomic::AtomicCell;
use futures::lock::Mutex;
use futures::task::AtomicWaker;
use futures::task::Context;
use futures::task::Poll;
use futures::Future;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct InnerSession {
    created_on_instant: Instant,
    session_id: String,
}

impl Drop for InnerSession {
    fn drop(&mut self) {
        // TODO: The inner cell should have a reference to
        // the counter of available sessions. An AtmicU64.
    }
}

#[derive(Debug, Clone)]
struct Session {
    inner: Arc<InnerSession>,
}

impl Session {
    fn get_session_id(&self) -> &str {
        &self.inner.session_id
    }

    fn get_age(&self, now: Instant) -> Duration {
        self.inner.created_on_instant - now
    }
}

#[derive(Debug)]
struct SessionPoolInner {
    pool: Mutex<VecDeque<Session>>,
    count: AtomicU64,
    awaiting_for_connection: Mutex<Vec<SessionAwaiter>>,
}

#[derive(Debug, Clone)]
struct SessionPool {
    inner: Arc<SessionPoolInner>,
}

impl SessionPool {
    async fn get_session(&self) -> Session {
        let mut pool = self.inner.pool.lock().await;

        match pool.pop_front() {
            Some(session) => session,
            None => {
                drop(pool);

                // Check if we already reached the max connections.
                // If so, await
                // If not yet, create a new session.

                self.wait_until_session_is_available().await
            }
        }
    }

    async fn return_session(&self, session: Session) {
        // Check if the session is still valid
        // Check if there is anyone waiting for sessions
        // If so, give the connection
    }

    pub async fn refill(&self) {
        // Check old connection and remove them
        // Request new connections. Order the connections so the oldest are first.
        // Check if there is any waker waiting for connections
        // If so, give them connections until exhausted.
        // Return
        todo!()
    }

    async fn wait_until_session_is_available(&self) -> Session {
        let waker = SessionAwaiter::new();

        self.inner
            .awaiting_for_connection
            .lock()
            .await
            .push(waker.clone());

        waker.await
    }
}

#[derive(Clone)]
struct SessionAwaiter {
    session: Arc<AtomicCell<Option<Session>>>,
    waker: Arc<AtomicWaker>,
}

impl SessionAwaiter {
    fn new() -> SessionAwaiter {
        SessionAwaiter {
            session: Arc::new(AtomicCell::new(None)),
            waker: Arc::new(AtomicWaker::new()),
        }
    }

    fn get_waker(&self) -> &AtomicWaker {
        &self.waker
    }

    async fn awake(&self, session: Session) {
        self.session.store(Some(session));
    }
}

impl Future for SessionAwaiter {
    type Output = Session;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(session) = self.session.swap(None) {
            Poll::Ready(session)
        } else {
            self.waker.register(cx.waker());
            Poll::Pending
        }
    }
}
