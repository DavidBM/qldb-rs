use crossbeam_utils::atomic::AtomicCell;
use futures::{Future, task::{AtomicWaker, Context, Poll}, lock::Mutex};
use std::hash::Hash;
use std::collections::{VecDeque, BTreeSet};
use std::pin::Pin;
use std::sync::{Arc, atomic::{AtomicU64, Ordering::Relaxed}};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct InnerSession {
    created_on_instant: Instant,
    session_id: String,
}

#[derive(Debug, Clone)]
struct Session {
    inner: Arc<InnerSession>,
}

impl PartialEq for Session {
    fn eq(&self, other: &Session) -> bool {
        self.inner.session_id == other.inner.session_id
    }
}

impl Session {
    pub fn new(session_id: String) -> Session {
        Session {
            inner: Arc::new(InnerSession {
                created_on_instant: Instant::now(),
                session_id
            })
        }
    }

    fn get_session_id(&self) -> &str {
        &self.inner.session_id
    }

    fn get_age(&self) -> Duration {
        self.inner.created_on_instant.elapsed()
    }
}

#[derive(Debug)]
struct SessionPoolInner {
    pool: Mutex<VecDeque<Session>>,
    count: AtomicU64,
    awaiting_for_session: Mutex<BTreeSet<SessionAwaiter>>,
    max_sessions: u16,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionPool {
    inner: Arc<SessionPoolInner>,
}

impl SessionPool {
    pub fn new(max_sessions: u16) -> SessionPool {
        SessionPool {
            inner: Arc::new(SessionPoolInner {
                pool: Mutex::new(VecDeque::new()),
                count: AtomicU64::new(0),
                awaiting_for_session: Mutex::new(BTreeSet::new()),
                max_sessions
            }),
        }
    }

    async fn get_session(&self) -> Session {
        let mut pool = self.inner.pool.lock().await;

        match pool.pop_front() {
            Some(session) => session,
            None => {
                drop(pool);

                if self.inner.count.load(Relaxed) < self.inner.max_sessions.into() {
                    let session = self.create_session().await;
                    return session;
                }

                self.wait_until_session_is_available().await
            }
        }
    }

    async fn return_session(&self, session: InnerSession) {
        let session_age = session.created_on_instant.elapsed();

        if session_age > Duration::from_secs(15) {
            drop(session);
            return;
        }

        let mut pool = self.inner.pool.lock().await;

        let session = Session {
            inner: Arc::new(session)
        };

        if pool.contains(&session) {
            self.close_session().await;
            return;
        }

        let mut connection_awaiters = self.inner
            .awaiting_for_session
            .lock()
            .await;

        if let Some(awaiter) = connection_awaiters.iter().next() {
            let awaiter = awaiter.clone();
            connection_awaiters.remove(&awaiter);
            awaiter.awake(session);
            return;
        }

        pool.push_back(session);
    }

    async fn create_session(&self) -> Session {
        // Creates a session with the QLDB
        self.inner.count.fetch_add(1, Relaxed);

        // TODO! Make the call to QLDB as in client::QLDBSession::get_session
        Session::new("".to_string())
    }

    async fn close_session(&self) -> Session {
        todo!()
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
            .awaiting_for_session
            .lock()
            .await
            .insert(waker.clone());

        let session = waker.clone().await;

        self.inner
            .awaiting_for_session
            .lock()
            .await
            .remove(&waker);

        session
    }
}

#[derive(Clone)]
struct SessionAwaiter {
    session: Arc<AtomicCell<Option<Session>>>,
    waker: Arc<AtomicWaker>,
    id: u64,
    created_on: Instant
}

impl PartialEq for SessionAwaiter {
    fn eq(&self, other: &SessionAwaiter) -> bool {
        self.id == other.id
    }
}

impl Eq for SessionAwaiter {}

impl Hash for SessionAwaiter {
    fn hash<H>(&self, hasher: &mut H) where H: std::hash::Hasher {
        hasher.write_u64(self.id);
    }
}

impl PartialOrd for SessionAwaiter {
    fn partial_cmp(&self, other: &SessionAwaiter) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SessionAwaiter {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.created_on.cmp(&other.created_on)
    }
}

impl SessionAwaiter {
    fn new() -> SessionAwaiter {
        SessionAwaiter {
            session: Arc::new(AtomicCell::new(None)),
            waker: Arc::new(AtomicWaker::new()),
            id: rand::random(),
            created_on: Instant::now(),
        }
    }

    fn get_waker(&self) -> &AtomicWaker {
        &self.waker
    }

    fn awake(&self, session: Session) {
        self.session.store(Some(session));
        self.waker.wake();
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
