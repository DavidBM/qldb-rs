mod session_pool_thread;
mod agnostic_async_pool_monothread;
mod agnostic_async_pool_multithread;
#[cfg(feature = "internal_pool_with_spawner")]
mod session_pool_spawner;

use std::pin::Pin;
use std::sync::Arc;
use std::{time::Instant, future::Future};
pub use session_pool_thread::ThreadedSessionPool;
#[cfg(feature = "internal_pool_with_spawner")]
pub use session_pool_spawner::SpawnerSessionPool;
use log::error;

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
enum GetSessionError {
    #[error("The QLDB command returned an error")]
    Unrecoverable(eyre::Report),
    #[error("The QLDB command returned an error")]
    Recoverable(eyre::Report),
}

#[async_trait::async_trait]
pub trait SessionPool {
    async fn close(&self);

    async fn get(&self) -> eyre::Result<Session>;

    fn give_back(&self, session: Session);
}

pub type SpawnerFnMonothread = Arc<dyn Fn(Pin<Box<dyn Future<Output = ()>>>)>;

pub type SpawnerFnMonoMultithread = Arc<dyn Fn(Pin<Box<dyn Future<Output = ()> + Send>>) + Send + Sync>;
