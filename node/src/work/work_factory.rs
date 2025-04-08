use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tracing::warn;

use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider, Peer},
    Root, WorkNonce,
};
use rsnano_nullable_http_client::Url;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use rsnano_work::{WorkPool, WorkPoolBuilder};

use super::distributed_work_client::DistributedWorkClient;
use tokio::{select, time::timeout};
use tokio_util::sync::CancellationToken;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WorkRequest {
    pub root: Root,
    pub difficulty: u64,
}

impl WorkRequest {
    pub fn new(root: Root, difficulty: u64) -> Self {
        Self { root, difficulty }
    }

    pub fn new_test_instance() -> Self {
        Self::new(Root::from(100), 42)
    }
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

pub struct WorkFactory {
    pub local_work_pool: WorkPool,
    work_client: DistributedWorkClient,
    work_peers: Vec<Peer>,
    timeout: Duration,
    cancel_listener: OutputListenerMt<Root>,
    runtime: tokio::runtime::Handle,
    requests_made: AtomicUsize,
    running: Mutex<Vec<(usize, Root, CancellationToken)>>,
    stopped: AtomicBool,
}

impl WorkFactory {
    fn new(
        work_pool: WorkPool,
        work_client: DistributedWorkClient,
        work_peers: Vec<Peer>,
        timeout: Duration,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            local_work_pool: work_pool,
            work_client,
            work_peers,
            timeout,
            cancel_listener: OutputListenerMt::new(),
            runtime,
            requests_made: AtomicUsize::new(0),
            running: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
        }
    }

    pub fn disabled(runtime: tokio::runtime::Handle) -> Self {
        Self::builder(runtime)
            .local_work_pool(|p| p.disabled())
            .finish()
    }

    pub fn builder(runtime: tokio::runtime::Handle) -> WorkFactoryBuilder {
        WorkFactoryBuilder {
            local_work_pool: None,
            work_peers: Vec::new(),
            runtime,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn generate_work(&self, request: WorkRequest) -> Option<WorkNonce> {
        if self.stopped.load(Ordering::SeqCst) {
            return None;
        }

        if self.work_peers.is_empty() {
            self.generate_local(request)
        } else {
            self.generate_remote_or_local(request)
        }
    }

    fn generate_local(&self, request: WorkRequest) -> Option<WorkNonce> {
        if !self.local_work_pool.work_generation_enabled() {
            warn!("Local work generation is disabled!");
            None
        } else {
            self.local_work_pool
                .generate(request.root, request.difficulty)
        }
    }

    fn generate_remote_or_local(&self, request: WorkRequest) -> Option<WorkNonce> {
        let cancel_token = CancellationToken::new();

        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        self.running
            .lock()
            .unwrap()
            .push((id, request.root, cancel_token.clone()));

        let result = self
            .runtime
            .block_on(self.generate_remote(request.clone(), cancel_token.clone()));

        self.running.lock().unwrap().retain(|(i, _, _)| *i != id);

        if result.is_none() && !cancel_token.is_cancelled() {
            self.generate_local(request)
        } else {
            // TODO check if work is actually valid
            result
        }
    }

    async fn generate_remote(
        &self,
        request: WorkRequest,
        cancel_token: CancellationToken,
    ) -> Option<WorkNonce> {
        let result = AtomicU64::new(0);

        tokio_scoped::scope(|scope| {
            // Query all configured peers
            for peer in &self.work_peers {
                scope.spawn(async {
                    select! {
                        _ = async {
                                let res = self.generate_on_peer(peer.clone(), request.clone()).await;
                                if let Some(work) = res {
                                    result.store(work.into(), Ordering::SeqCst);

                                    // We have a valid result. Cancel all other running queries
                                    cancel_token.cancel();
                                }
                            } =>{ },
                        _ = cancel_token.cancelled() => {}
                    }
                });
            }
        });

        let work = result.load(Ordering::SeqCst);
        if work == 0 {
            None
        } else {
            Some(work.into())
        }
    }

    async fn generate_on_peer(&self, peer: Peer, request: WorkRequest) -> Option<WorkNonce> {
        let Some(url) = Self::work_peer_url(&peer) else {
            warn!("Invalid work peer: \"{}\"", peer);
            return None;
        };

        self.requests_made.fetch_add(1, Ordering::SeqCst);

        let result = timeout(
            self.timeout,
            self.work_client.generate_work(url.clone(), request.clone()),
        )
        .await;

        match result {
            Ok(Ok(work)) => Some(work),
            Ok(Err(e)) => {
                warn!("Work peer returned error: {:?}", e);
                None
            }
            Err(_) => {
                warn!(
                    "Work peer timed out after {} ms: \"{}\"",
                    self.timeout.as_millis(),
                    url.to_string()
                );
                None
            }
        }
    }

    fn work_peer_url(peer: &Peer) -> Option<Url> {
        Url::parse(&format!("http://{}:{}", peer.address, peer.port)).ok()
    }

    pub fn cancel(&self, root: Root) {
        self.cancel_listener.emit(root);
        self.local_work_pool.cancel(&root);
        {
            let to_cancel: Vec<_> = self
                .running
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(_, r, ct)| if *r == root { Some(ct.clone()) } else { None })
                .collect();

            for cancel_token in to_cancel {
                cancel_token.cancel();
            }
        }
    }

    pub fn work_generation_enabled(&self) -> bool {
        self.local_work_pool.work_generation_enabled()
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        let cancel_tokens: Vec<_> = self
            .running
            .lock()
            .unwrap()
            .iter()
            .map(|(_, _, ct)| ct.clone())
            .collect();
        for ct in cancel_tokens {
            ct.cancel();
        }
    }

    pub fn track_cancellations(&self) -> Arc<OutputTrackerMt<Root>> {
        self.cancel_listener.track()
    }

    pub fn requests_made(&self) -> usize {
        self.requests_made.load(Ordering::SeqCst)
    }
}

impl ContainerInfoProvider for WorkFactory {
    fn container_info(&self) -> ContainerInfo {
        self.local_work_pool.container_info()
    }
}

pub struct WorkFactoryBuilder {
    local_work_pool: Option<WorkPool>,
    work_peers: Vec<Peer>,
    timeout: Duration,
    runtime: tokio::runtime::Handle,
}

impl WorkFactoryBuilder {
    pub fn local_work_pool(mut self, f: impl FnOnce(WorkPoolBuilder) -> WorkPoolBuilder) -> Self {
        self.local_work_pool = Some(f(WorkPool::builder()).finish());
        self
    }

    pub fn finish(self) -> WorkFactory {
        let local_work_pool = self.local_work_pool.unwrap_or_default();
        WorkFactory::new(
            local_work_pool,
            DistributedWorkClient::default(),
            self.work_peers,
            self.timeout,
            self.runtime,
        )
    }
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tokio_runner::TokioRunner,
        work::distributed_work_client::{ConfiguredWorkResponse, DistributedWorkClient},
    };
    use rsnano_core::utils::Peer;
    use rsnano_nullable_http_client::Url;
    use std::{thread, time::Duration};
    use tracing_test::traced_test;

    #[test]
    fn use_local_work_pool_when_no_peers_given() {
        let expected_work = WorkNonce::from(12345);
        let (work_factory, _rt) = create_work_factory(TestContext {
            work_pool: WorkPool::new_null(expected_work),
            ..Default::default()
        });
        let request = WorkRequest::new_test_instance();

        let work = work_factory.generate_work(request.clone());

        assert_eq!(work, Some(expected_work));
    }

    #[test]
    fn cancellations_can_be_tracked() {
        let (work_factory, _rt) = create_work_factory(TestContext {
            work_pool: WorkPool::new_null(1.into()),
            ..Default::default()
        });
        let cancel_tracker = work_factory.track_cancellations();

        let root = Root::from(1);
        work_factory.cancel(root);

        assert_eq!(cancel_tracker.output(), vec![root]);
    }

    #[test]
    #[traced_test]
    fn work_generation_disabled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let work_factory = WorkFactory::disabled(runtime.handle().clone());
        let result = work_factory.generate_work(WorkRequest::new_test_instance());
        assert_eq!(result, None);
        assert!(logs_contain("Local work generation is disabled!"))
    }

    #[test]
    fn use_remote_work_server() {
        let expected_work = WorkNonce::new(42);
        let work_client = DistributedWorkClient::new_null_with(expected_work);
        let request_tracker = work_client.track_requests();
        let (work_factory, _rt) = create_work_factory(TestContext {
            work_pool: WorkPool::disabled(),
            work_client,
            work_peers: vec![Peer::new("foo.com", 123)],
            ..Default::default()
        });

        let request = WorkRequest::new_test_instance();
        let work = work_factory.generate_work(request.clone());

        assert_eq!(work, Some(expected_work));
        let output = request_tracker.output();
        assert_eq!(output.len(), 1, "no request sent");
        assert_eq!(
            output[0],
            (Url::parse("http://foo.com:123").unwrap(), request)
        );
    }

    #[test]
    fn when_peer_is_incorrect_should_use_local_work_pool() {
        let expected_work = WorkNonce::new(100);

        let (work_factory, _rt) = create_work_factory(TestContext {
            work_peers: vec![Peer::new("invalid peer", 123)],
            work_pool: WorkPool::new_null(expected_work),
            ..Default::default()
        });

        let result = work_factory.generate_work(WorkRequest::new_test_instance());
        assert_eq!(result, Some(expected_work));
    }

    #[test]
    fn when_work_server_returns_error_use_local_work_pool() {
        let expected_work = WorkNonce::new(100);

        let (work_factory, _rt) = create_work_factory(TestContext {
            work_peers: vec![Peer::new("127.0.0.1", 123)],
            work_pool: WorkPool::new_null(expected_work),
            work_client: DistributedWorkClient::new_failing_null("an error"),
            ..Default::default()
        });

        let result = work_factory.generate_work(WorkRequest::new_test_instance());
        assert_eq!(result, Some(expected_work));
    }

    #[test]
    fn when_timed_out_should_use_local_work_pool() {
        let expected_work = WorkNonce::new(100);

        let (work_factory, _rt) = create_work_factory(TestContext {
            work_peers: vec![Peer::new("127.0.0.1", 123)],
            work_pool: WorkPool::new_null(expected_work),
            work_client: DistributedWorkClient::new_halting_null(),
            timeout: Duration::ZERO,
            ..Default::default()
        });

        let result = work_factory.generate_work(WorkRequest::new_test_instance());

        assert_eq!(result, Some(expected_work));
    }

    #[test]
    fn calls_multiple_peers_and_uses_first_ok_result() {
        let expected_work = WorkNonce::new(100);
        let peer1 = Peer::new("127.0.0.1", 123);
        let peer2 = Peer::new("127.0.0.1", 456);
        let peer3 = Peer::new("127.0.0.1", 789);

        let (work_factory, _rt) = create_work_factory(TestContext {
            work_peers: vec![peer1, peer2, peer3],
            work_pool: WorkPool::disabled(),
            work_client: DistributedWorkClient::null_builder()
                .response(
                    "http://127.0.0.1:123",
                    ConfiguredWorkResponse::Error("failed".to_string()),
                )
                .response("http://127.0.0.1:456", ConfiguredWorkResponse::Halt)
                .response(
                    "http://127.0.0.1:789",
                    ConfiguredWorkResponse::Ok(expected_work),
                )
                .finish(),
            ..Default::default()
        });

        let result = work_factory.generate_work(WorkRequest::new_test_instance());

        assert_eq!(result, Some(expected_work));
    }

    #[test]
    fn cancel() {
        let (work_factory, _rt) = create_work_factory(TestContext {
            work_peers: vec![Peer::new("127.0.0.1", 123)],
            work_client: DistributedWorkClient::new_halting_null(),
            ..Default::default()
        });

        let request = WorkRequest::new_test_instance();

        let mut result = Some(WorkNonce::new(1000));
        std::thread::scope(|scope| {
            scope.spawn(|| result = work_factory.generate_work(request.clone()));
            while work_factory.requests_made() == 0 {
                thread::yield_now();
            }
            work_factory.cancel(request.root);
        });
        assert_eq!(result, None);
    }

    #[test]
    fn when_stopped_should_return_none() {
        let (work_factory, _rt) = create_work_factory(TestContext {
            work_peers: vec![Peer::new("127.0.0.1", 123)],
            work_client: DistributedWorkClient::new_halting_null(),
            ..Default::default()
        });

        work_factory.stop();

        assert_eq!(
            work_factory.generate_work(WorkRequest::new_test_instance()),
            None
        );
    }

    #[test]
    fn cancel_when_stopped() {
        let (work_factory, _rt) = create_work_factory(TestContext {
            work_peers: vec![Peer::new("127.0.0.1", 123)],
            work_client: DistributedWorkClient::new_halting_null(),
            ..Default::default()
        });

        let request = WorkRequest::new_test_instance();

        let mut result = Some(WorkNonce::new(1000));
        std::thread::scope(|scope| {
            scope.spawn(|| result = work_factory.generate_work(request.clone()));
            while work_factory.requests_made() == 0 {
                thread::yield_now();
            }
            work_factory.stop();
        });
        assert_eq!(result, None);
    }

    struct TestContext {
        work_pool: WorkPool,
        work_client: DistributedWorkClient,
        work_peers: Vec<Peer>,
        timeout: Duration,
    }

    impl Default for TestContext {
        fn default() -> Self {
            Self {
                work_pool: WorkPool::new_null(WorkNonce::new(42)),
                work_client: DistributedWorkClient::new_null_with(WorkNonce::new(43)),
                work_peers: Vec::new(),
                timeout: DEFAULT_TIMEOUT,
            }
        }
    }

    fn create_work_factory(context: TestContext) -> (WorkFactory, TokioRunner) {
        let mut runner = TokioRunner::new(1);
        runner.start();

        let factory = WorkFactory::new(
            context.work_pool,
            context.work_client,
            context.work_peers,
            context.timeout,
            runner.handle().clone(),
        );
        (factory, runner)
    }
}
