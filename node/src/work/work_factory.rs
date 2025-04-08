use std::sync::Arc;

use super::distributed_work_client::DistributedWorkClient;
use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider, Peer},
    Root, WorkNonce,
};
use rsnano_nullable_http_client::Url;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use rsnano_work::{WorkPool, WorkPoolBuilder};

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

pub struct WorkFactory {
    pub local_work_pool: WorkPool,
    work_client: DistributedWorkClient,
    work_peers: Vec<Peer>,
    cancel_listener: OutputListenerMt<Root>,
    runtime: tokio::runtime::Handle,
}

impl WorkFactory {
    fn new(
        work_pool: WorkPool,
        work_client: DistributedWorkClient,
        work_peers: Vec<Peer>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            local_work_pool: work_pool,
            work_client,
            work_peers,
            cancel_listener: OutputListenerMt::new(),
            runtime,
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
        }
    }

    pub fn generate_work(&self, request: WorkRequest) -> Option<WorkNonce> {
        if self.work_peers.is_empty() {
            self.local_work_pool
                .generate(request.root, request.difficulty)
        } else {
            // TODO logging
            let url: Url = format!(
                "http://{}:{}",
                self.work_peers[0].address, self.work_peers[0].port
            )
            .parse()
            .ok()?;

            // TODO logging
            self.runtime
                .block_on(self.work_client.generate_work(url, request))
                .ok()
        }
    }

    pub fn cancel(&self, root: Root) {
        self.cancel_listener.emit(root);
        self.local_work_pool.cancel(&root);
    }

    pub fn work_generation_enabled(&self) -> bool {
        self.local_work_pool.work_generation_enabled()
    }

    pub fn stop(&self) {
        //TODO
    }

    pub fn track_cancellations(&self) -> Arc<OutputTrackerMt<Root>> {
        self.cancel_listener.track()
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
            self.runtime,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work::distributed_work_client::DistributedWorkClient;
    use rsnano_core::utils::Peer;
    use rsnano_nullable_http_client::Url;

    #[test]
    fn use_local_work_pool_when_no_peers_given() {
        let expected_work = WorkNonce::from(12345);
        let work_factory = test_factory(TestContext {
            work_pool: WorkPool::new_null(expected_work),
            ..Default::default()
        });
        let request = WorkRequest::new_test_instance();

        let work = work_factory.generate_work(request.clone());

        assert_eq!(work, Some(expected_work));
    }

    #[test]
    fn cancellations_can_be_tracked() {
        let work_factory = test_factory(TestContext {
            work_pool: WorkPool::new_null(1.into()),
            ..Default::default()
        });
        let cancel_tracker = work_factory.track_cancellations();

        let root = Root::from(1);
        work_factory.cancel(root);

        assert_eq!(cancel_tracker.output(), vec![root]);
    }

    #[test]
    fn work_generation_disabled() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let work_factory = WorkFactory::disabled(runtime.handle().clone());
        let result = work_factory.generate_work(WorkRequest::new_test_instance());
        assert_eq!(result, None);
    }

    #[test]
    fn use_remote_work_server() {
        let expected_work = WorkNonce::new(42);
        let work_client = DistributedWorkClient::new_null_with(expected_work);
        let request_tracker = work_client.track_requests();
        let work_factory = test_factory(TestContext {
            work_pool: WorkPool::disabled(),
            work_client,
            work_peers: vec![Peer::new("foo.com", 123)],
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

    struct TestContext {
        work_pool: WorkPool,
        work_client: DistributedWorkClient,
        work_peers: Vec<Peer>,
    }

    impl Default for TestContext {
        fn default() -> Self {
            Self {
                work_pool: WorkPool::new_null(WorkNonce::new(42)),
                work_client: DistributedWorkClient::new_null_with(WorkNonce::new(43)),
                work_peers: Vec::new(),
            }
        }
    }

    fn test_factory(context: TestContext) -> WorkFactory {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        WorkFactory::new(
            context.work_pool,
            context.work_client,
            context.work_peers,
            runtime.handle().clone(),
        )
    }

    // TODO:
    // Backoff + Workrequest
    // Cancel
    // resolve hostnames
    // multiple peers
    // secondary peers
    // unresponsive work peers => use local work
}
