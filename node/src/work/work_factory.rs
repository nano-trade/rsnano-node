use std::sync::Arc;

use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider},
    Root, WorkNonce,
};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use rsnano_work::{WorkPool, WorkPoolBuilder};

#[derive(Clone)]
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
    cancel_listener: OutputListenerMt<Root>,
}

impl WorkFactory {
    fn new(work_pool: WorkPool) -> Self {
        Self {
            local_work_pool: work_pool,
            cancel_listener: OutputListenerMt::new(),
        }
    }

    pub fn disabled() -> Self {
        Self::builder().local_work_pool(|p| p.disabled()).finish()
    }

    pub fn builder() -> WorkFactoryBuilder {
        WorkFactoryBuilder {
            local_work_pool: None,
        }
    }

    pub fn generate_work(&self, request: WorkRequest) -> Option<WorkNonce> {
        self.local_work_pool
            .generate(request.root, request.difficulty)
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

impl Default for WorkFactory {
    fn default() -> Self {
        Self::builder().finish()
    }
}

pub struct WorkFactoryBuilder {
    local_work_pool: Option<WorkPool>,
}

impl WorkFactoryBuilder {
    pub fn local_work_pool(mut self, f: impl FnOnce(WorkPoolBuilder) -> WorkPoolBuilder) -> Self {
        self.local_work_pool = Some(f(WorkPool::builder()).finish());
        self
    }

    pub fn finish(self) -> WorkFactory {
        let local_work_pool = self.local_work_pool.unwrap_or_default();
        WorkFactory::new(local_work_pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work::distributed_work_client::DistributedWorkClient;

    #[test]
    fn use_local_work_pool_when_no_peers_given() {
        let expected_work = WorkNonce::from(12345);
        let work_pool = WorkPool::new_null(expected_work);
        let work_factory = WorkFactory::new(work_pool);
        let request = WorkRequest::new_test_instance();

        let work = work_factory.generate_work(request.clone());

        assert_eq!(work, Some(expected_work));
    }

    #[test]
    fn cancellations_can_be_tracked() {
        let work_pool = WorkPool::new_null(1.into());
        let work_factory = WorkFactory::new(work_pool);
        let cancel_tracker = work_factory.track_cancellations();

        let root = Root::from(1);
        work_factory.cancel(root);

        assert_eq!(cancel_tracker.output(), vec![root]);
    }

    #[test]
    fn work_generation_disabled() {
        let work_factory = WorkFactory::disabled();
        let result = work_factory.generate_work(WorkRequest::new_test_instance());
        assert_eq!(result, None);
    }

    #[test]
    #[ignore = "wip"]
    fn use_remote_work_server() {
        let work_pool = WorkPool::disabled();
        //let work_client = DistributedWorkClient::new_null();
        let work_factory = WorkFactory::new(work_pool);
    }

    // TODO:
    // Backoff + Workrequest
    // Cancel
    // resolve hostnames
    // multiple peers
    // secondary peers
    // unresponsive work peers => use local work
}
