use std::sync::Arc;

use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoProvider},
    Root, WorkNonce,
};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use rsnano_work::WorkPool;

#[derive(Clone)]
pub struct WorkRequest {
    pub root: Root,
    pub difficulty: u64,
}

impl WorkRequest {
    pub fn new_test_instance() -> Self {
        Self {
            root: Root::from(100),
            difficulty: 42,
        }
    }
}

pub struct WorkFactory {
    pub local_work_pool: WorkPool,
    cancel_listener: OutputListenerMt<Root>,
}

impl WorkFactory {
    pub fn new(work_pool: WorkPool) -> Self {
        Self {
            local_work_pool: work_pool,
            cancel_listener: OutputListenerMt::new(),
        }
    }

    pub fn new_null() -> Self {
        Self::new(WorkPool::disabled())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn use_local_work_factor_when_no_peers_given() {
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

    // TODO:
    // Backoff + Workrequest
    // Cancel
    // Local work
    // resolve hostnames
    // multiple peers
    // secondary peers
    // work generation disabled
    // unresponsive work peers => use local work
}
