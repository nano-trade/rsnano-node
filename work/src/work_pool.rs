use std::{
    mem::size_of,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use rsnano_core::{utils::ContainerInfo, Root, WorkNonce};

use tracing::warn;

use super::{
    gpu_work_generator::GpuWorkGenerator, CpuWorkGenerator, OpenClConfig, WorkItem,
    WorkQueueCoordinator, WorkThread, WorkThresholds, WorkTicket, WORK_THRESHOLDS_STUB,
};

pub struct WorkPool {
    threads: Vec<JoinHandle<()>>,
    work_queue: Arc<WorkQueueCoordinator>,
    work_thresholds: WorkThresholds,
    cpu_rate_limiter: Duration,
    has_open_cl: bool,
}

impl WorkPool {
    pub fn new(
        work_thresholds: WorkThresholds,
        thread_count: usize,
        cpu_rate_limiter: Duration,
        enable_open_cl: bool,
        opencl_config: OpenClConfig,
    ) -> Self {
        let mut pool = Self {
            threads: Vec::new(),
            work_queue: Arc::new(WorkQueueCoordinator::new()),
            work_thresholds,
            cpu_rate_limiter,
            has_open_cl: false,
        };

        pool.spawn_threads(thread_count, enable_open_cl, opencl_config);
        pool
    }

    pub fn new_dev() -> Self {
        Self::new(
            WorkThresholds::publish_dev().clone(),
            1,
            Duration::ZERO,
            false,
            OpenClConfig::default(),
        )
    }

    pub fn new_null(configured_work: WorkNonce) -> Self {
        let mut pool = Self {
            threads: Vec::new(),
            work_queue: Arc::new(WorkQueueCoordinator::new()),
            work_thresholds: WORK_THRESHOLDS_STUB.clone(),
            cpu_rate_limiter: Duration::ZERO,
            has_open_cl: false,
        };

        pool.threads
            .push(pool.spawn_stub_worker_thread(configured_work.into()));
        pool
    }

    pub fn disabled() -> Self {
        Self {
            threads: Vec::new(),
            work_queue: Arc::new(WorkQueueCoordinator::new()),
            work_thresholds: WORK_THRESHOLDS_STUB.clone(),
            cpu_rate_limiter: Duration::ZERO,
            has_open_cl: false,
        }
    }

    fn spawn_threads(
        &mut self,
        thread_count: usize,
        enable_open_cl: bool,
        opencl_config: OpenClConfig,
    ) {
        let mut gpu_work = if enable_open_cl {
            match GpuWorkGenerator::new(opencl_config) {
                Ok(gpu) => Some(gpu),
                Err(e) => {
                    warn!("Error initializing GPU: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        for _ in 0..thread_count {
            if let Some(gpu) = gpu_work.take() {
                self.threads.push(self.spawn_worker_thread(gpu));
                continue;
            }
            self.threads.push(self.spawn_cpu_worker_thread())
        }
    }

    fn spawn_cpu_worker_thread(&self) -> JoinHandle<()> {
        self.spawn_worker_thread(CpuWorkGenerator::new(self.cpu_rate_limiter))
    }

    fn spawn_stub_worker_thread(&self, configured_work: u64) -> JoinHandle<()> {
        self.spawn_worker_thread(StubWorkGenerator(configured_work))
    }

    fn spawn_worker_thread<T>(&self, work_generator: T) -> JoinHandle<()>
    where
        T: WorkGenerator + Send + 'static,
    {
        let work_queue = Arc::clone(&self.work_queue);
        thread::Builder::new()
            .name("Work pool".to_string())
            .spawn(move || {
                WorkThread::new(work_generator, work_queue).work_loop();
            })
            .unwrap()
    }

    pub fn has_opencl(&self) -> bool {
        self.has_open_cl
    }

    pub fn work_generation_enabled(&self) -> bool {
        !self.threads.is_empty()
    }

    pub fn cancel(&self, root: &Root) {
        self.work_queue.cancel(root);
    }

    pub fn stop(&self) {
        self.work_queue.stop();
    }

    pub fn size(&self) -> usize {
        self.work_queue.lock_work_queue().len()
    }

    pub fn pending_value_size() -> usize {
        size_of::<WorkItem>()
    }

    pub fn thread_count(&self) -> usize {
        self.threads.len()
    }

    pub fn threshold_base(&self) -> u64 {
        self.work_thresholds.threshold_base()
    }

    pub fn difficulty(&self, root: &Root, work: WorkNonce) -> u64 {
        self.work_thresholds.difficulty(root, work)
    }

    pub fn container_info(&self) -> ContainerInfo {
        [("pending", self.size(), Self::pending_value_size())].into()
    }

    pub fn generate_async(
        &self,
        root: Root,
        difficulty: u64,
        done: Option<Box<dyn FnOnce(Option<WorkNonce>) + Send>>,
    ) {
        debug_assert!(!root.is_zero());
        if !self.threads.is_empty() {
            self.work_queue.enqueue(WorkItem {
                root,
                min_difficulty: difficulty,
                callback: done,
            });
        } else if let Some(callback) = done {
            callback(None);
        }
    }

    pub fn generate_dev(&self, root: Root) -> Option<WorkNonce> {
        self.generate(root, self.work_thresholds.base)
    }

    pub fn generate(&self, root: Root, difficulty: u64) -> Option<WorkNonce> {
        if self.threads.is_empty() {
            return None;
        }

        let done_notifier = WorkDoneNotifier::new();
        let done_notifier_clone = done_notifier.clone();

        self.generate_async(
            root,
            difficulty,
            Some(Box::new(move |work| {
                done_notifier_clone.signal_done(work);
            })),
        );

        done_notifier.wait()
    }
}

#[derive(Default)]
struct WorkDoneState {
    work: Option<WorkNonce>,
    done: bool,
}

#[derive(Clone)]
struct WorkDoneNotifier {
    state: Arc<(Mutex<WorkDoneState>, Condvar)>,
}

impl WorkDoneNotifier {
    fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(WorkDoneState::default()), Condvar::new())),
        }
    }

    fn signal_done(&self, work: Option<WorkNonce>) {
        {
            let mut lock = self.state.0.lock().unwrap();
            lock.work = work;
            lock.done = true;
        }
        self.state.1.notify_one();
    }

    fn wait(&self) -> Option<WorkNonce> {
        let mut lock = self.state.0.lock().unwrap();
        loop {
            if lock.done {
                return lock.work;
            }
            lock = self.state.1.wait(lock).unwrap();
        }
    }
}

impl Drop for WorkPool {
    fn drop(&mut self) {
        self.stop();
        for handle in self.threads.drain(..) {
            handle.join().unwrap();
        }
    }
}

pub(crate) trait WorkGenerator {
    fn create(
        &mut self,
        item: &Root,
        min_difficulty: u64,
        work_ticket: &WorkTicket,
    ) -> Option<WorkNonce>;
}

struct StubWorkGenerator(u64);

impl WorkGenerator for StubWorkGenerator {
    fn create(
        &mut self,
        _item: &Root,
        _min_difficulty: u64,
        _work_ticket: &WorkTicket,
    ) -> Option<WorkNonce> {
        Some(self.0.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{Block, TestBlockBuilder};
    use std::sync::{mpsc, LazyLock};

    pub static WORK_POOL: LazyLock<WorkPool> = LazyLock::new(|| {
        WorkPool::new(
            WorkThresholds::publish_dev().clone(),
            rsnano_core::utils::get_cpu_count(),
            Duration::ZERO,
            false,
            OpenClConfig::default(),
        )
    });

    #[test]
    fn work_disabled() {
        let pool = WorkPool::new(
            WorkThresholds::publish_dev().clone(),
            0,
            Duration::ZERO,
            false,
            OpenClConfig::default(),
        );
        let result = pool.generate_dev(Root::from(1));
        assert_eq!(result, None);
    }

    #[test]
    fn work_one() {
        let pool = &WORK_POOL;
        let mut block = TestBlockBuilder::state().build();
        let root = block.root();
        block.set_work(pool.generate_dev(root).unwrap());
        assert!(pool.threshold_base() < difficulty(&block));
    }

    #[test]
    fn work_validate() {
        let pool = &WORK_POOL;
        let mut block = TestBlockBuilder::legacy_send().work(6).build();
        assert!(difficulty(&block) < pool.threshold_base());
        let root = block.root();
        block
            .as_block_mut()
            .set_work(pool.generate_dev(root).unwrap());
        assert!(difficulty(&block) > pool.threshold_base());
    }

    #[test]
    fn work_cancel() {
        let (tx, rx) = mpsc::channel();
        let key = Root::from(12345);
        WORK_POOL.generate_async(
            key,
            WorkThresholds::publish_dev().base,
            Some(Box::new(move |_done| {
                tx.send(()).unwrap();
            })),
        );
        WORK_POOL.cancel(&key);
        assert_eq!(rx.recv_timeout(Duration::from_secs(2)), Ok(()))
    }

    #[test]
    fn work_difficulty() {
        let root = Root::from(1);
        let difficulty1 = 0xff00000000000000;
        let difficulty2 = 0xfff0000000000000;
        let difficulty3 = 0xffff000000000000;
        let mut result_difficulty = u64::MAX;

        while result_difficulty > difficulty2 {
            let work = WORK_POOL.generate(root, difficulty1).unwrap();
            result_difficulty = WorkThresholds::publish_dev().difficulty(&root, work);
        }
        assert!(result_difficulty > difficulty1);

        result_difficulty = u64::MAX;
        while result_difficulty > difficulty3 {
            let work = WORK_POOL.generate(root, difficulty2).unwrap();
            result_difficulty = WorkThresholds::publish_dev().difficulty(&root, work);
        }
        assert!(result_difficulty > difficulty2);
    }

    fn difficulty(block: &Block) -> u64 {
        WorkThresholds::publish_dev().difficulty_block(block)
    }
}
