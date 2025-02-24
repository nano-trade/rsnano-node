use super::WorkPool;
use crate::{Root, WorkNonce};

/// The StubWorkPool assumes work == difficulty
pub struct StubWorkPool {
    base_difficulty: u64,
}

impl StubWorkPool {
    pub fn new(base_difficulty: u64) -> Self {
        Self { base_difficulty }
    }
}

impl Default for StubWorkPool {
    fn default() -> Self {
        Self::new(123)
    }
}

impl WorkPool for StubWorkPool {
    fn generate_async(
        &self,
        _root: Root,
        difficulty: u64,
        done: Option<Box<dyn FnOnce(Option<WorkNonce>) + Send>>,
    ) {
        if let Some(done) = done {
            done(Some(difficulty.into()))
        }
    }

    fn generate_dev(&self, _root: Root) -> Option<WorkNonce> {
        Some(self.base_difficulty.into())
    }

    fn generate(&self, _root: Root, difficulty: u64) -> Option<WorkNonce> {
        Some(difficulty.into())
    }
}
