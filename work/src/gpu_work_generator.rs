use tracing::error;

use rsnano_core::{Root, WorkNonce};

use super::{gpu::Gpu, OpenClConfig, WorkGenerator, WorkRng, WorkTicket, XorShift1024Star};

/// Generates the proof of work using a GPU with OpenCL
pub struct GpuWorkGenerator {
    gpu: Gpu,
    rnd: XorShift1024Star,
}

impl GpuWorkGenerator {
    pub fn new(config: OpenClConfig) -> ocl::Result<Self> {
        let gpu = Gpu::new(config)?;

        Ok(Self {
            gpu,
            rnd: XorShift1024Star::new(),
        })
    }
}

impl WorkGenerator for GpuWorkGenerator {
    fn create(
        &mut self,
        root: &Root,
        min_difficulty: u64,
        work_ticket: &WorkTicket,
    ) -> Option<WorkNonce> {
        if let Err(e) = self.gpu.set_task(root, min_difficulty) {
            error!("Error setting task: {:?}", e);
            return None;
        }

        loop {
            let attempt = self.rnd.next_work();
            let mut out = [0u8; 8];
            match self.gpu.run(&mut out, attempt) {
                Ok(true) => {
                    let work = WorkNonce::from(u64::from_le_bytes(out));
                    return Some(work);
                }
                Ok(false) => {}
                Err(err) => {
                    error!("Error computing work on GPU {}", err);
                    if let Err(err) = self.gpu.reset_bufs() {
                        error!("Failed to reset GPU {}'s buffers", err);
                        return None;
                    }
                }
            }

            if work_ticket.expired() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rsnano_core::{Difficulty, DifficultyV1};

    use crate::WorkThresholds;

    use super::*;

    #[test]
    fn gpu_work() {
        let mut work_generator = GpuWorkGenerator::new(OpenClConfig::default()).unwrap();
        let min_difficulty = WorkThresholds::publish_dev().threshold_base();
        let work_ticket = WorkTicket::never_expires();

        let root = Root::from(123);
        let result = work_generator
            .create(&root, min_difficulty, &work_ticket)
            .unwrap();
        assert!(DifficultyV1 {}.get_difficulty(&root, result) >= min_difficulty);
    }
}
