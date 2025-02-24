use tracing::error;

use super::{gpu::Gpu, OpenClConfig, WorkGenerator, WorkRng, WorkTicket, XorShift1024Star};
use crate::{Root, WorkNonce};

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
    ) -> Option<u64> {
        if let Err(e) = self.gpu.set_task(root.as_bytes(), min_difficulty) {
            error!("Error setting task: {:?}", e);
            return None;
        }

        loop {
            let attempt = self.rnd.next_work();
            let mut out = [0u8; 8];
            match self.gpu.run(&mut out, attempt) {
                Ok(true) => {
                    let work = WorkNonce::from(u64::from_le_bytes(out));
                    return Some(work.into());
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
    use crate::{work::WorkThresholds, Difficulty, DifficultyV1};

    use super::*;

    #[test]
    #[cfg_attr(not(feature = "opencl"), ignore)]
    fn gpu_work() {
        let mut work_generator = GpuWorkGenerator::new(OpenClConfig::default()).unwrap();
        let min_difficulty = WorkThresholds::publish_full().threshold_base();
        let work_ticket = WorkTicket::never_expires();

        let root = Root::from(123);
        let result = work_generator
            .create(&root, min_difficulty, &work_ticket)
            .unwrap();
        assert!(DifficultyV1 {}.get_difficulty(&root, result) >= min_difficulty);
    }
}
