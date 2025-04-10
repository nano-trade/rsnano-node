use crate::{
    cementation::ConfirmingSetEvent,
    consensus::{ActiveElectionsContainer, AecCooldownReason},
    utils::BackpressureEventProcessor,
};
use std::sync::{Arc, RwLock};

pub(crate) struct ConfirmingSetEventProcessor {
    pub(crate) active_elections: Arc<RwLock<ActiveElectionsContainer>>,
}

impl BackpressureEventProcessor<ConfirmingSetEvent> for ConfirmingSetEventProcessor {
    fn cool_down(&mut self) {
        self.active_elections
            .write()
            .unwrap()
            .set_cooldown(true, AecCooldownReason::ConfirmingSetEventQueueFull);
    }

    fn recovered(&mut self) {
        self.active_elections
            .write()
            .unwrap()
            .set_cooldown(false, AecCooldownReason::ConfirmingSetEventQueueFull);
    }

    fn process(&mut self, event: ConfirmingSetEvent) {
        match event {
            ConfirmingSetEvent::ConfirmationFailed(block_hash) => {
                // Do some cleanup due to this block never being processed
                self.active_elections
                    .write()
                    .unwrap()
                    .remove_recently_confirmed(&block_hash);
            }
            ConfirmingSetEvent::NearFull => {
                self.active_elections
                    .write()
                    .unwrap()
                    .set_cooldown(true, AecCooldownReason::ConfirmingSetFull);
            }
            ConfirmingSetEvent::Recovered => {
                self.active_elections
                    .write()
                    .unwrap()
                    .set_cooldown(false, AecCooldownReason::ConfirmingSetFull);
            }
        }
    }
}
