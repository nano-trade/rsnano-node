use strum::EnumCount;
use strum_macros::EnumCount as EnumCountMacro;

#[derive(Clone, Debug, PartialEq, EnumCountMacro, num_derive::FromPrimitive)]
pub enum CooldownSource {
    /// The node is cooling down due to high load
    ConfirmingSet,
    ActiveElections,
}

pub(crate) struct CooldownController {
    // Array of cooldown states, indexed by CooldownSource as usize
    source_states: [bool; CooldownSource::COUNT],
    cool_down: bool,
}

impl CooldownController {
    pub fn new() -> Self {
        Self { 
            source_states: [false; CooldownSource::COUNT],
            cool_down: false 
        }
    }

    pub fn is_cooling_down(&self) -> bool {
        self.cool_down
    }

    pub fn is_source_cooling_down(&self, source: CooldownSource) -> bool {
        self.source_states[source as usize]
    }

    pub fn set_cooldown(&mut self, cooldown_source: CooldownSource, cool_down: bool) {
        // Update the specific source state
        let index = cooldown_source as usize;
        self.source_states[index] = cool_down;
        
        // Update overall cooldown state - true if any source is cooling down
        self.cool_down = self.source_states.iter().any(|&state| state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_cooldown() {
        let controller = CooldownController::new();
        assert_eq!(controller.is_cooling_down(), false);
    }

    #[test]
    fn one_cooldown() {
        let mut controller = CooldownController::new();
        controller.set_cooldown(CooldownSource::ConfirmingSet, true);
        assert_eq!(controller.is_cooling_down(), true);
    }

    #[test]
    fn end_cooldown() {
        let mut controller = CooldownController::new();
        controller.set_cooldown(CooldownSource::ConfirmingSet, true);
        controller.set_cooldown(CooldownSource::ConfirmingSet, false);
        assert_eq!(controller.is_cooling_down(), false);
    }

    #[test]
    fn mutliple_sources_different_cooldowns() {
        let mut controller = CooldownController::new();
        controller.set_cooldown(CooldownSource::ConfirmingSet, true);
        controller.set_cooldown(CooldownSource::ActiveElections, false);
        assert_eq!(controller.is_cooling_down(), true);
    }
}
