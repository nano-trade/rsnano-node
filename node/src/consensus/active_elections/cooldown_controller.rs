use strum::EnumCount;
use strum_macros::EnumCount as EnumCountMacro;

#[derive(Clone, Debug, PartialEq, EnumCountMacro)]
pub enum AecCooldownReason {
    ConfirmingSetFull,
    ConfirmingSetEventQueueFull,
    AecEventQueueFull,
}

pub(crate) struct CooldownController {
    // Array of cooldown states, indexed by CooldownSource as usize
    source_states: [bool; AecCooldownReason::COUNT],
    cool_down: bool,
}

impl CooldownController {
    pub fn new() -> Self {
        Self {
            source_states: [false; AecCooldownReason::COUNT],
            cool_down: false,
        }
    }

    pub fn is_cooling_down(&self) -> bool {
        self.cool_down
    }

    pub fn set_cooldown(&mut self, cool_down: bool, reason: AecCooldownReason) -> CooldownResult {
        let was_cooling_down_before = self.is_cooling_down();

        // Update the specific source state
        let index = reason as usize;
        self.source_states[index] = cool_down;

        // Update overall cooldown state - true if any source is cooling down
        self.cool_down = self.source_states.iter().any(|&state| state);

        if self.is_cooling_down() && !was_cooling_down_before {
            CooldownResult::CooldownStarted
        } else if !self.is_cooling_down() && was_cooling_down_before {
            CooldownResult::Recovered
        } else {
            CooldownResult::Unchanged
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum CooldownResult {
    CooldownStarted,
    Recovered,
    Unchanged,
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
        let result = controller.set_cooldown(true, AecCooldownReason::ConfirmingSetFull);
        assert_eq!(result, CooldownResult::CooldownStarted);
        assert_eq!(controller.is_cooling_down(), true);
    }

    #[test]
    fn end_cooldown() {
        let mut controller = CooldownController::new();
        controller.set_cooldown(true, AecCooldownReason::ConfirmingSetFull);
        let result = controller.set_cooldown(false, AecCooldownReason::ConfirmingSetFull);
        assert_eq!(result, CooldownResult::Recovered);
        assert_eq!(controller.is_cooling_down(), false);
    }

    #[test]
    fn mutliple_sources_different_cooldowns() {
        let mut controller = CooldownController::new();
        controller.set_cooldown(true, AecCooldownReason::ConfirmingSetFull);
        let result = controller.set_cooldown(false, AecCooldownReason::AecEventQueueFull);
        assert_eq!(result, CooldownResult::Unchanged);
        assert_eq!(controller.is_cooling_down(), true);
    }
}
