use rsnano_core::Account;
use rsnano_node::bootstrap::state::{BlockingEntry, BootstrapState, Priority};

#[derive(Default)]
pub(crate) struct BootstrapInfo {
    pub priority_accounts: usize,
    pub blocked_accounts: usize,
    pub priorities: Vec<(Priority, Account)>,
    pub blocked: Vec<BlockingEntry>,
}

impl BootstrapInfo {
    pub(crate) fn update(&mut self, state: &BootstrapState) {
        self.priority_accounts = state.candidate_accounts.priority_len();
        self.blocked_accounts = state.candidate_accounts.blocked_len();
        self.priorities = state
            .candidate_accounts
            .iter_priorities()
            .map(|(prio, acc)| (prio, *acc))
            .take(50)
            .collect();
        self.blocked = state
            .candidate_accounts
            .iter_blocked()
            .take(50)
            .cloned()
            .collect();
    }
}
