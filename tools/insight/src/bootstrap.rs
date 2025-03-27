use rsnano_core::Account;
use rsnano_node::bootstrap::state::{BlockingEntry, BootstrapState, Priority};

#[derive(Default)]
pub(crate) struct BootstrapInfo {
    pub priority_accounts: usize,
    pub blocked_accounts: usize,
    pub priorities: Vec<(Priority, Account)>,
    pub blocked: Vec<BlockingEntry>,
    pub search: String,
}

impl BootstrapInfo {
    pub(crate) fn update(&mut self, state: &BootstrapState) {
        let target_account = Account::decode_account(&self.search).ok();
        self.priority_accounts = state.candidate_accounts.priority_len();
        self.blocked_accounts = state.candidate_accounts.blocked_len();
        self.priorities = state
            .candidate_accounts
            .iter_priorities()
            .filter_map(|(prio, acc)| {
                if target_account.is_none() || target_account.as_ref() == Some(acc) {
                    Some((prio, *acc))
                } else {
                    None
                }
            })
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
