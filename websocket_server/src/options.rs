use super::{ConfirmationOptions, VoteOptions};

#[derive(Clone)]
pub enum Options {
    Confirmation(ConfirmationOptions),
    Vote(VoteOptions),
    Other,
}

impl Options {
    /**
     * Update options, if available for a given topic
     * @return false on success
     */
    pub fn update(&mut self, options: &serde_json::Value) {
        if let Options::Confirmation(i) = self {
            i.update(options);
        }
    }
}
