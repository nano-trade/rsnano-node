use rsnano_core::{to_hex_string, Account, Root};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct HttpWorkRequest {
    action: &'static str,
    hash: String,
    difficulty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    account: Option<String>,
}

impl HttpWorkRequest {
    pub fn new(root: Root, difficulty: u64, account: Option<Account>) -> Self {
        Self {
            action: "work_generate",
            hash: root.to_string(),
            difficulty: to_hex_string(difficulty),
            account: account.map(|a| a.encode_account()),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct HttpWorkResponse {
    work: String,
}
