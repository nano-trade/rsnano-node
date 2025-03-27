use rsnano_core::WorkNonce;
use serde::{Deserialize, Serialize};

use crate::RpcF64;

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub struct ActiveDifficultyResponse {
    pub deprecated: String,
    pub network_minimum: WorkNonce,
    pub network_receive_minimum: WorkNonce,
    pub network_current: WorkNonce,
    pub network_receive_current: WorkNonce,
    pub multiplier: RpcF64,
    pub difficulty_trend: Option<RpcF64>,
}
