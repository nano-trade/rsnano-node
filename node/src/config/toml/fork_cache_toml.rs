use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct ForkCacheToml {
    pub max_size: Option<usize>,
    pub max_forks_per_root: Option<usize>,
}
