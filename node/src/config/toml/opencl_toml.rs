use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct OpenclToml {
    pub device: Option<usize>,
    pub enable: Option<bool>,
    pub platform: Option<usize>,
    pub threads: Option<usize>,
}
