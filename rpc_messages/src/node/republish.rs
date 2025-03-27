use crate::{RpcCommand, RpcU32};
use rsnano_core::BlockHash;
use serde::{Deserialize, Serialize};

impl RpcCommand {
    pub fn republish(args: RepublishArgs) -> Self {
        Self::Republish(args)
    }
}

impl From<BlockHash> for RepublishArgs {
    fn from(value: BlockHash) -> Self {
        Self::builder(value).build()
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct RepublishArgs {
    pub hash: BlockHash,
    pub sources: Option<RpcU32>,
    pub destinations: Option<RpcU32>,
    pub count: Option<RpcU32>,
}

impl RepublishArgs {
    pub fn builder(hash: BlockHash) -> RepublishArgsBuilder {
        RepublishArgsBuilder::new(hash)
    }
}

pub struct RepublishArgsBuilder {
    hash: BlockHash,
    sources: Option<RpcU32>,
    destinations: Option<RpcU32>,
    count: Option<RpcU32>,
}

impl RepublishArgsBuilder {
    pub fn new(hash: BlockHash) -> Self {
        Self {
            hash,
            sources: None,
            destinations: None,
            count: None,
        }
    }

    pub fn with_sources(mut self, sources: u32) -> Self {
        self.sources = Some(sources.into());
        self
    }

    pub fn with_destinations(mut self, destinations: u32) -> Self {
        self.destinations = Some(destinations.into());
        self
    }

    pub fn with_count(mut self, count: u32) -> Self {
        self.count = Some(count.into());
        self
    }

    pub fn build(self) -> RepublishArgs {
        RepublishArgs {
            hash: self.hash,
            sources: self.sources,
            destinations: self.destinations,
            count: self.count,
        }
    }
}
