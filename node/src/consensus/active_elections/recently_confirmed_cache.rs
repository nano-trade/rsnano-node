use std::collections::{HashMap, VecDeque};

use rsnano_core::{utils::ContainerInfo, BlockHash, QualifiedRoot};

pub(super) struct RecentlyConfirmedCache {
    by_root: HashMap<QualifiedRoot, BlockHash>,
    by_hash: HashMap<BlockHash, QualifiedRoot>,
    sequential: VecDeque<BlockHash>,
    max_len: usize,
}

impl RecentlyConfirmedCache {
    pub fn new(max_len: usize) -> Self {
        Self {
            sequential: VecDeque::new(),
            by_root: HashMap::new(),
            by_hash: HashMap::new(),
            max_len,
        }
    }

    pub fn put(&mut self, root: QualifiedRoot, hash: BlockHash) -> bool {
        if self.by_hash.contains_key(&hash) || self.by_root.contains_key(&root) {
            return false;
        }
        self.sequential.push_back(hash);
        self.by_root.insert(root.clone(), hash);
        self.by_hash.insert(hash, root);
        if self.sequential.len() > self.max_len {
            if let Some(old_hash) = self.sequential.pop_front() {
                if let Some(old_root) = self.by_hash.remove(&old_hash) {
                    self.by_root.remove(&old_root);
                }
            }
        }
        true
    }

    pub fn erase(&mut self, hash: &BlockHash) {
        if let Some(root) = self.by_hash.remove(hash) {
            self.by_root.remove(&root);
            self.sequential.retain(|i| i != hash);
        }
    }

    pub fn root_exists(&self, root: &QualifiedRoot) -> bool {
        self.by_root.contains_key(root)
    }

    pub fn hash_exists(&self, hash: &BlockHash) -> bool {
        self.by_hash.contains_key(hash)
    }

    pub fn clear(&mut self) {
        self.sequential.clear();
        self.by_root.clear();
        self.by_hash.clear();
    }

    pub fn len(&self) -> usize {
        self.sequential.len()
    }

    pub fn container_info(&self) -> ContainerInfo {
        [(
            "confirmed",
            self.len(),
            std::mem::size_of::<BlockHash>() * 3 + std::mem::size_of::<QualifiedRoot>(),
        )]
        .into()
    }
}
