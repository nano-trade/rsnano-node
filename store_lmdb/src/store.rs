use std::{
    ffi::CString,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use lmdb_sys::MDB_SUCCESS;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use rsnano_core::utils::UnixTimestamp;

use crate::{
    successor_store::LmdbSuccessorStore, LmdbAccountStore, LmdbBlockStore,
    LmdbConfirmationHeightStore, LmdbEnv, LmdbFinalVoteStore, LmdbOnlineWeightStore, LmdbPeerStore,
    LmdbPendingStore, LmdbPrunedStore, LmdbReadTransaction, LmdbRepWeightStore, LmdbVersionStore,
    LmdbWriteTransaction,
};

pub struct LedgerCache {
    pub confirmed_count: AtomicU64,
    pub block_count: AtomicU64,
    pub pruned_count: AtomicU64,
    pub account_count: AtomicU64,
}

impl LedgerCache {
    pub fn new() -> Self {
        Self {
            confirmed_count: AtomicU64::new(0),
            block_count: AtomicU64::new(0),
            pruned_count: AtomicU64::new(0),
            account_count: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.confirmed_count.store(0, Ordering::SeqCst);
        self.block_count.store(0, Ordering::SeqCst);
        self.pruned_count.store(0, Ordering::SeqCst);
        self.account_count.store(0, Ordering::SeqCst);
    }
}

pub struct LmdbStore {
    pub env: LmdbEnv,
    pub cache: Arc<LedgerCache>,
    pub block: LmdbBlockStore,
    pub account: LmdbAccountStore,
    pub pending: LmdbPendingStore,
    pub pruned: LmdbPrunedStore,
    pub rep_weight: Arc<LmdbRepWeightStore>,
    pub confirmation_height: LmdbConfirmationHeightStore,
    pub successors: LmdbSuccessorStore,
    // extract these?
    pub final_vote: LmdbFinalVoteStore,
    pub online_weight: LmdbOnlineWeightStore,
    pub peer: LmdbPeerStore,
    pub version: LmdbVersionStore,
}

impl LmdbStore {
    pub fn new_null() -> Self {
        Self::new(LmdbEnv::new_null()).unwrap()
    }

    pub fn new(env: LmdbEnv) -> anyhow::Result<Self> {
        Ok(Self {
            cache: Arc::new(LedgerCache::new()),
            block: LmdbBlockStore::new(&env)?,
            account: LmdbAccountStore::new(&env)?,
            pending: LmdbPendingStore::new(&env)?,
            online_weight: LmdbOnlineWeightStore::new(&env)?,
            pruned: LmdbPrunedStore::new(&env)?,
            rep_weight: Arc::new(LmdbRepWeightStore::new(&env)?),
            peer: LmdbPeerStore::new(&env)?,
            confirmation_height: LmdbConfirmationHeightStore::new(&env)?,
            final_vote: LmdbFinalVoteStore::new(&env)?,
            successors: LmdbSuccessorStore::new(&env.environment)?,
            version: LmdbVersionStore::new(&env)?,
            env,
        })
    }

    pub fn memory_stats(&self) -> anyhow::Result<MemoryStats> {
        let stats = self.env.environment.stat()?;
        Ok(MemoryStats {
            branch_pages: stats.branch_pages(),
            depth: stats.depth(),
            entries: stats.entries(),
            leaf_pages: stats.leaf_pages(),
            overflow_pages: stats.overflow_pages(),
            page_size: stats.page_size(),
        })
    }

    pub fn tx_begin_read(&self) -> LmdbReadTransaction {
        self.env.tx_begin_read()
    }

    pub fn tx_begin_write(&self) -> LmdbWriteTransaction {
        self.env.tx_begin_write()
    }
}

#[derive(Serialize, Deserialize)]
pub struct MemoryStats {
    pub branch_pages: usize,
    pub depth: u32,
    pub entries: usize,
    pub leaf_pages: usize,
    pub overflow_pages: usize,
    pub page_size: u32,
}

/// Takes a filepath, appends '_backup_<timestamp>' to the end (but before any extension) and saves that file in the same directory
pub fn create_backup_file(env: &LmdbEnv) -> anyhow::Result<()> {
    let source_path = env.file_path();
    let backup_path = backup_file_path(source_path)?;

    info!(
        "Performing {:?} backup before database upgrade...",
        source_path
    );

    let backup_path_cstr = CString::new(
        backup_path
            .as_os_str()
            .to_str()
            .ok_or_else(|| anyhow!("invalid backup path"))?,
    )?;
    let status =
        unsafe { lmdb_sys::mdb_env_copy(env.environment.env(), backup_path_cstr.as_ptr()) };
    if status != MDB_SUCCESS {
        error!("{:?} backup failed", source_path);
        Err(anyhow!("backup failed"))
    } else {
        info!("Backup created: {:?}", backup_path);
        Ok(())
    }
}

fn backup_file_path(source_path: &Path) -> anyhow::Result<PathBuf> {
    let extension = source_path
        .extension()
        .ok_or_else(|| anyhow!("no extension"))?
        .to_str()
        .ok_or_else(|| anyhow!("invalid extension"))?;

    let mut backup_path = source_path
        .parent()
        .ok_or_else(|| anyhow!("no parent path"))?
        .to_owned();

    let file_stem = source_path
        .file_stem()
        .ok_or_else(|| anyhow!("no file stem"))?
        .to_str()
        .ok_or_else(|| anyhow!("invalid file stem"))?;

    let backup_filename = format!(
        "{}_backup_{}.{}",
        file_stem,
        UnixTimestamp::now(),
        extension
    );
    backup_path.push(&backup_filename);
    Ok(backup_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LmdbEnvFactory;

    #[test]
    fn create_store() -> anyhow::Result<()> {
        let env = LmdbEnvFactory::new_null().create_env("/nulled/store.ldb")?;
        let _ = LmdbStore::new(env)?;
        Ok(())
    }
}
