use crate::{
    LmdbAccountStore, LmdbBlockStore, LmdbConfirmationHeightStore, LmdbDatabase, LmdbEnv,
    LmdbFinalVoteStore, LmdbOnlineWeightStore, LmdbPeerStore, LmdbPendingStore, LmdbPrunedStore,
    LmdbReadTransaction, LmdbRepWeightStore, LmdbVersionStore, LmdbWriteTransaction, WriteQueue,
    Writer, STORE_VERSION_CURRENT, STORE_VERSION_MINIMUM,
};
use lmdb::{DatabaseFlags, WriteFlags};
use lmdb_sys::MDB_SUCCESS;
use rsnano_core::utils::UnixTimestamp;
use serde::{Deserialize, Serialize};
use std::{
    ffi::CString,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tracing::{debug, error, info};

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
    pub write_queue: Arc<WriteQueue>,
    pub cache: Arc<LedgerCache>,
    pub block: Arc<LmdbBlockStore>,
    pub account: Arc<LmdbAccountStore>,
    pub pending: Arc<LmdbPendingStore>,
    pub pruned: Arc<LmdbPrunedStore>,
    pub rep_weight: Arc<LmdbRepWeightStore>,
    pub confirmation_height: Arc<LmdbConfirmationHeightStore>,
    pub final_vote: Arc<LmdbFinalVoteStore>,
    // extract these?
    pub online_weight: Arc<LmdbOnlineWeightStore>,
    pub peer: Arc<LmdbPeerStore>,
    pub version: Arc<LmdbVersionStore>,
}

impl LmdbStore {
    pub fn new_null() -> Self {
        Self::new(LmdbEnv::new_null()).unwrap()
    }

    pub fn new(env: LmdbEnv) -> anyhow::Result<Self> {
        upgrade_if_needed(&env)?;

        Ok(Self {
            write_queue: env.write_queue.clone(),
            cache: Arc::new(LedgerCache::new()),
            block: Arc::new(LmdbBlockStore::new(&env)?),
            account: Arc::new(LmdbAccountStore::new(&env)?),
            pending: Arc::new(LmdbPendingStore::new(&env)?),
            online_weight: Arc::new(LmdbOnlineWeightStore::new(&env)?),
            pruned: Arc::new(LmdbPrunedStore::new(&env)?),
            rep_weight: Arc::new(LmdbRepWeightStore::new(&env)?),
            peer: Arc::new(LmdbPeerStore::new(&env)?),
            confirmation_height: Arc::new(LmdbConfirmationHeightStore::new(&env)?),
            final_vote: Arc::new(LmdbFinalVoteStore::new(&env)?),
            version: Arc::new(LmdbVersionStore::new(&env)?),
            env,
        })
    }

    pub fn rebuild_db(&self, txn: &mut LmdbWriteTransaction) -> anyhow::Result<()> {
        let tables = [
            self.account.database(),
            self.block.database(),
            self.pruned.database(),
            self.confirmation_height.database(),
            self.pending.database(),
        ];
        for table in tables {
            rebuild_table(&self.env, txn, table)?;
        }

        Ok(())
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

    pub fn tx_begin_write(&self, writer: Writer) -> LmdbWriteTransaction {
        self.env.tx_begin_write_for(writer)
    }
}

fn upgrade_if_needed(env: &LmdbEnv) -> Result<(), anyhow::Error> {
    let upgrade_info = LmdbVersionStore::check_upgrade(&env)?;
    if upgrade_info.is_fully_upgraded {
        debug!("No database upgrade needed");
        return Ok(());
    }

    info!("Upgrade in progress...");
    do_upgrades(&env)?;
    info!("Upgrade done!");
    env.sync()?;
    Ok(())
}

fn rebuild_table(
    env: &LmdbEnv,
    rw_txn: &mut LmdbWriteTransaction,
    db: LmdbDatabase,
) -> anyhow::Result<()> {
    let temp = unsafe {
        rw_txn
            .rw_txn_mut()
            .create_db(Some("temp_table"), DatabaseFlags::empty())
    }?;
    copy_table(env, rw_txn, db, temp)?;
    crate::Transaction::refresh(rw_txn);
    rw_txn.clear_db(db)?;
    copy_table(env, rw_txn, temp, db)?;
    unsafe { rw_txn.rw_txn_mut().drop_db(temp) }?;
    crate::Transaction::refresh(rw_txn);
    Ok(())
}

fn copy_table(
    env: &LmdbEnv,
    rw_txn: &mut LmdbWriteTransaction,
    source: LmdbDatabase,
    target: LmdbDatabase,
) -> anyhow::Result<()> {
    let ro_txn = env.tx_begin_read();
    {
        let mut cursor = ro_txn.txn().open_ro_cursor(source)?;
        for x in cursor.iter_start() {
            let (k, v) = x?;
            rw_txn.put(target, k, v, WriteFlags::APPEND)?;
        }
    }
    if ro_txn.txn().count(source) != rw_txn.rw_txn_mut().count(target) {
        bail!("table count mismatch");
    }
    Ok(())
}

fn do_upgrades(env: &LmdbEnv) -> anyhow::Result<()> {
    let version_store = LmdbVersionStore::new(env)?;
    let mut txn = env.tx_begin_write();

    let version = match version_store.get(&txn) {
        Some(v) => v,
        None => {
            let new_version = STORE_VERSION_MINIMUM;
            info!("Setting db version to {}", new_version);
            version_store.put(&mut txn, new_version);
            new_version
        }
    };

    if version < STORE_VERSION_MINIMUM {
        error!("The version of the ledger ({}) is lower than the minimum ({}) which is supported for upgrades. Either upgrade to a v24 node first or delete the ledger.", version, STORE_VERSION_MINIMUM);
        bail!("version too low");
    }

    if version > STORE_VERSION_CURRENT {
        error!(
            "The version of the ledger ({}) is too high for this node",
            version
        );
        bail!("version too high");
    }

    // most recent version
    Ok(())
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

    #[test]
    fn version_too_high_for_upgrade() -> anyhow::Result<()> {
        let env = LmdbEnv::new_null();
        set_store_version(&env, i32::MAX)?;
        assert_upgrade_fails(env, "version too high");
        Ok(())
    }

    #[test]
    fn version_too_low_for_upgrade() -> anyhow::Result<()> {
        let env = LmdbEnv::new_null();
        set_store_version(&env, STORE_VERSION_MINIMUM - 1)?;
        assert_upgrade_fails(env, "version too low");
        Ok(())
    }

    #[test]
    fn writes_db_version_for_new_store() {
        let env = LmdbEnv::new_null();
        let store = LmdbStore::new(env).unwrap();
        let txn = store.tx_begin_read();
        assert_eq!(store.version.get(&txn), Some(STORE_VERSION_MINIMUM));
    }

    fn assert_upgrade_fails(env: LmdbEnv, error_msg: &str) {
        let store = LmdbStore::new(env);
        match store {
            Ok(_) => panic!("store should not be created!"),
            Err(e) => {
                assert_eq!(e.to_string(), error_msg);
            }
        }
    }

    fn set_store_version(env: &LmdbEnv, current_version: i32) -> Result<(), anyhow::Error> {
        let version_store = LmdbVersionStore::new(env)?;
        let mut txn = env.tx_begin_write();
        version_store.put(&mut txn, current_version);
        Ok(())
    }
}
