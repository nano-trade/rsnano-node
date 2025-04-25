use crate::{
    get_env_flags, LmdbAccountStore, LmdbBlockStore, LmdbConfig, LmdbConfirmationHeightStore,
    LmdbDatabase, LmdbEnv, LmdbEnvFactory, LmdbFinalVoteStore, LmdbOnlineWeightStore,
    LmdbPeerStore, LmdbPendingStore, LmdbPrunedStore, LmdbReadTransaction, LmdbRepWeightStore,
    LmdbVersionStore, LmdbWriteTransaction, NullTransactionTracker, TransactionTracker, WriteQueue,
    Writer, STORE_VERSION_CURRENT, STORE_VERSION_MINIMUM,
};
use lmdb::{DatabaseFlags, WriteFlags};
use lmdb_sys::MDB_SUCCESS;
use rsnano_core::utils::UnixTimestamp;
use rsnano_nullable_lmdb::EnvironmentOptions;
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
    pub online_weight: Arc<LmdbOnlineWeightStore>,
    pub pruned: Arc<LmdbPrunedStore>,
    pub rep_weight: Arc<LmdbRepWeightStore>,
    pub peer: Arc<LmdbPeerStore>,
    pub confirmation_height: Arc<LmdbConfirmationHeightStore>,
    pub final_vote: Arc<LmdbFinalVoteStore>,
    pub version: Arc<LmdbVersionStore>,
}

pub struct LmdbStoreBuilder<'a> {
    path: &'a Path,
    options: Option<LmdbConfig>,
    tracker: Option<Arc<dyn TransactionTracker>>,
}

impl<'a> LmdbStoreBuilder<'a> {
    fn new(path: &'a Path) -> Self {
        Self {
            path,
            options: None,
            tracker: None,
        }
    }

    pub fn options(mut self, options: LmdbConfig) -> Self {
        self.options = Some(options);
        self
    }

    pub fn txn_tracker(mut self, tracker: Arc<dyn TransactionTracker>) -> Self {
        self.tracker = Some(tracker);
        self
    }

    pub fn build(self, env_factory: &LmdbEnvFactory) -> anyhow::Result<LmdbStore> {
        let options = self.options.unwrap_or_default();

        let txn_tracker = self
            .tracker
            .unwrap_or_else(|| Arc::new(NullTransactionTracker::new()));

        LmdbStore::new(env_factory, self.path, &options, txn_tracker)
    }
}

impl LmdbStore {
    pub fn new_null() -> Self {
        Self::new_with_env(LmdbEnv::new_null()).unwrap()
    }

    pub fn open(path: &Path) -> LmdbStoreBuilder<'_> {
        LmdbStoreBuilder::new(path)
    }

    fn new(
        env_factory: &LmdbEnvFactory,
        path: impl AsRef<Path>,
        options: &LmdbConfig,
        txn_tracker: Arc<dyn TransactionTracker>,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref();
        upgrade_if_needed(path, env_factory)?;

        let env_options = EnvironmentOptions {
            max_dbs: options.max_databases,
            map_size: options.map_size,
            flags: get_env_flags(options),
            path,
        };
        let mut env = env_factory.create_with_options(env_options)?;
        env.set_transaction_tracker(txn_tracker);
        Self::new_with_env(env)
    }

    fn new_with_env(env: LmdbEnv) -> anyhow::Result<Self> {
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

fn upgrade_if_needed(path: &Path, env_factory: &LmdbEnvFactory) -> Result<(), anyhow::Error> {
    let env = Arc::new(env_factory.create_env(path)?);
    let upgrade_info = LmdbVersionStore::check_upgrade(&env)?;
    if upgrade_info.is_fully_upgraded {
        debug!("No database upgrade needed");
        return Ok(());
    }

    info!("Upgrade in progress...");
    do_upgrades(env.clone())?;
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

fn do_upgrades(env: Arc<LmdbEnv>) -> anyhow::Result<()> {
    let version_store = LmdbVersionStore::new(&env)?;
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
    use crate::TestDbFile;

    #[test]
    fn create_store() -> anyhow::Result<()> {
        let file = TestDbFile::random();
        let _ = LmdbStore::open(&file.path).build(&Default::default())?;
        Ok(())
    }

    #[test]
    fn version_too_high_for_upgrade() -> anyhow::Result<()> {
        let file = TestDbFile::random();
        set_store_version(&file, i32::MAX)?;
        assert_upgrade_fails(&file.path, "version too high");
        Ok(())
    }

    #[test]
    fn version_too_low_for_upgrade() -> anyhow::Result<()> {
        let file = TestDbFile::random();
        set_store_version(&file, STORE_VERSION_MINIMUM - 1)?;
        assert_upgrade_fails(&file.path, "version too low");
        Ok(())
    }

    #[test]
    fn writes_db_version_for_new_store() {
        let file = TestDbFile::random();
        let store = LmdbStore::open(&file.path)
            .build(&Default::default())
            .unwrap();
        let txn = store.tx_begin_read();
        assert_eq!(store.version.get(&txn), Some(STORE_VERSION_MINIMUM));
    }

    fn assert_upgrade_fails(path: &Path, error_msg: &str) {
        match LmdbStore::open(path).build(&Default::default()) {
            Ok(_) => panic!("store should not be created!"),
            Err(e) => {
                assert_eq!(e.to_string(), error_msg);
            }
        }
    }

    fn set_store_version(file: &TestDbFile, current_version: i32) -> Result<(), anyhow::Error> {
        let env = Arc::new(LmdbEnvFactory::default().create_env(&file.path)?);
        let version_store = LmdbVersionStore::new(&env)?;
        let mut txn = env.tx_begin_write();
        version_store.put(&mut txn, current_version);
        Ok(())
    }
}
