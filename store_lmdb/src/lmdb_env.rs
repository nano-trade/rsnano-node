use crate::{
    LmdbConfig, LmdbReadTransaction, LmdbWriteTransaction, NullTransactionTracker, SyncStrategy,
    TransactionTracker, WriteQueue, Writer,
};
use lmdb::EnvironmentFlags;
use rsnano_nullable_lmdb::{
    ConfiguredDatabase, ConfiguredDatabaseBuilder, EnvironmentOptions, EnvironmentStubBuilder,
    LmdbDatabase, LmdbEnvironment, LmdbEnvironmentFactory,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub struct NullLmdbEnvBuilder {
    env_builder: EnvironmentStubBuilder,
}

impl NullLmdbEnvBuilder {
    pub fn database(self, name: impl Into<String>, dbi: LmdbDatabase) -> NullDatabaseBuilder {
        NullDatabaseBuilder {
            db_builder: ConfiguredDatabaseBuilder::new(name, dbi, self.env_builder),
        }
    }

    pub fn configured_database(mut self, db: ConfiguredDatabase) -> Self {
        self.env_builder = self.env_builder.configured_database(db);
        self
    }

    pub fn build(self) -> LmdbEnv {
        let env = self.env_builder.finish();
        LmdbEnv::new(env, "/nulled/ledger.ldb".into())
    }
}

pub struct NullDatabaseBuilder {
    db_builder: ConfiguredDatabaseBuilder,
}

impl NullDatabaseBuilder {
    pub fn entry(mut self, key: &[u8], value: &[u8]) -> Self {
        self.db_builder = self.db_builder.entry(key, value);
        self
    }

    pub fn build(self) -> NullLmdbEnvBuilder {
        NullLmdbEnvBuilder {
            env_builder: self.db_builder.finish(),
        }
    }
}

#[derive(Default)]
pub struct LmdbEnvFactory {
    env_factory: LmdbEnvironmentFactory,
}

impl LmdbEnvFactory {
    pub fn new_null() -> Self {
        Self {
            env_factory: LmdbEnvironmentFactory::new_null(),
        }
    }

    pub fn create_env(&self, path: impl AsRef<Path>) -> anyhow::Result<LmdbEnv> {
        let cfg = LmdbConfig::default();
        let options = EnvironmentOptions {
            path: path.as_ref(),
            max_dbs: cfg.max_databases,
            map_size: cfg.map_size,
            flags: get_env_flags(&cfg),
        };
        self.create_with_options(options)
    }

    pub fn create_with_options(&self, options: EnvironmentOptions) -> anyhow::Result<LmdbEnv> {
        let db_file_path = options.path.to_path_buf();
        let env = self.env_factory.create_env(options)?;
        Ok(LmdbEnv::new(env, db_file_path))
    }
}

pub struct LmdbEnv {
    pub environment: LmdbEnvironment,
    next_txn_id: AtomicU64,
    pub txn_tracker: Arc<dyn TransactionTracker>,
    pub write_queue: Arc<WriteQueue>,
    path: PathBuf,
}

impl LmdbEnv {
    pub fn new_null() -> Self {
        Self::new(
            LmdbEnvironment::new_null(),
            PathBuf::from("/nulled/ledger.ldb"),
        )
    }

    pub fn new_null_with() -> NullLmdbEnvBuilder {
        NullLmdbEnvBuilder {
            env_builder: EnvironmentStubBuilder::default(),
        }
    }

    pub fn new(env: LmdbEnvironment, path: PathBuf) -> Self {
        Self {
            environment: env,
            next_txn_id: AtomicU64::new(0),
            txn_tracker: Arc::new(NullTransactionTracker::new()),
            write_queue: Arc::new(WriteQueue::new()),
            path,
        }
    }

    pub fn set_transaction_tracker(&mut self, txn_tracker: Arc<dyn TransactionTracker>) {
        self.txn_tracker = txn_tracker;
    }

    pub fn tx_begin_read(&self) -> LmdbReadTransaction {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        LmdbReadTransaction::new(txn_id, &self.environment, self.create_txn_callbacks())
            .expect("Could not create LMDB read-only transaction")
    }

    pub fn tx_begin_write(&self) -> LmdbWriteTransaction {
        self.tx_begin_write_for(Writer::Generic)
    }

    pub fn tx_begin_write_for(&self, writer: Writer) -> LmdbWriteTransaction {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        LmdbWriteTransaction::new(
            txn_id,
            &self.environment,
            self.create_txn_callbacks(),
            self.write_queue.clone(),
            writer,
        )
        .expect("Could not create LMDB read-write transaction")
    }

    pub fn file_path(&self) -> &Path {
        &self.path
    }

    pub fn sync(&self) -> anyhow::Result<()> {
        self.environment.sync(true)?;
        Ok(())
    }

    fn create_txn_callbacks(&self) -> Arc<dyn TransactionTracker> {
        Arc::clone(&self.txn_tracker)
    }
}

pub fn get_env_flags(options: &LmdbConfig) -> EnvironmentFlags {
    // It seems if there's ever more threads than mdb_env_set_maxreaders has read slots available, we get failures on transaction creation unless MDB_NOTLS is specified
    // This can happen if something like 256 io_threads are specified in the node config
    // MDB_NORDAHEAD will allow platforms that support it to load the DB in memory as needed.
    // MDB_NOMEMINIT prevents zeroing malloc'ed pages. Can provide improvement for non-sensitive data but may make memory checkers noisy (e.g valgrind).
    let mut flags =
        EnvironmentFlags::NO_SUB_DIR | EnvironmentFlags::NO_TLS | EnvironmentFlags::NO_READAHEAD;

    if options.sync == SyncStrategy::NosyncSafe {
        flags |= EnvironmentFlags::NO_META_SYNC;
    } else if options.sync == SyncStrategy::NosyncUnsafe {
        flags |= EnvironmentFlags::NO_SYNC;
    } else if options.sync == SyncStrategy::NosyncUnsafeLargeMemory {
        flags |=
            EnvironmentFlags::NO_SYNC | EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC;
    } else if options.sync == SyncStrategy::NosyncUnsafeWriteMap {
        flags |= EnvironmentFlags::NO_SYNC | EnvironmentFlags::WRITE_MAP;
    }

    if !options.mem_init {
        flags |= EnvironmentFlags::NO_MEM_INIT;
    }
    flags
}

pub struct TestDbFile {
    pub path: PathBuf,
}

impl TestDbFile {
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: Path::new("/tmp").join(path),
        }
    }

    pub fn random() -> Self {
        Self::new(Self::temp_file_name())
    }

    fn temp_file_name() -> PathBuf {
        PathBuf::from(format!("{}.ldb", uuid::Uuid::new_v4().simple()))
    }

    fn lock_file_path(&self) -> PathBuf {
        let mut lock_file_path = self.path.parent().unwrap().to_owned();
        let mut fname = self.path.file_name().unwrap().to_os_string();
        fname.push("-lock");
        lock_file_path.push(fname);
        lock_file_path
    }
}

impl Drop for TestDbFile {
    fn drop(&mut self) {
        if self.path.exists() {
            std::fs::remove_file(&self.path).unwrap();
            let lock_file = self.lock_file_path();
            if lock_file.exists() {
                std::fs::remove_file(&lock_file).unwrap();
            }

            if let Some(parent) = self.path.parent() {
                if parent != Path::new("/tmp") {
                    std::fs::remove_dir(parent).unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod rw_txn {
        use super::*;
        use crate::PutEvent;
        use lmdb::{DatabaseFlags, WriteFlags};

        #[test]
        fn can_track_puts() {
            let env = LmdbEnv::new_null();

            let database = env
                .environment
                .create_db(Some("testdb"), DatabaseFlags::empty())
                .unwrap();

            let mut txn = env.tx_begin_write();
            let tracker = txn.track_puts();
            let key = &[1, 2, 3];
            let value = &[4, 5, 6];
            let flags = WriteFlags::APPEND;
            txn.put(database, key, value, flags).unwrap();

            let puts = tracker.output();
            assert_eq!(
                puts,
                vec![PutEvent {
                    database,
                    key: key.to_vec(),
                    value: value.to_vec(),
                    flags
                }]
            )
        }
    }

    mod test_db_file {
        use super::*;

        #[test]
        fn tmp_test() {
            let path = Path::new("foo.tmp");
            assert_eq!(path.parent(), Some(Path::new("")));
            assert_eq!(Path::new(""), Path::new(""))
        }

        #[test]
        fn dont_panic_when_file_not_found() {
            let file = TestDbFile::new("does-not-exist.ldb");
            drop(file)
        }

        #[test]
        fn delete_file_when_dropped() {
            let file = TestDbFile::new("drop-test.ldb");
            let mut lock_file_path = file.path.parent().unwrap().to_owned();
            lock_file_path.push("drop-test.ldb-lock");
            std::fs::write(&file.path, "foo").unwrap();
            std::fs::write(&lock_file_path, "foo").unwrap();
            let path = file.path.clone();
            drop(file);
            assert_eq!(path.exists(), false, "db file was not deleted");
            assert_eq!(lock_file_path.exists(), false, "lock file was not deleted");
        }

        #[test]
        fn delete_dir_when_dropped() {
            let file = TestDbFile::new("drop-dir/db.ldb");
            std::fs::create_dir(file.path.parent().unwrap()).unwrap();
            std::fs::write(&file.path, "foo").unwrap();
            let path = file.path.clone();
            drop(file);
            assert_eq!(path.exists(), false);
            assert_eq!(path.parent().unwrap().exists(), false);
        }

        #[test]
        fn tmp_file_name() {
            let filename = TestDbFile::temp_file_name();
            assert_eq!(filename.extension().unwrap(), "ldb");
            assert_eq!(filename.file_stem().unwrap().len(), 32);
            assert_ne!(TestDbFile::temp_file_name(), TestDbFile::temp_file_name());
        }
    }
}
