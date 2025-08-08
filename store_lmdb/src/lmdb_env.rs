use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use rsnano_nullable_lmdb::{
    ConfiguredDatabase, ConfiguredDatabaseBuilder, EnvironmentOptions, EnvironmentStubBuilder,
    LmdbDatabase, LmdbEnvironment, LmdbEnvironmentFactory,
};

use crate::{LmdbConfig, ReadTransaction, SyncStrategy, WriteTransaction};
use lmdb::EnvironmentFlags;

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
        LmdbEnv::new(env, "/nulled/ledger.ldb")
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

    pub fn create_env(&self, path: impl Into<PathBuf>) -> anyhow::Result<LmdbEnv> {
        let cfg = LmdbConfig::default();
        let options = EnvironmentOptions {
            path: path.into(),
            max_dbs: cfg.max_databases,
            map_size: cfg.map_size,
            flags: get_lmdb_flags(&cfg),
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

    pub fn new(env: LmdbEnvironment, path: impl Into<PathBuf>) -> Self {
        Self {
            environment: env,
            path: path.into(),
        }
    }

    pub fn tx_begin_read(&self) -> ReadTransaction {
        ReadTransaction::new(&self.environment)
            .expect("Could not create LMDB read-only transaction")
    }

    pub fn tx_begin_write(&self) -> WriteTransaction {
        WriteTransaction::new(&self.environment)
            .expect("Could not create LMDB read-write transaction")
    }

    pub fn file_path(&self) -> &Path {
        &self.path
    }

    pub fn sync(&self) -> anyhow::Result<()> {
        self.environment.sync(true)?;
        Ok(())
    }

    pub fn copy_db(&self, destination: &Path) -> lmdb::Result<()> {
        self.environment.copy_db(destination)
    }
}

pub fn get_lmdb_flags(config: &LmdbConfig) -> EnvironmentFlags {
    // It seems if there's ever more threads than mdb_env_set_maxreaders has read slots available, we get failures on transaction creation unless MDB_NOTLS is specified
    // This can happen if something like 256 io_threads are specified in the node config
    // MDB_NORDAHEAD will allow platforms that support it to load the DB in memory as needed.
    // MDB_NOMEMINIT prevents zeroing malloc'ed pages. Can provide improvement for non-sensitive data but may make memory checkers noisy (e.g valgrind).
    let mut flags = EnvironmentFlags::NO_SUB_DIR | EnvironmentFlags::NO_TLS;

    if config.sync == SyncStrategy::NosyncSafe {
        flags |= EnvironmentFlags::NO_META_SYNC;
    } else if config.sync == SyncStrategy::NosyncUnsafe {
        flags |= EnvironmentFlags::NO_SYNC | EnvironmentFlags::NO_META_SYNC;
    } else if config.sync == SyncStrategy::NosyncUnsafeLargeMemory {
        flags |=
            EnvironmentFlags::NO_SYNC | EnvironmentFlags::WRITE_MAP | EnvironmentFlags::MAP_ASYNC;
    } else if config.sync == SyncStrategy::NosyncUnsafeWriteMap {
        flags |= EnvironmentFlags::NO_SYNC | EnvironmentFlags::WRITE_MAP;
    }

    if !config.mem_init {
        flags |= EnvironmentFlags::NO_MEM_INIT;
    }
    flags
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
}
