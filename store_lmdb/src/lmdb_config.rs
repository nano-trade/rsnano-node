use std::path::PathBuf;

use rsnano_nullable_lmdb::{EnvironmentFlags, EnvironmentOptions};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum SyncStrategy {
    /** Always flush to disk on commit. This is default. */
    Always,
    /** Do not flush meta data eagerly. This may cause loss of transactions, but maintains integrity. */
    NosyncSafe,

    /**
     * Let the OS decide when to flush to disk. On filesystems with write ordering, this has the same
     * guarantees as nosync_safe, otherwise corruption may occur on system crash.
     */
    NosyncUnsafe,
    /**
     * Use a writeable memory map. Let the OS decide when to flush to disk, and make the request asynchronous.
     * This may give better performance on systems where the database fits entirely in memory, otherwise is
     * may be slower.
     * @warning Do not use this option if external processes uses the database concurrently.
     */
    NosyncUnsafeLargeMemory,

    /// Never sync
    NosyncUnsafeWriteMap,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LmdbConfig {
    pub sync: SyncStrategy,
    pub max_databases: u32,
    pub map_size: usize,
    pub mem_init: bool,
}

impl Default for LmdbConfig {
    fn default() -> Self {
        Self {
            sync: SyncStrategy::Always,
            max_databases: 128,
            map_size: 256 * 1024 * 1024 * 1024,
            mem_init: false,
        }
    }
}

impl LmdbConfig {
    pub fn new() -> Self {
        Default::default()
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

pub fn default_ledger_lmdb_options(path: impl Into<PathBuf>) -> EnvironmentOptions {
    EnvironmentOptions {
        max_dbs: 128,
        map_size: 256 * 1024 * 1024 * 1024,
        flags: EnvironmentFlags::NO_SUB_DIR
            | EnvironmentFlags::NO_TLS
            | EnvironmentFlags::NO_MEM_INIT,
        path: path.into(),
    }
}
