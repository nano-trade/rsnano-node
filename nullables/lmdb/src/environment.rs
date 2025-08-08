use std::{
    ffi::CString,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use lmdb::{DatabaseFlags, EnvironmentFlags, Stat};
use lmdb_sys::{MDB_env, MDB_CP_COMPACT, MDB_SUCCESS};

use super::{ConfiguredDatabase, LmdbDatabase, RoTransaction, RwTransaction};
use crate::{ConfiguredDatabaseBuilder, ReadTransaction, Result, WriteTransaction};

#[derive(Clone)]
pub struct EnvironmentOptions {
    pub max_dbs: u32,
    pub map_size: usize,
    pub flags: EnvironmentFlags,
    pub path: PathBuf,
}

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

    pub fn error(mut self, key: &[u8], error: lmdb::Error) -> Self {
        self.db_builder = self.db_builder.error(key, error);
        self
    }

    pub fn build(self) -> NullLmdbEnvBuilder {
        NullLmdbEnvBuilder {
            env_builder: self.db_builder.finish(),
        }
    }
}

#[derive(Default)]
pub struct LmdbEnvironmentFactory {
    is_nulled: bool,
}

impl LmdbEnvironmentFactory {
    pub fn new_null() -> Self {
        Self { is_nulled: true }
    }

    pub fn create(&self, options: EnvironmentOptions) -> Result<LmdbEnv> {
        let db_file_path = options.path.to_path_buf();

        if self.is_nulled {
            Ok(LmdbEnv::new(LmdbEnvironment::new_null(), db_file_path))
        } else {
            Ok(LmdbEnv::new(LmdbEnvironment::new(options)?, db_file_path))
        }
    }
}

pub struct LmdbEnv {
    environment: LmdbEnvironment,
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

    pub fn begin_read(&self) -> ReadTransaction {
        ReadTransaction::new(&self.environment)
            .expect("Could not create LMDB read-only transaction")
    }

    pub fn begin_write(&self) -> WriteTransaction {
        WriteTransaction::new(&self.environment)
            .expect("Could not create LMDB read-write transaction")
    }

    pub fn file_path(&self) -> &Path {
        &self.path
    }

    pub fn sync(&self) -> Result<()> {
        self.environment.sync(true)
    }

    pub fn copy_db(&self, destination: &Path) -> Result<()> {
        self.environment.copy_db(destination)
    }

    pub fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> Result<LmdbDatabase> {
        self.environment.create_db(name, flags)
    }

    pub fn open_db(&self, name: Option<&str>) -> Result<LmdbDatabase> {
        self.environment.open_db(name)
    }

    pub fn stat(&self) -> Result<Stat> {
        self.environment.stat()
    }
}

pub struct LmdbEnvironment(EnvironmentStrategy);

impl LmdbEnvironment {
    pub fn new(options: EnvironmentOptions) -> Result<Self> {
        Ok(Self(EnvironmentStrategy::Real(EnvironmentWrapper::build(
            options,
        )?)))
    }

    pub fn new_with(env: lmdb::Environment) -> Self {
        Self(EnvironmentStrategy::Real(EnvironmentWrapper::new(env)))
    }

    pub fn new_null() -> Self {
        Self::new_null_with(Vec::new())
    }

    pub fn new_null_with(databases: Vec<ConfiguredDatabase>) -> Self {
        Self(EnvironmentStrategy::Nulled(EnvironmentStub {
            databases: Arc::new(Mutex::new(databases)),
        }))
    }

    pub fn null_builder() -> EnvironmentStubBuilder {
        EnvironmentStubBuilder::default()
    }

    pub fn begin_ro_txn(&self) -> lmdb::Result<RoTransaction> {
        match &self.0 {
            EnvironmentStrategy::Real(s) => s.begin_ro_txn(),
            EnvironmentStrategy::Nulled(s) => s.begin_ro_txn(),
        }
    }

    pub fn begin_rw_txn(&self) -> lmdb::Result<RwTransaction> {
        match &self.0 {
            EnvironmentStrategy::Real(s) => s.begin_rw_txn(),
            EnvironmentStrategy::Nulled(s) => s.begin_rw_txn(),
        }
    }

    pub fn create_db(
        &self,
        name: Option<&str>,
        flags: DatabaseFlags,
    ) -> lmdb::Result<LmdbDatabase> {
        match &self.0 {
            EnvironmentStrategy::Real(s) => s.create_db(name, flags),
            EnvironmentStrategy::Nulled(s) => s.create_db(name, flags),
        }
    }

    pub fn env(&self) -> *mut MDB_env {
        match &self.0 {
            EnvironmentStrategy::Real(s) => s.env(),
            EnvironmentStrategy::Nulled(_) => unimplemented!(),
        }
    }

    pub fn open_db(&self, name: Option<&str>) -> lmdb::Result<LmdbDatabase> {
        match &self.0 {
            EnvironmentStrategy::Real(s) => s.open_db(name),
            EnvironmentStrategy::Nulled(s) => s.open_db(name),
        }
    }

    pub fn sync(&self, force: bool) -> lmdb::Result<()> {
        if let EnvironmentStrategy::Real(s) = &self.0 {
            s.sync(force)?;
        }
        Ok(())
    }

    pub fn stat(&self) -> lmdb::Result<Stat> {
        match &self.0 {
            EnvironmentStrategy::Real(s) => s.stat(),
            EnvironmentStrategy::Nulled(s) => s.stat(),
        }
    }

    pub fn copy_db(&self, destination: &Path) -> lmdb::Result<()> {
        if let EnvironmentStrategy::Real(_) = &self.0 {
            let c_path = CString::new(destination.as_os_str().to_str().unwrap()).unwrap();
            let status =
                unsafe { lmdb_sys::mdb_env_copy2(self.env(), c_path.as_ptr(), MDB_CP_COMPACT) };
            if status == MDB_SUCCESS {
                Ok(())
            } else {
                Err(lmdb::Error::Other(status))
            }
        } else {
            Ok(())
        }
    }
}

enum EnvironmentStrategy {
    Nulled(EnvironmentStub),
    Real(EnvironmentWrapper),
}

struct EnvironmentWrapper(lmdb::Environment);

impl EnvironmentWrapper {
    fn new(env: lmdb::Environment) -> Self {
        Self(env)
    }

    fn build(options: EnvironmentOptions) -> lmdb::Result<Self> {
        let env = lmdb::Environment::new()
            .set_max_dbs(options.max_dbs)
            .set_map_size(options.map_size)
            .set_flags(options.flags)
            .open_with_permissions(&options.path, 0o600.try_into().unwrap())?;
        Ok(Self(env))
    }

    fn begin_ro_txn(&self) -> lmdb::Result<RoTransaction> {
        self.0.begin_ro_txn().map(|txn| {
            // todo: don't use static life time
            let txn = unsafe {
                std::mem::transmute::<lmdb::RoTransaction<'_>, lmdb::RoTransaction<'static>>(txn)
            };
            RoTransaction::new(txn)
        })
    }

    fn begin_rw_txn(&self) -> lmdb::Result<RwTransaction> {
        self.0.begin_rw_txn().map(|txn| {
            // todo: don't use static life time
            let txn = unsafe {
                std::mem::transmute::<lmdb::RwTransaction<'_>, lmdb::RwTransaction<'static>>(txn)
            };
            RwTransaction::new(txn)
        })
    }

    fn create_db(&self, name: Option<&str>, flags: DatabaseFlags) -> lmdb::Result<LmdbDatabase> {
        self.0.create_db(name, flags).map(LmdbDatabase::new)
    }

    fn env(&self) -> *mut MDB_env {
        self.0.env()
    }

    fn open_db(&self, name: Option<&str>) -> lmdb::Result<LmdbDatabase> {
        self.0.open_db(name).map(LmdbDatabase::new)
    }

    fn sync(&self, force: bool) -> lmdb::Result<()> {
        self.0.sync(force)
    }

    fn stat(&self) -> lmdb::Result<Stat> {
        self.0.stat()
    }
}

struct EnvironmentStub {
    databases: Arc<Mutex<Vec<ConfiguredDatabase>>>,
}

impl EnvironmentStub {
    fn begin_ro_txn(&self) -> lmdb::Result<RoTransaction> {
        //todo  don't clone!
        Ok(RoTransaction::new_null(
            self.databases.lock().unwrap().clone(),
        ))
    }

    fn begin_rw_txn(&self) -> lmdb::Result<RwTransaction> {
        //todo  don't clone!
        Ok(RwTransaction::new_null(self.databases.clone()))
    }

    fn create_db(&self, name: Option<&str>, _flags: DatabaseFlags) -> lmdb::Result<LmdbDatabase> {
        let mut guard = self.databases.lock().unwrap();
        if let Some(db) = guard.iter().find(|x| name == Some(&x.db_name)) {
            return Ok(db.dbi);
        }

        let dbi = create_dbi(&guard);
        guard.push(ConfiguredDatabase::new(dbi, name.unwrap().to_owned()));
        Ok(dbi)
    }

    fn open_db(&self, name: Option<&str>) -> lmdb::Result<LmdbDatabase> {
        self.databases
            .lock()
            .unwrap()
            .iter()
            .find(|x| name == Some(&x.db_name))
            .map(|x| x.dbi)
            .ok_or(lmdb::Error::NotFound)
    }

    fn stat(&self) -> lmdb::Result<Stat> {
        todo!()
    }
}

fn create_dbi(guard: &std::sync::MutexGuard<'_, Vec<ConfiguredDatabase>>) -> LmdbDatabase {
    let id = guard.iter().map(|i| i.dbi.as_nulled()).max().unwrap_or(41) + 1;
    LmdbDatabase::new_null(id)
}

#[derive(Default)]
pub struct EnvironmentStubBuilder {
    databases: Vec<ConfiguredDatabase>,
}

impl EnvironmentStubBuilder {
    pub fn database(self, name: impl Into<String>, dbi: LmdbDatabase) -> ConfiguredDatabaseBuilder {
        ConfiguredDatabaseBuilder::new(name, dbi, self)
    }

    pub fn configured_database(mut self, db: ConfiguredDatabase) -> Self {
        if self
            .databases
            .iter()
            .any(|x| x.dbi == db.dbi || x.db_name == db.db_name)
        {
            panic!(
                "trying to duplicated database for {} / {}",
                db.dbi.as_nulled(),
                db.db_name
            );
        }
        self.databases.push(db);
        self
    }

    pub fn finish(self) -> LmdbEnvironment {
        LmdbEnvironment::new_null_with(self.databases)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PutEvent;
    use lmdb::WriteFlags;
    use std::{
        env::temp_dir,
        ops::Deref,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn open_unknown_database_fails() {
        let path = TempLmdbFile::new();
        let env = create_lmdb_env(path);
        let result = env.open_db(Some("UNKNOWN"));
        assert_eq!(result, Err(lmdb::Error::NotFound));
    }

    #[test]
    fn create_db() {
        let path = TempLmdbFile::new();
        let env = create_lmdb_env(path);
        env.create_db(Some("mydb"), DatabaseFlags::empty()).unwrap();
        let result = env.open_db(Some("mydb"));
        assert!(result.is_ok());
    }

    #[test]
    fn write_key_value() {
        let path = TempLmdbFile::new();
        let env = create_lmdb_env(path);
        let dbi = env.create_db(Some("mydb"), DatabaseFlags::empty()).unwrap();
        {
            let mut tx = env.begin_rw_txn().unwrap();
            tx.put(dbi, &[1, 2], &[3, 4], WriteFlags::empty()).unwrap();
            tx.commit().unwrap();
        }
        let tx = env.begin_ro_txn().unwrap();
        let result = tx.get(dbi, &[1, 2]).unwrap();
        assert_eq!(result, [3, 4]);
    }

    #[test]
    fn can_track_puts() {
        let env = LmdbEnv::new_null();

        let database = env
            .create_db(Some("testdb"), DatabaseFlags::empty())
            .unwrap();

        let mut txn = env.begin_write();
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

    mod nullability {
        use super::*;

        #[test]
        fn read_database() {
            let database = LmdbDatabase::new_null(1);
            let env = LmdbEnvironment::null_builder()
                .database("foo", database)
                .entry(&[1, 2], &[3, 4])
                .finish()
                .finish();

            let tx = env.begin_ro_txn().unwrap();
            let result = tx.get(database, &[1, 2]).unwrap();
            assert_eq!(result, [3, 4]);
        }

        #[test]
        fn open_unknown_database_fails() {
            let env = LmdbEnvironment::new_null();
            let result = env.open_db(Some("UNKNOWN"));
            assert_eq!(result, Err(lmdb::Error::NotFound));
        }

        #[test]
        fn create_db() {
            let env = LmdbEnvironment::new_null();
            env.create_db(Some("mydb"), DatabaseFlags::empty()).unwrap();
            let result = env.open_db(Some("mydb"));
            assert!(result.is_ok());
        }

        #[test]
        fn write_key_value() {
            let env = LmdbEnvironment::new_null();
            let dbi = env.create_db(Some("mydb"), DatabaseFlags::empty()).unwrap();
            {
                let mut tx = env.begin_rw_txn().unwrap();
                tx.put(dbi, &[1, 2], &[3, 4], WriteFlags::empty()).unwrap();
                tx.commit().unwrap();
            }
            let tx = env.begin_ro_txn().unwrap();
            let result = tx.get(dbi, &[1, 2]).unwrap();
            assert_eq!(result, [3, 4]);
        }
    }

    fn create_lmdb_env(path: TempLmdbFile) -> LmdbEnvironment {
        let opts = EnvironmentOptions {
            max_dbs: 3,
            map_size: 1024 * 1024,
            flags: EnvironmentFlags::NO_SUB_DIR
                | EnvironmentFlags::NO_TLS
                | EnvironmentFlags::NO_READAHEAD
                | EnvironmentFlags::NO_SYNC
                | EnvironmentFlags::WRITE_MAP,
            path: path.to_path_buf(),
        };
        LmdbEnvironment::new(opts).unwrap()
    }

    static FILE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct TempLmdbFile(PathBuf);

    impl TempLmdbFile {
        pub fn new() -> Self {
            let mut path = temp_dir();
            path.push(format!(
                "lmdbtest-{}.ldb",
                FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            Self(path)
        }
    }

    impl Drop for TempLmdbFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    impl Deref for TempLmdbFile {
        type Target = Path;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
}
