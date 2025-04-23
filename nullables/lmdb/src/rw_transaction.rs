use super::{ConfiguredDatabase, LmdbDatabase, RoCursor};
use lmdb::DatabaseFlags;
use std::sync::{Arc, Mutex};

pub struct RwTransaction {
    strategy: RwTransactionStrategy,
}

impl RwTransaction {
    pub fn new(tx: lmdb::RwTransaction<'static>) -> Self {
        Self {
            strategy: RwTransactionStrategy::Real(RwTransactionWrapper(tx)),
        }
    }

    pub fn new_null(databases: Arc<Mutex<Vec<ConfiguredDatabase>>>) -> Self {
        let db_copies = databases.lock().unwrap().clone();
        Self {
            strategy: RwTransactionStrategy::Nulled(RwTransactionStub {
                db_copies,
                databases,
            }),
        }
    }

    pub fn get(&self, database: LmdbDatabase, key: &[u8]) -> lmdb::Result<&[u8]> {
        match &self.strategy {
            RwTransactionStrategy::Real(s) => s.get(database, key),
            RwTransactionStrategy::Nulled(s) => s.get(database, key),
        }
    }

    pub fn put(
        &mut self,
        database: LmdbDatabase,
        key: &[u8],
        data: &[u8],
        flags: lmdb::WriteFlags,
    ) -> lmdb::Result<()> {
        match &mut self.strategy {
            RwTransactionStrategy::Real(s) => {
                s.put(database.as_real(), key, data, flags)?;
            }
            RwTransactionStrategy::Nulled(s) => s.put(database, key, data)?,
        }
        Ok(())
    }

    pub fn del(
        &mut self,
        database: LmdbDatabase,
        key: &[u8],
        flags: Option<&[u8]>,
    ) -> lmdb::Result<()> {
        if let RwTransactionStrategy::Real(s) = &mut self.strategy {
            s.del(database.as_real(), key, flags)?;
        }
        Ok(())
    }

    /// ## Safety
    ///
    /// This function (as well as `Environment::open_db`,
    /// `Environment::create_db`, and `Database::open`) **must not** be called
    /// from multiple concurrent transactions in the same environment. A
    /// transaction which uses this function must finish (either commit or
    /// abort) before any other transaction may use this function.
    pub unsafe fn create_db(
        &self,
        name: Option<&str>,
        flags: DatabaseFlags,
    ) -> lmdb::Result<LmdbDatabase> {
        match &self.strategy {
            RwTransactionStrategy::Real(s) => s.create_db(name, flags),
            RwTransactionStrategy::Nulled(s) => s.create_db(name, flags),
        }
    }

    /// ## Safety
    ///
    /// This method is unsafe in the same ways as `Environment::close_db`, and
    /// should be used accordingly.
    pub unsafe fn drop_db(&mut self, database: LmdbDatabase) -> lmdb::Result<()> {
        if let RwTransactionStrategy::Real(s) = &mut self.strategy {
            s.drop_db(database.as_real())?;
        }
        Ok(())
    }

    pub fn clear_db(&mut self, database: LmdbDatabase) -> lmdb::Result<()> {
        if let RwTransactionStrategy::Real(s) = &mut self.strategy {
            s.clear_db(database.as_real())?;
        }
        Ok(())
    }

    pub fn open_ro_cursor(&self, database: LmdbDatabase) -> lmdb::Result<RoCursor> {
        match &self.strategy {
            RwTransactionStrategy::Real(s) => s.open_ro_cursor(database),
            RwTransactionStrategy::Nulled(s) => s.open_ro_cursor(database),
        }
    }

    pub fn count(&self, database: LmdbDatabase) -> u64 {
        match &self.strategy {
            RwTransactionStrategy::Real(s) => s.count(database.as_real()),
            RwTransactionStrategy::Nulled(_) => 0,
        }
    }

    pub fn commit(self) -> lmdb::Result<()> {
        match self.strategy {
            RwTransactionStrategy::Real(s) => s.commit()?,
            RwTransactionStrategy::Nulled(s) => s.commit(),
        }
        Ok(())
    }
}

enum RwTransactionStrategy {
    Real(RwTransactionWrapper),
    Nulled(RwTransactionStub),
}

pub struct RwTransactionWrapper(lmdb::RwTransaction<'static>);

impl RwTransactionWrapper {
    fn get(&self, database: LmdbDatabase, key: &[u8]) -> lmdb::Result<&[u8]> {
        lmdb::Transaction::get(&self.0, database.as_real(), &key)
    }

    fn put(
        &mut self,
        database: lmdb::Database,
        key: &[u8],
        data: &[u8],
        flags: lmdb::WriteFlags,
    ) -> lmdb::Result<()> {
        lmdb::RwTransaction::put(&mut self.0, database, &key, &data, flags)
    }

    fn del(
        &mut self,
        database: lmdb::Database,
        key: &[u8],
        flags: Option<&[u8]>,
    ) -> lmdb::Result<()> {
        lmdb::RwTransaction::del(&mut self.0, database, &key, flags)
    }

    fn clear_db(&mut self, database: lmdb::Database) -> lmdb::Result<()> {
        lmdb::RwTransaction::clear_db(&mut self.0, database)
    }

    fn commit(self) -> lmdb::Result<()> {
        lmdb::Transaction::commit(self.0)
    }

    fn open_ro_cursor(&self, database: LmdbDatabase) -> lmdb::Result<RoCursor> {
        let cursor = lmdb::Transaction::open_ro_cursor(&self.0, database.as_real());
        cursor.map(RoCursor::new)
    }

    fn count(&self, database: lmdb::Database) -> u64 {
        let stat = lmdb::Transaction::stat(&self.0, database);
        stat.unwrap().entries() as u64
    }

    /// ## Safety
    ///
    /// This method is unsafe in the same ways as `Environment::close_db`, and
    /// should be used accordingly.
    unsafe fn drop_db(&mut self, database: lmdb::Database) -> lmdb::Result<()> {
        lmdb::RwTransaction::drop_db(&mut self.0, database)
    }

    /// ## Safety
    ///
    /// This function (as well as `Environment::open_db`,
    /// `Environment::create_db`, and `Database::open`) **must not** be called
    /// from multiple concurrent transactions in the same environment. A
    /// transaction which uses this function must finish (either commit or
    /// abort) before any other transaction may use this function.
    unsafe fn create_db(
        &self,
        name: Option<&str>,
        flags: DatabaseFlags,
    ) -> lmdb::Result<LmdbDatabase> {
        lmdb::RwTransaction::create_db(&self.0, name, flags).map(LmdbDatabase::new)
    }
}

pub struct RwTransactionStub {
    db_copies: Vec<ConfiguredDatabase>,
    databases: Arc<Mutex<Vec<ConfiguredDatabase>>>,
}

impl RwTransactionStub {
    fn get(&self, database: LmdbDatabase, key: &[u8]) -> lmdb::Result<&[u8]> {
        let db = self.get_database(database)?;

        match db.entries.get(key) {
            Some(value) => Ok(value),
            None => Err(lmdb::Error::NotFound),
        }
    }

    fn put(&mut self, database: LmdbDatabase, key: &[u8], data: &[u8]) -> lmdb::Result<()> {
        let db = self.get_database_mut(database)?;
        db.entries.insert(key.to_vec(), data.to_vec());
        Ok(())
    }

    fn open_ro_cursor(&self, database: LmdbDatabase) -> lmdb::Result<RoCursor> {
        Ok(RoCursor::new_null_with(
            self.db_copies.iter().find(|db| db.dbi == database).unwrap(),
        ))
    }

    fn create_db(&self, _name: Option<&str>, _flags: DatabaseFlags) -> lmdb::Result<LmdbDatabase> {
        Ok(LmdbDatabase::new_null(42))
    }

    fn get_database(&self, database: LmdbDatabase) -> lmdb::Result<&ConfiguredDatabase> {
        self.db_copies
            .iter()
            .find(|d| d.dbi == database)
            .ok_or(lmdb::Error::NotFound)
    }

    fn get_database_mut(
        &mut self,
        database: LmdbDatabase,
    ) -> lmdb::Result<&mut ConfiguredDatabase> {
        self.db_copies
            .iter_mut()
            .find(|d| d.dbi == database)
            .ok_or(lmdb::Error::NotFound)
    }

    fn commit(self) {
        *self.databases.lock().unwrap() = self.db_copies;
    }
}
