// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std::collections::btree_map::{
    Entry,
};

use std::path::{
    Path,
    PathBuf,
};

/// A process is only permitted to have one open handle to each database. This manager
/// exists to enforce that constraint: don't open databases directly.
lazy_static! {
    static ref MANAGER: RwLock<Stores> = RwLock::new(Stores::new());
}

/// A struct to store a tuple of a path to a store
/// and the connection to that store. We stores these things
/// together to ensure that two stores at different paths cannot
/// be opened with the same name.
struct StoreConnection {
    conn: Weak<Conn>,
    path: PathBuf,
}

impl StoreConnection {
    fn new<T>(conn: &Arc<Conn>, path: T) -> StoreConnection where T: AsRef<Path> {
        StoreConnection {
            conn: Arc::downgrade(conn),
            path: path.as_ref().to_path_buf(),
        }
    }
}

/// Stores keeps a reference to a Conn that has been opened for a store
/// along with the path to the store and a key that uniquely identifies
/// that store. The key is stored as a String so that multiple in memory stores
/// can be named and uniquely identified.
pub struct Stores {
    connections: BTreeMap<String, StoreConnection>,
}

impl Stores {
    fn new() -> Stores {
        Stores {
            connections: Default::default(),
        }
    }

    pub fn singleton() -> &'static RwLock<Stores> {
        &*MANAGER
    }

    fn is_store_open(path: &str) -> bool {
        Stores::singleton().read().unwrap().is_open(path)
    }

    pub fn open_store<T>(path: T) -> Result<Store> where T: AsRef<Path> {
        let path_ref = path.as_ref();
        let path_string = path_ref.to_string_lossy();
        let (name, cannonical) = if path_string.len() > 0 {
            let cannonical = path_ref.canonicalize()?;
            let name: String = cannonical.to_string_lossy().into();
            (name, cannonical)
        } else {
            (path_string.into(), path_ref.to_path_buf())
        };
        Stores::singleton().write().unwrap().open(&name, cannonical)
    }

    pub fn open_named_in_memory_store(name: &str) -> Result<Store> {
        Stores::singleton().write().unwrap().open(name, "")
    }

    pub fn get_store<T>(path: T) -> Result<Option<Store>> where T: AsRef<Path> {
        let cannonical = path.as_ref().canonicalize()?;
        Stores::singleton().write().unwrap().get(cannonical.to_str().unwrap())
    }

    pub fn get_named_in_memory_store(name: &str) -> Result<Option<Store>> {
        Stores::singleton().write().unwrap().get(name)
    }

    pub fn connect_store<T>(path: T) -> Result<Store> where T: AsRef<Path> {
        let name = path.as_ref().canonicalize()?;
        Stores::singleton().write().unwrap().connect(name.to_str().unwrap())
    }

    pub fn connect_named_in_memory_store(name: &str) -> Result<Store> {
        Stores::singleton().write().unwrap().connect(name)
    }

    pub fn close_store<T>(path: T) -> Result<()> where T: AsRef<Path> {
        let name = path.as_ref().canonicalize()?;
        Stores::singleton().write().unwrap().close(name.to_str().unwrap())
    }

    pub fn close_named_in_memory_store(name: &str) -> Result<()> {
        Stores::singleton().write().unwrap().close(name)
    }
}

impl Stores {

    // Returns true if there exists an entry for the provided name in the connections map.
    // This does not guarentee that the weak reference we hold to the Conn is still valid.
    fn is_open(&self, name: &str) -> bool {
        self.connections.contains_key(name)
    }

    // Open a store with an existing connection if available, or
    // create a new connection if not.
    pub fn open<T>(&mut self, name: &str, path: T) -> Result<Store> where T: AsRef<Path> {
        let store_conn = self.conn(name)?;
        Ok(match store_conn {
            Some((store_path, conn)) => {
                let new_path = path.as_ref().to_path_buf();
                if store_path != new_path {
                    bail!(ErrorKind::StorePathMismatch(name.to_string(), new_path, store_path));
                }
                Store {
                    conn: conn,
                    sqlite: ::new_connection(path)?,
                }
            },
            None => {
                let store = Store::open(&path)?;
                self.connections.insert(name.to_string(), StoreConnection::new(store.conn(), &path));
                store
            },
        })
    }

    // Fetches the conn for the provided path. It is possible that the weak conn in our
    // stored conns is no longer valid as it doesn't have any strong references left,
    // however, as we have previously opened it and not explicitly closed it, what we do
    // is re-create it for the same path.
    // If the conn has never been created, or closed, then it will no longer have a weak
    // reference in our stored conns map and therefore we return None.
    fn conn(&mut self, name: &str) -> Result<Option<(PathBuf, Arc<Conn>)>> {
        Ok(match self.connections.entry(name.to_string()) {
            Entry::Occupied(mut entry) => {
                let mut store_conn = entry.get_mut();
                let path = store_conn.path.to_path_buf();
                match store_conn.conn.upgrade() {
                    Some(conn) => Some((path, conn)),
                    None => {
                        let mut connection = ::new_connection(&store_conn.path)?;
                        let conn = Arc::new(Conn::connect(&mut connection)?);
                        (*store_conn).conn = Arc::downgrade(&conn);
                        Some((path, conn))
                    }
                }
            },
            Entry::Vacant(_) => None,
        })
    }

    // Returns a store with an existing connection to path, if available, or None if a
    // store at the provided path has not yet been opened.
    pub fn get(&mut self, name: &str) -> Result<Option<Store>> {
        Ok(match self.conn(name)? {
            Some((path, conn)) => {
                let sqlite = ::new_connection(&path)?;
                Some(Store { conn: conn, sqlite: sqlite, })
            },
            None => None,
        })
    }

    // Creates a new store on an existing connection with a new rusqlite connection.
    // Equivalent to forking an existing store.
    pub fn connect(&mut self, name: &str) -> Result<Store> {
        let (path, conn) = self.conn(name)?.ok_or(ErrorKind::StoreNotFound(name.to_string()))?;
        Ok(Store { conn: conn, sqlite: ::new_connection(path)?, })
    }

    // Drops the weak reference we have stored to an opened store there is no more than
    // one Store with a reference to the Conn for the provided path.
    pub fn close(&mut self, name: &str) -> Result<()> {
        let conn = self.connections.get(name).ok_or(ErrorKind::StoreNotFound(name.to_string()))?.conn.upgrade();
        if let Some(conn) = conn {
            // a ref count larger than two means that more than a single Store and the strong reference
            // we are using for this test are in existence.
            if Arc::strong_count(&conn) > 2 {
                bail!(ErrorKind::StoreConnectionStillActive(name.to_string()));
            }
        }
        self.connections.remove(name);
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stores_open_new_store() {
        let store = Stores::open_store("test.db").expect("Expected a store to be opened");
        assert_eq!(1, Arc::strong_count(store.conn()));
    }

    #[test]
    fn test_stores_open_new_named_in_memory_store() {
        let name = "test_stores_open_new_named_in_memory_store";
        let store = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert_eq!(1,  Arc::strong_count(store.conn()));
    }

    #[test]
    fn test_stores_open_existing_store() {
        let name = "test_stores_open_existing_store";
        {
            let store1 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
            assert_eq!(1, Arc::strong_count(store1.conn()));
        }
        {
            let store2 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
            assert_eq!(1, Arc::strong_count(store2.conn()));
        }
    }

    #[test]
    fn test_stores_get_open_store() {
        let name = "test_stores_get_open_store";
        {
            let store = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
            assert_eq!(1,  Arc::strong_count(store.conn()));
        }
        {
            let store_ref = Stores::get_named_in_memory_store(name).expect("Expected a store to be fetched").expect("store");
            assert_eq!(1,  Arc::strong_count(store_ref.conn()));
        }
    }

    #[test]
    fn test_stores_get_closed_store() {
        match Stores::get_named_in_memory_store("test_stores_get_closed_store").expect("Expected a store to be fetched") {
            None => (),
            Some(_) => panic!("Store is not open and so none should be returned"),
        }
    }

    #[test]
    fn test_stores_connect_open_store() {
        let name = "test_stores_connect_open_store";
        {
            let store1 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
            assert_eq!(1,  Arc::strong_count(store1.conn()));
        }

        // forking an open store leads to a ref count of 2 on the shared conn.
        let store2 = Stores::connect_named_in_memory_store(name).expect("expected a new store");
        assert_eq!(1,  Arc::strong_count(store2.conn()));

        {
            // fetching a reference to the original store also has a ref count of 2 on the shared conn
            let store3 = Stores::get_named_in_memory_store(name).expect("Expected a store to be fetched").unwrap();
            assert_eq!(2,  Arc::strong_count(store3.conn()));
        }

        {
            // forking again, in it's own scope increases the refcount.
            let store4 = Stores::connect_named_in_memory_store(name).expect("expected a new store");
            assert_eq!(2,  Arc::strong_count(store4.conn()));
            assert_eq!(2,  Arc::strong_count(store2.conn()));
        }

        // but now that scope is over, the original refcount is restored.
        assert_eq!(1,  Arc::strong_count(store2.conn()));
    }

    #[test]
    fn test_stores_connect_closed_store() {
        let name = "test_stores_connect_closed_store";
        let err = Stores::connect_named_in_memory_store(name).err();
        match err.unwrap() {
            Error(ErrorKind::StoreNotFound(message), _) => { assert_eq!(name, message); },
            x => panic!("expected Store Not Found error, got {:?}", x),
        }
    }

    #[test]
    fn test_stores_close_store_with_one_reference() {
        let name = "test_stores_close_store_with_one_reference";
        let store = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert_eq!(1,  Arc::strong_count(store.conn()));

        assert!(Stores::close_named_in_memory_store(name).is_ok());

        assert!(Stores::get_named_in_memory_store(name).expect("expected an empty result").is_none())
    }

    #[test]
    fn test_stores_close_store_with_multiple_references() {
        let name = "test_stores_close_store_with_multiple_references";

        let store1 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
        assert_eq!(1,  Arc::strong_count(store1.conn()));

        // forking an open store leads to a ref count of 2 on the shared conn.
        let store2 = Stores::connect_named_in_memory_store(name).expect("expected a connected store");
        assert_eq!(2,  Arc::strong_count(store2.conn()));

        let err = Stores::close_named_in_memory_store(name).err();
                match err.unwrap() {
                    Error(ErrorKind::StoreConnectionStillActive(message), _) => { assert_eq!(name, message); },
                    x => panic!("expected StoreConnectionStillActive error, got {:?}", x),
        }
    }

    #[test]
    fn test_stores_close_store_with_scoped_multiple_references() {
        let name = "test_stores_close_store_with_scoped_multiple_references";
        {
            let store1 = Stores::open_named_in_memory_store(name).expect("Expected a store to be opened");
            assert_eq!(1, Arc::strong_count(store1.conn()));

            // forking an open store leads to a ref count of 2 on the shared conn.
            let store2 = Stores::connect_named_in_memory_store(name).expect("expected a new store");
            assert_eq!(2, Arc::strong_count(store2.conn()));

            let err = Stores::close_named_in_memory_store(name).err();
            match err.unwrap() {
                Error(ErrorKind::StoreConnectionStillActive(message), _) => { assert_eq!(name, message); },
                x => panic!("expected StoreConnectionStillActive error, got {:?}", x),
            }
        }

        // outside of the scope, there should only be one strong reference so we can close the connection
        assert!(Stores::close_named_in_memory_store(name).is_ok());
        assert!(Stores::get_named_in_memory_store(name).expect("expected an empty result").is_none())
    }

    #[test]
    fn test_stores_close_unopened_store() {
        let name = "test_stores_close_unopened_store";

        let err = Stores::close_named_in_memory_store(name).err();
        match err.unwrap() {
            Error(ErrorKind::StoreNotFound(message), _) => { assert_eq!(name, message); },
            x => panic!("expected StoreNotFound error, got {:?}", x),
        }
    }

    #[test]
    fn test_stores_connect_perform_mutable_operations() {
        let path = "test.db";
        {
            let mut store1 = Stores::open_store(path).expect("Expected a store to be opened");
            assert_eq!(1,  Arc::strong_count(store1.conn()));
            let mut in_progress = store1.begin_transaction().expect("begun");
            in_progress.transact(r#"[
                {  :db/ident       :foo/bar
                   :db/cardinality :db.cardinality/one
                   :db/index       true
                   :db/unique      :db.unique/identity
                   :db/valueType   :db.type/long }
                {  :db/ident       :foo/baz
                   :db/cardinality :db.cardinality/one
                   :db/valueType   :db.type/boolean }
                {  :db/ident       :foo/x
                   :db/cardinality :db.cardinality/many
                   :db/valueType   :db.type/long }]"#).expect("transact");

            in_progress.commit().expect("commit");
        }

        {
            // forking an open store leads to a ref count of 2 on the shared conn.
            // we should be able to perform write operations on this connection
            let mut store2 = Stores::connect_store(path).expect("expected a new store");
            let mut in_progress = store2.begin_transaction().expect("begun");
            in_progress.transact(r#"[
                {:foo/bar 15 :foo/baz false, :foo/x [1 2 3]}
                {:foo/bar 99 :foo/baz true}
                {:foo/bar -2 :foo/baz true}
                ]"#).expect("transact");
            in_progress.commit().expect("commit");
        }
    }
}
