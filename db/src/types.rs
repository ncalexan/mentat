// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std::collections::HashMap;
use std::collections::BTreeMap;

extern crate mentat_core;

pub use self::mentat_core::{
    Entid,
    ValueType,
    TypedValue,
    Attribute,
};

/// Represents one partition of the entid space.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Partition {
    /// The first entid in the partition.
    pub start: i64,
    /// The next entid to be allocated in the partition.
    pub index: i64,
}

impl Partition {
    pub fn new(start: i64, next: i64) -> Partition {
        assert!(start <= next, "A partition represents a monotonic increasing sequence of entids.");
        Partition { start: start, index: next }
    }
}

/// Map partition names to `Partition` instances.
pub type PartitionMap = BTreeMap<String, Partition>;
/// Map `String` idents (`:db/ident`) to positive integer entids (`1`).
pub type IdentMap = BTreeMap<String, Entid>;

/// Map positive integer entids (`1`) to `String` idents (`:db/ident`).
pub type EntidMap = BTreeMap<Entid, String>;

/// Map attribute entids to `Attribute` instances.
pub type SchemaMap = BTreeMap<i64, Attribute>;

/// Represents a Mentat schema.
///
/// Maintains the mapping between string idents and positive integer entids; and exposes the schema
/// flags associated to a given entid (equivalently, ident).
///
/// TODO: consider a single bi-directional map instead of separate ident->entid and entid->ident
/// maps.
#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Schema {
    /// Map entid->ident.
    ///
    /// Invariant: is the inverse map of `ident_map`.
    pub entid_map: EntidMap,

    /// Map ident->entid.
    ///
    /// Invariant: is the inverse map of `entid_map`.
    pub ident_map: IdentMap,

    /// Map entid->attribute flags.
    ///
    /// Invariant: key-set is the same as the key-set of `entid_map` (equivalently, the value-set of
    /// `ident_map`).
    pub schema_map: SchemaMap,
}

/// Represents the metadata required to query from, or apply transactions to, a Mentat store.
///
/// See https://github.com/mozilla/mentat/wiki/Thoughts:-modeling-db-conn-in-Rust.
#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct DB {
    /// Map partition name->`Partition`.
    ///
    /// TODO: represent partitions as entids.
    pub partition_map: PartitionMap,

    /// The schema of the store.
    pub schema: Schema,

    /// The next TempID identifier to be allocated.
    next_temp_id_idx: i64,
}

impl DB {
    pub fn new(partition_map: PartitionMap, schema: Schema) -> DB {
        DB {
            partition_map: partition_map,
            schema: schema,
            next_temp_id_idx: -1_000_000,
        }
    }

    // pub fn allocate_temp_id<T>(&mut self, partition: T) -> TempId where T: Into<String> {
    //     let idx = self.next_temp_id_idx;
    //     self.next_temp_id_idx -= 1;
    //     TempId {
    //         partition: partition.into(),
    //         idx: idx,
    //     }
    // }
}

/// Represents a temporary ID on its way to being resolved.
///
/// A TempId is scoped to a single transaction.  The transaction parser produces `Entity` instances
/// that may include `IdLiteral` instances; such id literals are indepedent of a `DB` instance.  As
/// they are transacted, each id literal is resolved to a concrete `TempId` instance.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct TempIdX {
    /// The partition the entid this ID literal is resolved to will be allocated in.
    partition: String,

    /// A negative integer identifying this `TempId` uniquely in the scope of a single transaction.
    idx: i64,
}

/// A pair [a v] in the store.
///
/// Used to represent lookup-refs and [TEMPID a v] upserts as they are resolved.
pub type AVPair = (Entid, TypedValue);

/// Map [a v] pairs to existing entids.
///
/// Used to resolve lookup-refs and upserts.
pub type AVMap<'a> = HashMap<&'a AVPair, Entid>;

/// A transaction report summarizes an applied transaction.
#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct TxReport {
    /// The transaction ID of the transaction.
    pub tx: Entid,

    /// The timestamp when the transaction was commited.
    ///
    /// This is milliseconds after the Unix epoch according to the transactor's local clock.
    // TODO: :db.type/instant.
    pub tx_instant: i64,

    // /// A map from temporary ID to allocated entid.
    // pub temp_ids: BTreeMap<TempId, Entid>,
}
