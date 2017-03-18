// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

///! This module defines core types that support the transaction processor.

extern crate edn;

use std::collections::BTreeMap;

use self::edn::types::Value;
use self::edn::symbols::NamespacedKeyword;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Entid {
    Entid(i64),
    Ident(NamespacedKeyword),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef {
    pub a: Entid,
    // In theory we could allow nested lookup-refs.  In practice this would require us to process
    // lookup-refs in multiple phases, like how we resolve tempids, which isn't worth the effort.
    pub v: Value, // An atom.
}

pub type MapNotation = BTreeMap<Entid, AtomOrLookupRefOrVectorOrMapNotation>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AtomOrLookupRefOrVectorOrMapNotation {
    Atom(Value),
    LookupRef(LookupRef),
    Vector(Vec<AtomOrLookupRefOrVectorOrMapNotation>),
    MapNotation(MapNotation),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum EntidOrLookupRefOrTempId {
    Entid(Entid),
    LookupRef(LookupRef),
    TempId(String),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum OpType {
    Add,
    Retract,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Entity {
    // Like [:db/add|:db/retract e a v].
    AddOrRetract {
        op: OpType,
        e: EntidOrLookupRefOrTempId,
        a: Entid,
        v: AtomOrLookupRefOrVectorOrMapNotation,
    },
    // Like {:db/id "tempid" a1 v1 a2 v2}.
    MapNotation(MapNotation),
}
