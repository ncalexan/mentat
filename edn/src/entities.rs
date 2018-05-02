// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module defines core types that support the transaction processor.

use std::collections::BTreeMap;
use std::fmt;

use symbols::{
    Keyword,
    PlainSymbol,
};

/// A tempid, either an external tempid given in a transaction (usually as an `Value::Text`),
/// or an internal tempid allocated by Mentat itself.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum TempId {
    External(String),
    Internal(i64),
}

impl TempId {
    pub fn into_external(self) -> Option<String> {
        match self {
            TempId::External(s) => Some(s),
            TempId::Internal(_) => None,
        }
    }
}

impl fmt::Display for TempId {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &TempId::External(ref s) => write!(f, "{}", s),
            &TempId::Internal(x) => write!(f, "<tempid {}>", x),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Entid {
    Entid(i64),
    Ident(Keyword),
}

impl From<i64> for Entid {
    fn from(v: i64) -> Self {
        Entid::Entid(v)
    }
}

impl From<Keyword> for Entid {
    fn from(v: Keyword) -> Self {
        Entid::Ident(v)
    }
}

impl Entid {
    pub fn unreversed(&self) -> Option<Entid> {
        match self {
            &Entid::Entid(_) => None,
            &Entid::Ident(ref a) => a.unreversed().map(Entid::Ident),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct LookupRef<V> {
    pub a: AttributePlace,
    // In theory we could allow nested lookup-refs.  In practice this would require us to process
    // lookup-refs in multiple phases, like how we resolve tempids, which isn't worth the effort.
    pub v: V, // An atom.
}

/// A "transaction function" that exposes some value determined by the current transaction.  The
/// prototypical example is the current transaction ID, `(transaction-tx)`.
///
/// A natural next step might be to expose the current transaction instant `(transaction-instant)`,
/// but that's more difficult: the transaction itself can set the transaction instant (with some
/// restrictions), so the transaction function must be late-binding.  Right now, that's difficult to
/// arrange in the transactor.
///
/// In the future, we might accept arguments; for example, perhaps we might expose `(ancestor
/// (transaction-tx) n)` to find the n-th ancestor of the current transaction.  If we do accept
/// arguments, then the special case of `(lookup-ref a v)` should be handled as part of the
/// generalization.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TxFunction {
    pub op: PlainSymbol,
}

pub type MapNotation<V> = BTreeMap<Entid, ValuePlace<V>>;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum ValuePlace<V> {
    // We never know at parse-time whether an integer or ident is really an entid, but we will often
    // know when building entities programmatically.
    Entid(Entid),
    // We never know at parse-time whether a string is really a tempid, but we will often know when
    // building entities programmatically.
    TempId(TempId),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
    Vector(Vec<ValuePlace<V>>),
    Atom(V),
    MapNotation(MapNotation<V>),
}

impl<V> From<TxFunction> for ValuePlace<V> {
    fn from(v: TxFunction) -> Self {
        ValuePlace::TxFunction(v)
    }
}

// // TODO: think more about how general we want this lift to be.
// impl<V> From<V> for ValuePlace<V> {
//     fn from(v: V) -> Self {
//         ValuePlace::Atom(v)
//     }
// }

impl<V> From<LookupRef<V>> for ValuePlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        ValuePlace::LookupRef(v)
    }
}

impl<V> From<Vec<ValuePlace<V>>> for ValuePlace<V> {
    fn from(v: Vec<ValuePlace<V>>) -> Self {
        ValuePlace::Vector(v)
    }
}

impl<V> From<MapNotation<V>> for ValuePlace<V> {
    fn from(v: MapNotation<V>) -> Self {
        ValuePlace::MapNotation(v)
    }
}

impl From<i64> for AttributePlace {
    fn from(v: i64) -> Self {
        AttributePlace::Entid(Entid::Entid(v))
    }
}

impl From<Keyword> for AttributePlace {
    fn from(v: Keyword) -> Self {
        AttributePlace::Entid(Entid::Ident(v))
    }
}

// impl From<&str> for Entid {
//     fn from(v: &str) -> Self {
//         AttributePlace::Entid(Entid::Ident(Keyword::new v  ))
//     }
// }

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum EntityPlace<V> {
    Entid(Entid),
    TempId(TempId),
    LookupRef(LookupRef<V>),
    TxFunction(TxFunction),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributePlace {
    Entid(Entid),
}

impl<V, E: Into<Entid>> From<E> for EntityPlace<V> {
    fn from(v: E) -> Self {
        EntityPlace::Entid(v.into())
    }
}

impl<V> From<LookupRef<V>> for EntityPlace<V> {
    fn from(v: LookupRef<V>) -> Self {
        EntityPlace::LookupRef(v)
    }
}

impl<V> From<TxFunction> for EntityPlace<V> {
    fn from(v: TxFunction) -> Self {
        EntityPlace::TxFunction(v)
    }
}

impl<V> From<TempId> for EntityPlace<V> {
    fn from(v: TempId) -> Self {
        EntityPlace::TempId(v)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum OpType {
    Add,
    Retract,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum Entity<V> {
    // Like [:db/add|:db/retract e a v].
    AddOrRetract {
        op: OpType,
        e: EntityPlace<V>,
        a: AttributePlace,
        v: ValuePlace<V>,
    },
    // Like {:db/id "tempid" a1 v1 a2 v2}.
    MapNotation(MapNotation<V>),
}
