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

//! XXX

use std;
use std::collections::HashMap;
use std::rc::Rc;

use types::*;
use mentat_tx::entities::OpType;

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum Term<E, V> {
    AddOrRetract(OpType, E, Entid, V),
    RetractAttribute(E, Entid),
    RetractEntity(E)
}

pub type EntidOr<T> = std::result::Result<Entid, T>;
pub type TypedValueOr<T> = std::result::Result<TypedValue, T>;

pub type TempId = Rc<String>;
pub type TempIdMap = HashMap<TempId, Entid>;

pub type LookupRef = Rc<AVPair>;

/// Internal representation of an entid on its way to resolution.  We either have the simple case (a
/// numeric entid), a lookup-ref that still needs to be resolved (an atomized [a v] pair), or a temp
/// ID that needs to be upserted or allocated (an atomized temp ID).
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum LookupRefOrTempId {
    LookupRef(LookupRef),
    TempId(TempId)
}

pub type TermWithTempIdsAndLookupRefs = Term<EntidOr<LookupRefOrTempId>, TypedValueOr<LookupRefOrTempId>>;
pub type TermWithTempIds = Term<EntidOr<TempId>, TypedValueOr<TempId>>;
pub type TermWithoutTempIds = Term<Entid, TypedValue>;
pub type Population = Vec<TermWithTempIds>;
