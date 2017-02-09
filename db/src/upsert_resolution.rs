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

//! This module implements the upsert resolution algorithm described at
//! https://github.com/mozilla/mentat/wiki/Transacting:-upsert-resolution-algorithm.

use std;
use std::iter::{empty, once};

// use mentat_core::Attribute;
use mentat_tx::entities::OpType;
use types::*;
use internal_types::*;

/// XXX Population type models 
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
enum PopulationType {
    /// "Simple upserts" that look like [:db/add TEMPID a v], where a is :db.unique/identity.
    UpsertsE,

    /// "Complex upserts" that look like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
    UpsertsEV,

    /// Entities that look like:
    /// - [:db/add TEMPID b OTHERID], where b is not :db.unique/identity;
    /// - [:db/add TEMPID b v], where b is not :db.unique/identity.
    /// - [:db/add e b OTHERID].
    Allocations,

    /// Entities that do not reference temp IDs.
    Inert,
}

/// A "Simple upsert" that looks like [:db/add TEMPID a v], where a is :db.unique/identity.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
struct UpsertE(TempId, Attribute, Entid, TypedValue);

/// A "Complex upsert" that looks like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
struct UpsertEV(TempId, Attribute, Entid, TempId);

#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
struct Generation {
    /// "Simple upserts" that look like [:db/add TEMPID a v], where a is :db.unique/identity.
    upserts_e: Vec<UpsertE>,

    /// "Complex upserts" that look like [:db/add TEMPID a OTHERID], where a is :db.unique/identity
    upserts_ev: Vec<UpsertEV>,

    /// Entities that look like:
    /// - [:db/add TEMPID b OTHERID], where b is not :db.unique/identity;
    /// - [:db/add TEMPID b v], where b is not :db.unique/identity.
    /// - [:db/add e b OTHERID].
    allocations: Vec<TermWithTempIds>,

    /// Entities that upserted and no longer reference temp IDs.  These assertions are guaranteed to
    /// be in the store.
    upserted: Vec<TermWithoutTempIds>,

    /// Entities that resolved due to other upserts and no longer reference temp IDs.  These
    /// assertions may or may not be in the store.
    resolved: Vec<TermWithoutTempIds>,

    // /// Entities that look like [:db/add TEMPID b OTHERID], where b is not :db.unique/identity.
    // allocations_ev: Population,

    // /// Entities that look like [:db/add TEMPID b OTHERID], where b is not :db.unique/identity.
    // allocations_e: Population,

    // /// Entities that look like [:db/add e b OTHERID], where b is not :db.unique/identity
    // allocations_v: Population,

    // /// Upserts that upserted.
    // upserted: Population,

    // /// Allocations that resolved due to other upserts.
    // resolved: Population,
}

impl Generation {
    /// Return true if it's possible to evolve this generation further.
    fn can_evolve(&self) -> bool {
        !self.upserts_e.is_empty()
    }

    /// Evolve this generation one step further by rewriting the existing :db/add entities using the
    /// given temporary IDs.
    ///
    /// TODO: Considering doing this in place; the function already consumes `self`.
    fn evolve_one_step(self, temp_id_map: &TempIdMap) -> Generation {
        let mut next = Generation::default();
        next.allocations = self.allocations;
        next.upserted = self.upserted;
        next.resolved = self.resolved;

        for UpsertE(t, _, a, v) in self.upserts_e {
            match temp_id_map.get(&*t) {
                Some(&n) => next.upserted.push(Term::AddOrRetract(OpType::Add, n, a, v)),
                None => next.allocations.push(Term::AddOrRetract(OpType::Add, std::result::Result::Err(t), a, std::result::Result::Ok(v))),
            }
        }

        for UpsertEV(t1, attribute, a, t2) in self.upserts_ev {
            match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
                (Some(&n1), Some(&n2)) => next.resolved.push(Term::AddOrRetract(OpType::Add, n1, a, TypedValue::Ref(n2))),
                (None, Some(&n2)) => next.upserts_e.push(UpsertE(t1, attribute, a, TypedValue::Ref(n2))),
                (Some(&n1), None) => next.allocations.push(Term::AddOrRetract(OpType::Add, std::result::Result::Ok(n1), a, std::result::Result::Err(t2))),
                (None, None) => next.allocations.push(Term::AddOrRetract(OpType::Add, std::result::Result::Err(t1), a, std::result::Result::Err(t2))),
            }
        }

        next
    }

    // /// Iterate any temporary IDs present in entities still requiring allocation.
    // ///
    // /// Note: the return type is Box<> since `impl Trait` is not yet stable.
    // fn temp_ids_iter<'a>(&'a self) -> Box<Iterator<Item=TempId> + 'a> {
    //     // fn collect<B: FromIterator<Self::Item>>(self) -> B
    //     for term in &self.allocations {
    //         match term {
    //             &Term::AddOrRetract(_, std::result::Result::Err(ref t1), _, std::result::Result::Err(ref t2)) => once(t1).chain(once(t2)) as &Iterator<Item=TempId>,
    //             &Term::AddOrRetract(_, std::result::Result::Err(ref t1), _, std::result::Result::Ok(_)) => once(t1) as &Iterator<Item=TempId>,
    //             &Term::AddOrRetract(_, std::result::Result::Ok(_), _, std::result::Result::Err(ref t2)) => once(t2) as &Iterator<Item=TempId>,
    //             &Term::AddOrRetract(_, std::result::Result::Ok(_), _, std::result::Result::Ok(_)) => empty() as &Iterator<Item=TempId>,
    //         }
    //     });

    //     Box::new(i)
    // }
}



        // // TODO: handle all allocations uniformly? // .into_iter().chain(self.allocations_e.into_iter()).chain(self.allocations_v.into_iter()) {
        // for term in self.allocations_ev {
        //     match term {
        //         Term::AddOrRetract(op, std::result::Result::Err(t1), a, std::result::Result::Err(t2)) => {
        //             match (temp_id_map.get(&*t1), temp_id_map.get(&*t2)) {
        //                 (Some(&n1), Some(&n2)) => next.resolved.push(Term::AddOrRetract(op, std::result::Result::Ok(n1), a, std::result::Result::Ok(TypedValue::Ref(n2)))),
        //                 (None, Some(&n2)) => next.allocations_e.push(Term::AddOrRetract(op, std::result::Result::Err(t1), a, std::result::Result::Ok(TypedValue::Ref(n2)))),
        //                 (Some(&n1), None) => next.allocations_v.push(Term::AddOrRetract(op, std::result::Result::Ok(n1), a, std::result::Result::Err(t2))),
        //                 (None, None) => next.allocations_ev.push(Term::AddOrRetract(op, std::result::Result::Err(t1), a, std::result::Result::Err(t2))),
        //             }
        //         },
        //         _ => panic!("At the disco"),
        //     }
        // }

        // // TODO: same as upserts_e!
        // for term in self.allocations_e {
        //     match term {
        //         Term::AddOrRetract(op, std::result::Result::Err(t), a, v) => {
        //             match temp_id_map.get(&*t) {
        //                 Some(&n) => next.resolved.push(Term::AddOrRetract(op, std::result::Result::Ok(n), a, v)),
        //                 None => next.allocations_e.push(Term::AddOrRetract(op, std::result::Result::Err(t), a, v)),
        //             }
        //         },
        //         _ => panic!("At the disco"),
        //     }
        // }

        // for term in self.allocations_v {
        //     match term {
        //         Term::AddOrRetract(op, e, a, std::result::Result::Err(t)) => {
        //             match temp_id_map.get(&*t) {
        //                 Some(&n) => next.resolved.push(Term::AddOrRetract(op, e, a, std::result::Result::Ok(TypedValue::Ref(n)))),
        //                 None => next.allocations_v.push(Term::AddOrRetract(op, e, a, std::result::Result::Err(t))),
        //             }
        //         },
        //         _ => panic!("At the disco"),
        //     }
        // }

        // next.inert = self.inert;

    //     next

    //     // AddOrRetract(OpType, E, Entid, V),
    //     // RetractAttribute(E, Entid),
    //     // RetractEntity(E)
    // }
